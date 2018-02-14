package executor

import (
	"hash"
	"hash/fnv"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strconv"

	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/encoding"
	"github.com/robot-dreams/zdb2/encoding/stream"
	"github.com/willf/bloom"
)

const (
	// Bloom filter parameters.
	m = 1 << 24
	k = 5

	// If you ask for more than this many partitions, then you can go hash join
	// yourself with yourself.
	maxPartitions = 1 << 20
)

type result struct {
	record zdb2.Record
	err    error
}

type hybridHashJoin struct {
	// r and s are Iterators over the two input tables to be joined, where r is
	// the smaller of the two tables.
	r zdb2.Iterator
	s zdb2.Iterator

	// Header for the joined table.  Note that the fields of r appear first.
	t *zdb2.TableHeader

	// The fields on which the (equi)join should be performed.
	rJoinField string
	sJoinField string

	// Fraction of records in r to keep in an in-memory hash table.
	inMemoryFraction float64

	// Hash function used to distribute records (based on the value of their
	// join fields) among partitions.
	hashFunc hash.Hash32

	// Number of on-disk partitions for records that aren't processed
	// immediately using via the in-memory hash table.
	numPartitions int

	// Location for storing on-disk partitions; we assume that a hybridHashJoin
	// instance has exclusive access to its partitionDir.
	partitionDir string

	// To keep the structure of the code simple, we decouple the join algorithm
	// from the process of returning results when Next is called.
	results chan *result
}

var _ zdb2.Iterator = (*hybridHashJoin)(nil)

func NewHybridHashJoin(
	r, s zdb2.Iterator,
	rJoinField, sJoinField string,
	inMemoryFraction float64,
	numPartitions int,
) (*hybridHashJoin, error) {
	t, err := zdb2.JoinedHeader(
		r.TableHeader(), s.TableHeader(), rJoinField, sJoinField)
	if err != nil {
		return nil, err
	}
	if inMemoryFraction <= 0 || inMemoryFraction >= 1 {
		return nil, errors.Newf(
			"inMemoryFraction must be in (0, 1); got %v", inMemoryFraction)
	}
	if numPartitions <= 0 || numPartitions > maxPartitions {
		return nil, errors.Newf(
			"numPartitions %d is outside allowed range (0, %d]",
			numPartitions,
			maxPartitions)
	}
	partitionDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	h := &hybridHashJoin{
		r:                r,
		s:                s,
		t:                t,
		rJoinField:       rJoinField,
		sJoinField:       sJoinField,
		inMemoryFraction: inMemoryFraction,
		hashFunc:         fnv.New32(),
		numPartitions:    numPartitions,
		partitionDir:     partitionDir,
		results:          make(chan *result),
	}
	go h.start()
	return h, nil
}

func (h *hybridHashJoin) start() {
	defer close(h.results)

	_, _, err := h.initialPass()
	if err != nil {
		h.results <- &result{nil, err}
		return
	}
}

func (h *hybridHashJoin) initialPass() ([]string, []string, error) {
	// We keep a Bloom filter over the set of join field values in r, so that
	// during our initial pass over s, we can immediately discard records which
	// are guaranteed not to join with any records in r.
	bloomFilter := bloom.New(m, k)

	// During our initial pass over r, if the FNV-1 hash of a record's join
	// field is <= inMemoryHashThreshold, then we keep that record in the
	// inMemoryHashTable (instead of writing it to one of the partitions on disk
	// for processing in the next pass).
	inMemoryHashThreshold := uint32(math.Floor(h.inMemoryFraction * math.MaxUint32))
	inMemoryHashTable := make(map[interface{}][]zdb2.Record)

	// Perform initial pass over the records in r.
	rPartitionPaths := h.partitionPaths(
		h.r.TableHeader().Name, h.numPartitions)
	rPartitionedWrite, err := stream.NewPartitionedWrite(
		rPartitionPaths, h.r.TableHeader())
	if err != nil {
		return nil, nil, err
	}
	rJoinIndex, rJoinType := zdb2.MustFieldIndexAndType(
		h.r.TableHeader(), h.rJoinField)
	for {
		rRecord, err := h.r.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}
		rJoinValue := rRecord[rJoinIndex]
		rSerializedJoinValue, err := encoding.SerializeValue(rJoinType, rJoinValue)

		// Add field value to Bloom filter.
		if err != nil {
			return nil, nil, err
		}
		bloomFilter.Add(rSerializedJoinValue)

		// Send the record to the correct output partition (either one of the
		// on-disk partitions, or the in-memory hash table).
		partition := h.getPartition(
			h.hashFunc,
			inMemoryHashThreshold,
			h.numPartitions,
			rSerializedJoinValue)
		if partition == h.numPartitions {
			inMemoryHashTable[rJoinValue] = append(
				inMemoryHashTable[rJoinValue],
				rRecord)
		} else {
			err = rPartitionedWrite.WriteRecordToPartition(rRecord, partition)
			if err != nil {
				return nil, nil, err
			}
		}
		err = rPartitionedWrite.Close()
		if err != nil {
			return nil, nil, err
		}
	}

	// Perform initial pass over the records in s.
	sPartitionPaths := h.partitionPaths(
		h.s.TableHeader().Name, h.numPartitions)
	sPartitionedWrite, err := stream.NewPartitionedWrite(
		sPartitionPaths, h.s.TableHeader())
	if err != nil {
		return nil, nil, err
	}
	sJoinIndex, sJoinType := zdb2.MustFieldIndexAndType(
		h.s.TableHeader(), h.sJoinField)
	for {
		sRecord, err := h.r.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}
		sJoinValue := sRecord[sJoinIndex]
		sSerializedJoinValue, err := encoding.SerializeValue(sJoinType, sJoinValue)

		// If the join value doesn't appear in the Bloom filter, then the record
		// definitely won't be joined with any records in r, and we can discard
		// it right away.
		if !bloomFilter.Test(sSerializedJoinValue) {
			continue
		}

		// Send the record to the correct output partition (either to one of the
		// on-disk partitions, or for checking against the in-memory hash table).
		partition := h.getPartition(
			h.hashFunc,
			inMemoryHashThreshold,
			h.numPartitions,
			sSerializedJoinValue)
		if partition == h.numPartitions {
			// If the join value appears in the in-memory hash table, then we can
			// process the record right away.
			for _, rRecord := range inMemoryHashTable[sJoinValue] {
				h.results <- &result{zdb2.JoinedRecord(rRecord, sRecord), nil}
			}
		} else {
			err := sPartitionedWrite.WriteRecordToPartition(sRecord, partition)
			if err != nil {
				return nil, nil, err
			}
		}
		err = sPartitionedWrite.Close()
		if err != nil {
			return nil, nil, err
		}
	}
	return rPartitionPaths, sPartitionPaths, nil
}

// The result will be in [0, numPartitions]; if the result is equal to
// numPartitions then the record belongs to the in-memory "partition".
func (h *hybridHashJoin) getPartition(
	hashFunc hash.Hash32,
	inMemoryHashThreshold uint32,
	numPartitions int,
	serializedValue []byte,
) int {
	hashFunc.Reset()
	_, _ = hashFunc.Write(serializedValue)
	n := hashFunc.Sum32()
	if n <= inMemoryHashThreshold {
		return numPartitions
	} else {
		return int(n % uint32(numPartitions))
	}
}

func (h *hybridHashJoin) partitionPaths(
	prefix string,
	numPartitions int,
) []string {
	result := make([]string, numPartitions)
	for i := 0; i < numPartitions; i++ {
		result[i] = h.partitionDir + "/" + prefix + "-" + strconv.Itoa(i)
	}
	return result
}

func (h *hybridHashJoin) TableHeader() *zdb2.TableHeader {
	return h.t
}

func (h *hybridHashJoin) Next() (zdb2.Record, error) {
	result, ok := <-h.results
	if !ok {
		return nil, io.EOF
	}
	return result.record, result.err
}

func (h *hybridHashJoin) Close() error {
	for _, iter := range []zdb2.Iterator{h.r, h.s} {
		err := iter.Close()
		if err != nil {
			return err
		}
	}
	return os.RemoveAll(h.partitionDir)
}

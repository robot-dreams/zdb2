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
	"github.com/robot-dreams/zdb2/executor/stream"
	"github.com/willf/bloom"
)

const (
	// Bloom filter parameters.
	m = 1 << 28
	k = 3

	// No one needs more than this many partitions.
	maxPartitions = 1 << 20
)

// We define a separate result type so that we can use a channel for decoupling
// result generation from result consumption.
type result struct {
	record zdb2.Record
	err    error
}

// hybridHashJoin supports EquiJoin using the hybrid strategy described in
// section 2.5 of the following reference:
//
//     http://www.cs.ucr.edu/~tsotras/cs236/W15/join.pdf
//
// Note that the query planner is responsible for choosing appropriate values of
// inMemoryFraction and numPartitions when instantiating the hybridHashJoin.
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
	go h.startResultGeneration()
	return h, nil
}

func (h *hybridHashJoin) startResultGeneration() {
	defer close(h.results)

	rPartitionPaths, sPartitionPaths, err := h.initialPass()
	if err != nil {
		h.results <- &result{nil, err}
		return
	}

	for i := 0; i < h.numPartitions; i++ {
		rPartitionPath := rPartitionPaths[i]
		sPartitionPath := sPartitionPaths[i]
		err := h.processPartition(rPartitionPath, sPartitionPath)
		if err != nil {
			h.results <- &result{nil, err}
			return
		}
	}
}

// The initial pass performs the following steps:
//
// - Add all join field values in r to a Bloom filter, and then immediately
//   discard any records in s that do not match any of the join field values
//
// - Store some records of r in an in-memory hash table, and then immediately
//   join them with any records of s that match
//
// - Write the remaining records of r and s to the appropriate on-disk
//   partitions for processing in a later step
//
// The returned slices are the full paths to the on-disk partitions of r and s,
// where the position in the slice indicates the partition number.  Note that
// the returned slices are both guaranteed to have length h.numPartitions.
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
	rPartitionPaths := h.partitionPaths(h.r.TableHeader().Name)
	rPartitionedWrite, err := stream.NewPartitionedWrite(rPartitionPaths, h.r.TableHeader())
	if err != nil {
		return nil, nil, err
	}
	rJoinPosition, rJoinType := zdb2.MustFieldPositionAndType(
		h.r.TableHeader(), h.rJoinField)
	rRecordFunc := func(
		rRecord zdb2.Record,
		rJoinType zdb2.Type,
		rJoinValue interface{},
	) error {
		rSerializedJoinValue, err := zdb2.SerializeValue(rJoinType, rJoinValue)
		if err != nil {
			return err
		}
		bloomFilter.Add(rSerializedJoinValue)

		// Send the record to the correct output partition (either one of the
		// on-disk partitions, or the in-memory hash table).
		partition := h.getPartition(
			inMemoryHashThreshold,
			rSerializedJoinValue)
		if partition == h.numPartitions {
			inMemoryHashTable[rJoinValue] = append(inMemoryHashTable[rJoinValue], rRecord)
		} else {
			err = rPartitionedWrite.WriteRecordToPartition(rRecord, partition)
			if err != nil {
				return err
			}
		}
		return nil
	}
	err = h.forEachRecord(h.r, rJoinPosition, rJoinType, rRecordFunc)
	if err != nil {
		return nil, nil, err
	}
	err = rPartitionedWrite.Close()
	if err != nil {
		return nil, nil, err
	}

	// Perform initial pass over the records in s.
	sPartitionPaths := h.partitionPaths(h.s.TableHeader().Name)
	sPartitionedWrite, err := stream.NewPartitionedWrite(sPartitionPaths, h.s.TableHeader())
	if err != nil {
		return nil, nil, err
	}
	sJoinPosition, sJoinType := zdb2.MustFieldPositionAndType(
		h.s.TableHeader(), h.sJoinField)
	sRecordFunc := func(
		sRecord zdb2.Record,
		sJoinType zdb2.Type,
		sJoinValue interface{},
	) error {
		// If the join value doesn't appear in the Bloom filter, then the record
		// definitely won't be joined with any records in r, and we can discard
		// it right away.
		sSerializedJoinValue, err := zdb2.SerializeValue(sJoinType, sJoinValue)
		if err != nil {
			return err
		}
		if !bloomFilter.Test(sSerializedJoinValue) {
			return nil
		}

		// Send the record to the correct output partition (either to one of the
		// on-disk partitions, or for checking against the in-memory hash table).
		partition := h.getPartition(
			inMemoryHashThreshold,
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
				return err
			}
		}
		return nil
	}
	err = h.forEachRecord(h.s, sJoinPosition, sJoinType, sRecordFunc)
	if err != nil {
		return nil, nil, err
	}
	err = sPartitionedWrite.Close()
	if err != nil {
		return nil, nil, err
	}
	return rPartitionPaths, sPartitionPaths, nil
}

func (h *hybridHashJoin) partitionPaths(prefix string) []string {
	result := make([]string, h.numPartitions)
	for i := 0; i < h.numPartitions; i++ {
		result[i] = h.partitionDir + "/" + prefix + "-" + strconv.Itoa(i)
	}
	return result
}

// The result will be in [0, h.numPartitions]; if the result is equal to
// h.numPartitions then the record belongs to the in-memory "partition".
func (h *hybridHashJoin) getPartition(
	inMemoryHashThreshold uint32,
	serializedValue []byte,
) int {
	h.hashFunc.Reset()
	_, _ = h.hashFunc.Write(serializedValue)
	n := h.hashFunc.Sum32()
	if n <= inMemoryHashThreshold {
		return h.numPartitions
	} else {
		return int(n % uint32(h.numPartitions))
	}
}

func (h *hybridHashJoin) forEachRecord(
	iter zdb2.Iterator,
	joinPosition int,
	joinType zdb2.Type,
	recordFunc func(zdb2.Record, zdb2.Type, interface{}) error,
) error {
	for {
		record, err := iter.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		joinValue := record[joinPosition]
		err = recordFunc(record, joinType, joinValue)
		if err != nil {
			return err
		}
	}
}

func (h *hybridHashJoin) processPartition(rPartitionPath, sPartitionPath string) error {
	// Load all records from the partition of r into an in-memory hash table.
	rScan, err := stream.NewScan(rPartitionPath)
	if err != nil {
		return err
	}
	inMemoryHashTable := make(map[interface{}][]zdb2.Record)
	rJoinPosition, rJoinType := zdb2.MustFieldPositionAndType(
		h.r.TableHeader(), h.rJoinField)
	rRecordFunc := func(
		rRecord zdb2.Record,
		rJoinType zdb2.Type,
		rJoinValue interface{},
	) error {
		inMemoryHashTable[rJoinValue] = append(inMemoryHashTable[rJoinValue], rRecord)
		return nil
	}
	err = h.forEachRecord(rScan, rJoinPosition, rJoinType, rRecordFunc)
	if err != nil {
		return err
	}

	// Scan all records from the corresponding partition of s and compare them
	// against the in-memory hash table.
	sScan, err := stream.NewScan(sPartitionPath)
	if err != nil {
		return err
	}
	sJoinPosition, sJoinType := zdb2.MustFieldPositionAndType(
		h.s.TableHeader(), h.sJoinField)
	sRecordFunc := func(
		sRecord zdb2.Record,
		sJoinType zdb2.Type,
		sJoinValue interface{},
	) error {
		for _, rRecord := range inMemoryHashTable[sJoinValue] {
			h.results <- &result{zdb2.JoinedRecord(rRecord, sRecord), nil}
		}
		return nil
	}
	return h.forEachRecord(sScan, sJoinPosition, sJoinType, sRecordFunc)
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

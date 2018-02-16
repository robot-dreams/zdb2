package executor

import (
	"io"

	"github.com/robot-dreams/zdb2"
)

// hashJoinClassic performs an EquiJoin on two tables, where the entire smaller
// table can fit into an in-memory hash map.
type hashJoinClassic struct {
	// r and s are Iterators over the two input tables to be joined, where r is
	// the smaller of the two tables.
	r zdb2.Iterator
	s zdb2.Iterator

	// Header for the joined table.  Note that the fields of r appear first.
	t *zdb2.TableHeader

	// The fields on which the (equi)join should be performed.
	rJoinField string
	sJoinField string

	// To keep the structure of the code simple, we decouple the join algorithm
	// from the process of returning results when Next is called.
	results chan *result
}

var _ zdb2.Iterator = (*hashJoinClassic)(nil)

func NewHashJoinClassic(
	r, s zdb2.Iterator,
	rJoinField, sJoinField string,
) (*hashJoinClassic, error) {
	t, err := zdb2.JoinedHeader(
		r.TableHeader(), s.TableHeader(), rJoinField, sJoinField)
	if err != nil {
		return nil, err
	}
	h := &hashJoinClassic{
		r:          r,
		s:          s,
		t:          t,
		rJoinField: rJoinField,
		sJoinField: sJoinField,
		results:    make(chan *result),
	}
	go h.start()
	return h, nil
}

func (h *hashJoinClassic) start() {
	defer close(h.results)

	// Build in-memory hash table over records in r.
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
	err := forEachRecord(h.r, rJoinPosition, rJoinType, rRecordFunc)
	if err != nil {
		h.results <- &result{nil, err}
	}

	// Scan records in s and look for matches.
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
	err = forEachRecord(h.s, sJoinPosition, sJoinType, sRecordFunc)
	if err != nil {
		h.results <- &result{nil, err}
	}
}

func (c *hashJoinClassic) TableHeader() *zdb2.TableHeader {
	return c.t
}

func (c *hashJoinClassic) Next() (zdb2.Record, error) {
	result, ok := <-c.results
	if !ok {
		return nil, io.EOF
	}
	return result.record, result.err
}

func (c *hashJoinClassic) Close() error {
	for _, iter := range []zdb2.Iterator{c.r, c.s} {
		err := iter.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

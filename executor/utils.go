package executor

import (
	"io"
	"sort"

	"github.com/robot-dreams/zdb2"
)

// We define a separate result type so that we can use a channel for decoupling
// result generation from result consumption.
type result struct {
	record zdb2.Record
	err    error
}

func forEachRecord(
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

type byField struct {
	sortFieldPosition int
	sortFieldType     zdb2.Type
	descending        bool
	records           []zdb2.Record
}

var _ sort.Interface = (*byField)(nil)

func (b *byField) Len() int {
	return len(b.records)
}

func (b *byField) Swap(i, j int) {
	b.records[i], b.records[j] = b.records[j], b.records[i]
}

func (b *byField) Less(i, j int) bool {
	v1 := b.records[i][b.sortFieldPosition]
	v2 := b.records[j][b.sortFieldPosition]
	if b.descending {
		return zdb2.Less(b.sortFieldType, v2, v1)
	} else {
		return zdb2.Less(b.sortFieldType, v1, v2)
	}
}

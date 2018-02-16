package executor

import (
	"io"

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

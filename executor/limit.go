package executor

import (
	"io"

	"github.com/robot-dreams/zdb2"
)

// limit sets an upper bound on the number of Records that can be read from
// the input Iterator.
type limit struct {
	iter           zdb2.Iterator
	t              *zdb2.TableHeader
	maxRecords     int
	numRecordsRead int
}

var _ zdb2.Iterator = (*limit)(nil)

func NewLimit(iter zdb2.Iterator, maxRecords int) *limit {
	return &limit{
		iter:       iter,
		t:          iter.TableHeader(),
		maxRecords: maxRecords,
	}
}

func (l *limit) TableHeader() *zdb2.TableHeader {
	return l.t
}

func (l *limit) Next() (zdb2.Record, error) {
	if l.numRecordsRead == l.maxRecords {
		return nil, io.EOF
	} else {
		r, err := l.iter.Next()
		if err != nil {
			return nil, err
		}
		l.numRecordsRead++
		return r, nil
	}
}

func (l *limit) Close() error {
	return l.iter.Close()
}

package index

import (
	"io"

	"github.com/robot-dreams/zdb2"
)

type Entry struct {
	Key int32
	RID zdb2.RecordID
}

type Iterator interface {
	// Returns io.EOF if there are no more entries.
	Next() (Entry, error)
}

type EmptyIterator struct{}

func (EmptyIterator) Next() (Entry, error) {
	return Entry{}, io.EOF
}

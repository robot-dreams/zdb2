package index

import (
	"io"
)

type RecordID struct {
	PageID int32
	SlotID uint16
}

type Entry struct {
	Key int32
	RID RecordID
}

type Iterator interface {
	// Returns io.EOF if there are no more entries.
	Next() (Entry, error)
}

type EmptyIterator struct{}

func (EmptyIterator) Next() (Entry, error) {
	return Entry{}, io.EOF
}

package zdb2

import (
	"io"
)

type inMemoryScan struct {
	t       *TableHeader
	records []Record
}

var _ Iterator = (*inMemoryScan)(nil)

func NewInMemoryScan(t *TableHeader, records []Record) *inMemoryScan {
	return &inMemoryScan{
		t:       t,
		records: records,
	}
}

func (m *inMemoryScan) TableHeader() *TableHeader {
	return m.t
}

func (m *inMemoryScan) Next() (Record, error) {
	if len(m.records) == 0 {
		return nil, io.EOF
	}
	r := m.records[0]
	m.records = m.records[1:]
	return r, nil
}

func (m *inMemoryScan) Close() error {
	m.records = nil
	return nil
}

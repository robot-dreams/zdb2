package zdb2

import "io"

type Type uint8

const (
	UnknownType Type = iota
	Int32
	Float64
	String
)

type Field struct {
	Name string
	Type Type
}

type TableHeader struct {
	Name string
	// Invariant: len(Fields) <= 0xFF
	Fields []*Field
}

type Record []interface{}

func (r1 Record) Equals(r2 Record) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i := range r1 {
		v1 := r1[i]
		v2 := r2[i]
		if v1 != v2 {
			return false
		}
	}
	return true
}

type Iterator interface {
	TableHeader() *TableHeader
	Next() (Record, error)
	Close() error
}

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

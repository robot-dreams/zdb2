package zdb2

const BlockSize = 1 << 16

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

type Predicate func(Record) bool

type Iterator interface {
	TableHeader() *TableHeader
	Next() (Record, error)
	Close() error
}

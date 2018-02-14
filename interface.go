package zdb2

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
	Init() error
	Next() (Record, error)
	Close() error
}

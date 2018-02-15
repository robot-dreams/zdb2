package executor

import "github.com/robot-dreams/zdb2"

// selection restricts Records from the input to those that satisfy the
// specified Predicate.
type selection struct {
	iter zdb2.Iterator
	p    zdb2.Predicate
}

var _ zdb2.Iterator = (*selection)(nil)

func NewSelection(iter zdb2.Iterator, p zdb2.Predicate) *selection {
	return &selection{
		iter: iter,
		p:    p,
	}
}

func (s *selection) TableHeader() *zdb2.TableHeader {
	return s.iter.TableHeader()
}

func (s *selection) Next() (zdb2.Record, error) {
	for {
		record, err := s.iter.Next()
		if err != nil {
			return nil, err
		}
		if s.p(record) {
			return record, nil
		}
	}
}

func (s *selection) Close() error {
	return s.iter.Close()
}

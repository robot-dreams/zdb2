package stream

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/robot-dreams/zdb2"
)

type result struct {
	record zdb2.Record
	err    error
}

type scan struct {
	r      *bufio.Reader
	t      *zdb2.TableHeader
	closed bool
	c      io.Closer
}

var _ zdb2.Iterator = (*scan)(nil)

func NewScan(path string) (*scan, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(f)
	t, err := zdb2.ReadTableHeader(r)
	if err != nil {
		return nil, err
	}
	s := &scan{
		r: r,
		t: t,
		c: f,
	}
	return s, nil
}

func (s *scan) TableHeader() *zdb2.TableHeader {
	return s.t
}

func (s *scan) Next() (zdb2.Record, error) {
	if s.closed {
		return nil, errors.New("Cannot call Next after scan was closed")
	}
	record, err := s.t.ReadRecord(s.r)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *scan) Close() error {
	if s.closed {
		return nil
	}
	defer func() {
		s.closed = true
	}()
	return s.c.Close()
}

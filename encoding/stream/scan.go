package stream

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/robot-dreams/zdb2"
)

type result struct {
	record zdb2.Record
	err    error
}

type scan struct {
	r       *bufio.Reader
	t       *zdb2.TableHeader
	results chan *result
	closed  bool
	done    chan struct{}
	wg      *sync.WaitGroup
	c       io.Closer
}

var _ zdb2.Iterator = (*scan)(nil)

func NewScan(path string) (*scan, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(f)
	t, err := ReadTableHeader(r)
	if err != nil {
		return nil, err
	}
	s := &scan{
		r:       r,
		t:       t,
		results: make(chan *result),
		done:    make(chan struct{}),
		wg:      &sync.WaitGroup{},
		c:       f,
	}
	s.wg.Add(1)
	go s.scanRecords()
	return s, nil
}

func (s *scan) scanRecords() {
	defer s.wg.Done()
	for {
		record, err := ReadRecord(s.r, s.t)
		if err == io.EOF {
			close(s.results)
			return
		} else if err != nil {
			s.sendResult(nil, err)
			return
		}
		if !s.sendResult(record, nil) {
			return
		}
	}
}

// Returns whether the result was successfully sent.
func (s *scan) sendResult(record zdb2.Record, err error) bool {
	select {
	case <-s.done:
		return false
	case s.results <- &result{record, err}:
		return true
	}
}

func (s *scan) TableHeader() *zdb2.TableHeader {
	return s.t
}

func (s *scan) Next() (zdb2.Record, error) {
	select {
	case <-s.done:
		return nil, errors.New(
			"Next cannot be called after Iterator has been closed.")
	case result, ok := <-s.results:
		if !ok {
			return nil, io.EOF
		} else {
			return result.record, result.err
		}
	}
}

func (s *scan) Close() error {
	if s.closed {
		return nil
	}
	defer func() {
		s.closed = true
	}()
	close(s.done)
	s.wg.Wait()
	return s.c.Close()
}

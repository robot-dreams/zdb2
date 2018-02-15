package stream

import (
	"bufio"
	"io"
	"os"

	"github.com/robot-dreams/zdb2"
)

type write struct {
	w      *bufio.Writer
	t      *zdb2.TableHeader
	closed bool
	c      io.Closer
}

func NewWrite(path string, t *zdb2.TableHeader) (*write, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)
	err = writeTableHeader(w, t)
	if err != nil {
		return nil, err
	}
	return &write{
		w: w,
		t: t,
		c: f,
	}, nil
}

func (w *write) WriteRecord(record zdb2.Record) error {
	return writeRecord(w.w, w.t, record)
}

func (w *write) Close() error {
	if w.closed {
		return nil
	}
	err := w.w.Flush()
	if err != nil {
		return err
	}
	defer func() {
		w.closed = true
	}()
	return w.c.Close()
}

func WriteAll(path string, t *zdb2.TableHeader, records []zdb2.Record) error {
	w, err := NewWrite(path, t)
	if err != nil {
		return err
	}
	for _, record := range records {
		err = w.WriteRecord(record)
		if err != nil {
			return err
		}
	}
	return w.Close()
}

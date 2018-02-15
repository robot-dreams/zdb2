package executor

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"strconv"

	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2"
)

type csvScan struct {
	r      *csv.Reader
	t      *zdb2.TableHeader
	closed bool
	c      io.Closer
}

var _ zdb2.Iterator = (*csvScan)(nil)

func NewCSVScan(path string, t *zdb2.TableHeader) (*csvScan, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(bufio.NewReader(f))
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	if len(header) != len(t.Fields) {
		return nil, errors.Newf(
			"csv header %v doesn't match table header %v",
			header,
			*t)
	}
	for i := range header {
		if header[i] != t.Fields[i].Name {
			return nil, errors.Newf(
				"csv header %v doesn't match table header %v",
				header,
				*t)
		}
	}
	return &csvScan{
		r: r,
		t: t,
		c: f,
	}, nil
}

func (c *csvScan) TableHeader() *zdb2.TableHeader {
	return c.t
}

func (c *csvScan) Next() (zdb2.Record, error) {
	row, err := c.r.Read()
	if err != nil {
		return nil, err
	}
	record := make(zdb2.Record, len(row))
	for i, column := range row {
		value, err := parseValue(c.t.Fields[i].Type, column)
		if err != nil {
			return nil, err
		}
		record[i] = value
	}
	return record, nil
}

func parseValue(type_ zdb2.Type, s string) (interface{}, error) {
	switch type_ {
	case zdb2.Int32:
		x, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(x), nil
	case zdb2.Float64:
		x, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return x, nil
	case zdb2.String:
		return s, nil
	default:
		return nil, errors.Newf("Unsupported type %v", type_)
	}
}

func (c *csvScan) Close() error {
	if c.closed {
		return nil
	}
	defer func() {
		c.closed = true
	}()
	return c.c.Close()
}

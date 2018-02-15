package executor

import (
	"io"

	"github.com/robot-dreams/zdb2"
)

type inMemoryScan struct {
	t       *zdb2.TableHeader
	records []zdb2.Record
}

var _ zdb2.Iterator = (*inMemoryScan)(nil)

func NewInMemoryScan(t *zdb2.TableHeader, records []zdb2.Record) *inMemoryScan {
	return &inMemoryScan{
		t:       t,
		records: records,
	}
}

func (m *inMemoryScan) TableHeader() *zdb2.TableHeader {
	return m.t
}

func (m *inMemoryScan) Next() (zdb2.Record, error) {
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

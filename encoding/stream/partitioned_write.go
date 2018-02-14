package stream

import (
	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2"
)

type partitionedWrite struct {
	ws     []*write
	closed bool
}

func NewPartitionedWrite(
	paths []string,
	t *zdb2.TableHeader,
) (*partitionedWrite, error) {
	ws := make([]*write, len(paths))
	for i, path := range paths {
		w, err := NewWrite(path, t)
		if err != nil {
			return nil, err
		}
		ws[i] = w
	}
	return &partitionedWrite{
		ws: ws,
	}, nil
}

func (p *partitionedWrite) WriteRecordToPartition(
	record zdb2.Record,
	partition int,
) error {
	if partition < 0 || partition >= len(p.ws) {
		return errors.Newf("Invalid partition %d", partition)
	}
	return p.ws[partition].WriteRecord(record)
}

func (p *partitionedWrite) Close() error {
	if p.closed {
		return nil
	}
	defer func() {
		p.closed = true
	}()
	for _, w := range p.ws {
		err := w.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

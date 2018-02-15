package executor

import "github.com/robot-dreams/zdb2"

// distinct discards duplicate Records from the input Iterator; duplicate
// records must already be grouped.
type distinct struct {
	iter       zdb2.Iterator
	lastRecord zdb2.Record
}

var _ zdb2.Iterator = (*distinct)(nil)

func NewDistinct(iter zdb2.Iterator) *distinct {
	return &distinct{
		iter: iter,
	}
}

func (d *distinct) TableHeader() *zdb2.TableHeader {
	return d.iter.TableHeader()
}

func (d *distinct) Next() (zdb2.Record, error) {
	for {
		record, err := d.iter.Next()
		if err != nil {
			return nil, err
		}
		if !record.Equals(d.lastRecord) {
			d.lastRecord = record
			return record, nil
		}
	}
}

func (d *distinct) Close() error {
	return d.iter.Close()
}

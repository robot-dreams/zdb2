package executor

import (
	"io"
	"sort"

	"github.com/robot-dreams/zdb2"
)

type sortInMemory struct {
	iter          zdb2.Iterator
	sortedRecords []zdb2.Record
}

var _ zdb2.Iterator = (*sortInMemory)(nil)

func NewSortInMemory(
	iter zdb2.Iterator,
	sortField string,
	descending bool,
) (*sortInMemory, error) {
	t := iter.TableHeader()
	sortFieldPosition, sortFieldType := zdb2.MustFieldPositionAndType(t, sortField)
	records, err := zdb2.ReadAll(iter)
	if err == io.EOF {
		records = nil
	} else if err != nil {
		return nil, err
	}
	sort.Sort(&byField{
		sortFieldPosition: sortFieldPosition,
		sortFieldType:     sortFieldType,
		descending:        descending,
		records:           records,
	})
	return &sortInMemory{
		iter:          iter,
		sortedRecords: records,
	}, nil
}

func (s *sortInMemory) TableHeader() *zdb2.TableHeader {
	return s.iter.TableHeader()
}

func (s *sortInMemory) Next() (zdb2.Record, error) {
	if len(s.sortedRecords) == 0 {
		return nil, io.EOF
	}
	record := s.sortedRecords[0]
	s.sortedRecords = s.sortedRecords[1:]
	return record, nil
}

func (s *sortInMemory) Close() error {
	s.sortedRecords = nil
	return s.iter.Close()
}

package executor

import (
	"container/heap"
	"io"

	"github.com/robot-dreams/zdb2"
)

type iterWithRecord struct {
	iter   zdb2.Iterator
	record zdb2.Record
}

// merge takes a listed of sorted Iterators as input and returns a single stream
// of Records in totally sorted order.
type merge struct {
	inputs            []*iterWithRecord
	t                 *zdb2.TableHeader
	sortFieldPosition int
	sortFieldType     zdb2.Type
	descending        bool

	// We keep track of these so we can close them when the merge is closed.
	exhaustedIters []zdb2.Iterator
}

var _ heap.Interface = (*merge)(nil)

var _ zdb2.Iterator = (*merge)(nil)

func NewMerge(
	iters []zdb2.Iterator,
	t *zdb2.TableHeader,
	sortField string,
	descending bool,
) (*merge, error) {
	inputs := make([]*iterWithRecord, 0, len(iters))
	exhaustedIters := make([]zdb2.Iterator, 0, len(iters))
	for _, iter := range iters {
		record, err := iter.Next()
		if err == io.EOF {
			exhaustedIters = append(exhaustedIters, iter)
		} else if err != nil {
			return nil, err
		} else {
			inputs = append(inputs, &iterWithRecord{iter, record})
		}
	}
	sortFieldPosition, sortFieldType := zdb2.MustFieldPositionAndType(t, sortField)
	m := &merge{
		inputs:            inputs,
		t:                 t,
		sortFieldPosition: sortFieldPosition,
		sortFieldType:     sortFieldType,
		descending:        descending,
		exhaustedIters:    exhaustedIters,
	}
	heap.Init(m)
	return m, nil
}

func (m *merge) Len() int {
	return len(m.inputs)
}

func (m *merge) Swap(i, j int) {
	m.inputs[i], m.inputs[j] = m.inputs[j], m.inputs[i]
}

func (m *merge) Less(i, j int) bool {
	v1 := m.inputs[i].record[m.sortFieldPosition]
	v2 := m.inputs[j].record[m.sortFieldPosition]
	if m.descending {
		return zdb2.Less(m.sortFieldType, v2, v1)
	} else {
		return zdb2.Less(m.sortFieldType, v1, v2)
	}
}

func (m *merge) Push(x interface{}) {
	m.inputs = append(m.inputs, x.(*iterWithRecord))
}

func (m *merge) Pop() interface{} {
	i := len(m.inputs) - 1
	result := m.inputs[i]
	m.inputs = m.inputs[:i]
	return result
}

func (m *merge) TableHeader() *zdb2.TableHeader {
	return m.t
}

func (m *merge) Next() (zdb2.Record, error) {
	if m.Len() == 0 {
		return nil, io.EOF
	}
	i := heap.Pop(m).(*iterWithRecord)
	nextRecord, err := i.iter.Next()
	if err == io.EOF {
		m.exhaustedIters = append(m.exhaustedIters, i.iter)
	} else if err != nil {
		return nil, err
	} else {
		heap.Push(m, &iterWithRecord{i.iter, nextRecord})
	}
	return i.record, nil
}

func (m *merge) Close() error {
	for _, input := range m.inputs {
		err := input.iter.Close()
		if err != nil {
			return err
		}
	}
	for _, iter := range m.exhaustedIters {
		err := iter.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

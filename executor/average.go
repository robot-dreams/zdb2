package executor

import (
	"fmt"
	"io"

	"github.com/robot-dreams/zdb2"
)

// average computes the average over groups of Records; the input Iterator must
// already be grouped.
type average struct {
	iter                 zdb2.Iterator
	averageTableHeader   *zdb2.TableHeader
	averageFieldPosition int
	averageFieldType     zdb2.Type
	groupFieldPosition   int
	nextRecord           zdb2.Record
}

var _ zdb2.Iterator = (*average)(nil)

func NewAverage(
	iter zdb2.Iterator,
	averageFieldName string,
	groupFieldName string,
) (*average, error) {
	t := iter.TableHeader()
	averageFieldPosition, averageFieldType := zdb2.MustFieldPositionAndType(
		t, averageFieldName)
	groupFieldPosition, _ := zdb2.MustFieldPositionAndType(
		t, groupFieldName)
	name := fmt.Sprintf("average(%v.%v)", t.Name, averageFieldName)
	record, err := iter.Next()
	if err == io.EOF {
		record = nil
	} else if err != nil {
		return nil, err
	}
	return &average{
		iter: iter,
		averageTableHeader: &zdb2.TableHeader{
			Name: name,
			Fields: []*zdb2.Field{
				{groupFieldName, t.Fields[groupFieldPosition].Type},
				{"average", zdb2.Float64},
			},
		},
		averageFieldPosition: averageFieldPosition,
		averageFieldType:     averageFieldType,
		groupFieldPosition:   groupFieldPosition,
		nextRecord:           record,
	}, nil
}

func (a *average) TableHeader() *zdb2.TableHeader {
	return a.averageTableHeader
}

func (a *average) Next() (zdb2.Record, error) {
	if a.nextRecord == nil {
		return nil, io.EOF
	}
	currentGroup := a.nextRecord[a.groupFieldPosition]
	sum := zdb2.CoerceToFloat64(
		a.averageFieldType,
		a.nextRecord[a.averageFieldPosition])
	count := 1
	a.nextRecord = nil
	for {
		record, err := a.iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		} else if record[a.groupFieldPosition] != currentGroup {
			a.nextRecord = record
			break
		}
		sum += zdb2.CoerceToFloat64(
			a.averageFieldType,
			record[a.averageFieldPosition])
		count++
	}
	return zdb2.Record{currentGroup, sum / float64(count)}, nil
}

func (a *average) Close() error {
	a.nextRecord = nil
	return a.iter.Close()
}

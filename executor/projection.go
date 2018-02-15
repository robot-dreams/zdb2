package executor

import (
	"fmt"
	"strings"

	"github.com/robot-dreams/zdb2"
)

// projection returns Records from the input Iterator, but only restricted to
// the specified fields.  If a specified field does not appear in the input,
// then it will not appear in the output either.
type projection struct {
	iter                  zdb2.Iterator
	projectionTableHeader *zdb2.TableHeader
	fieldPositions        []int
}

var _ zdb2.Iterator = (*projection)(nil)

func NewProjection(iter zdb2.Iterator, fieldNames []string) *projection {
	t := iter.TableHeader()
	name := fmt.Sprintf("projection(%v, [%v])", t.Name, strings.Join(fieldNames, ","))
	fieldPositions := make([]int, len(fieldNames))
	for i, fieldName := range fieldNames {
		fieldPosition, _ := zdb2.MustFieldPositionAndType(t, fieldName)
		fieldPositions[i] = fieldPosition
	}
	fields := make([]*zdb2.Field, len(fieldPositions))
	for i, fieldPosition := range fieldPositions {
		fields[i] = t.Fields[fieldPosition]
	}
	return &projection{
		iter: iter,
		projectionTableHeader: &zdb2.TableHeader{
			Name:   name,
			Fields: fields,
		},
		fieldPositions: fieldPositions,
	}
}

func (p *projection) TableHeader() *zdb2.TableHeader {
	return p.projectionTableHeader
}

func (p *projection) Next() (zdb2.Record, error) {
	record, err := p.iter.Next()
	if err != nil {
		return nil, err
	}
	projectedRecord := make(zdb2.Record, len(p.fieldPositions))
	for i, fieldPosition := range p.fieldPositions {
		projectedRecord[i] = record[fieldPosition]
	}
	return projectedRecord, nil
}

func (p *projection) Close() error {
	return p.iter.Close()
}

package zdb2

import (
	"fmt"

	"github.com/dropbox/godropbox/errors"
)

func JoinedRecord(r1, r2 Record) Record {
	result := make(Record, 0, len(r1)+len(r2))
	result = append(result, r1...)
	result = append(result, r2...)
	return result
}

func JoinedHeader(
	t1 *TableHeader,
	t2 *TableHeader,
	joinField1 string,
	joinField2 string,
) (*TableHeader, error) {
	if !hasField(t1, joinField1) {
		return nil, errors.Newf("%v does not have field %v", *t1, joinField1)
	}
	if !hasField(t2, joinField2) {
		return nil, errors.Newf("%v does not have field %v", *t2, joinField2)
	}
	joinedName := fmt.Sprintf(
		"(%s.%s = %s.%s)", t1.Name, joinField1, t2.Name, joinField2)
	return &TableHeader{
		Name:   joinedName,
		Fields: append(qualifiedFields(t1), qualifiedFields(t2)...),
	}, nil
}

func hasField(t *TableHeader, fieldName string) bool {
	for _, field := range t.Fields {
		if field.Name == fieldName {
			return true
		}
	}
	return false
}

// Prepends the table name and "." to each field name for disambiguation.  For
// example, "id" in the "user" table becomes "user.id".
func qualifiedFields(t *TableHeader) []*Field {
	result := make([]*Field, 0, len(t.Fields))
	for _, field := range t.Fields {
		result = append(
			result,
			&Field{
				Name: t.Name + "." + field.Name,
				Type: field.Type,
			})
	}
	return result
}

func MustFieldIndexAndType(t *TableHeader, fieldName string) (int, Type) {
	for i, field := range t.Fields {
		if field.Name == fieldName {
			return i, field.Type
		}
	}
	panic(errors.Newf("%v does not have field %v", *t, fieldName))
}

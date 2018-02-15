package zdb2

import (
	"strings"

	"github.com/dropbox/godropbox/errors"
)

func FieldEquals(t *TableHeader, fieldName string, value interface{}) Predicate {
	fieldPosition, _ := MustFieldPositionAndType(t, fieldName)
	return func(record Record) bool {
		return record[fieldPosition] == value
	}
}

func FieldLess(t *TableHeader, fieldName string, value interface{}) Predicate {
	fieldPosition, fieldType := MustFieldPositionAndType(t, fieldName)
	switch fieldType {
	case Int32:
		x := value.(int32)
		return func(record Record) bool {
			return record[fieldPosition].(int32) < x
		}
	case Float64:
		x := value.(float64)
		return func(record Record) bool {
			return record[fieldPosition].(float64) < x
		}
	case String:
		s := value.(string)
		return func(record Record) bool {
			return strings.Compare(record[fieldPosition].(string), s) < 0
		}
	default:
		panic(errors.Newf("Unsupported type %v", fieldType))
	}
}

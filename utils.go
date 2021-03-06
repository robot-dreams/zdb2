package zdb2

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dropbox/godropbox/errors"
	. "github.com/dropbox/godropbox/gocheck2"
	. "gopkg.in/check.v1"
)

func CoerceToFloat64(type_ Type, value interface{}) float64 {
	switch type_ {
	case Int32:
		return float64(value.(int32))
	case Float64:
		return value.(float64)
	case String:
		x, err := strconv.ParseFloat(value.(string), 64)
		if err != nil {
			panic(err)
		}
		return x
	default:
		panic(errors.Newf("Unsupported type %v", type_))
	}
}

func Less(type_ Type, v1 interface{}, v2 interface{}) bool {
	switch type_ {
	case Int32:
		return v1.(int32) < v2.(int32)
	case Float64:
		return v1.(float64) < v2.(float64)
	case String:
		return strings.Compare(v1.(string), v2.(string)) < 0
	default:
		panic(errors.Newf("Unsupported type %v", type_))
	}
}

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
		"join(%s.%s = %s.%s)", t1.Name, joinField1, t2.Name, joinField2)
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

func MustFieldPositionAndType(t *TableHeader, fieldName string) (int, Type) {
	for i, field := range t.Fields {
		if field.Name == fieldName {
			return i, field.Type
		}
	}
	panic(errors.Newf("%v does not have field %v", *t, fieldName))
}

func ReadAll(iter Iterator) ([]Record, error) {
	var records []Record
	for {
		record, err := iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		} else {
			records = append(records, record)
		}
	}
	if len(records) == 0 {
		return nil, io.EOF
	} else {
		return records, nil
	}
}

// CheckIterator should only be used in tests.
func CheckIterator(c *C, iter Iterator, expected []Record) {
	// Ensure that the Iterator contains exactly the expected Records.
	for _, record := range expected {
		actual, err := iter.Next()
		c.Assert(err, IsNil)
		c.Assert(actual.Equals(record), IsTrue)
	}
	_, err := iter.Next()
	c.Assert(err, Equals, io.EOF)
	// Repeated calls to Next should continue to return io.EOF after the
	// reaching the end of the Iterator.
	_, err = iter.Next()
	c.Assert(err, Equals, io.EOF)
	_, err = iter.Next()
	c.Assert(err, Equals, io.EOF)
	// Repeated calls to Close should be handled properly.
	err = iter.Close()
	c.Assert(err, IsNil)
	err = iter.Close()
	c.Assert(err, IsNil)
}

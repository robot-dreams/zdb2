package zdb2

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

type PredicatesSuite struct{}

var _ = Suite(&PredicatesSuite{})

func (s *PredicatesSuite) TestMustFieldPositionAndType(c *C) {
	t := &TableHeader{
		Name: "users",
		Fields: []*Field{
			{"id", Int32},
			{"name", String},
		},
	}
	equals := FieldEquals(t, "id", int32(5))
	c.Assert(equals(Record{int32(5), "Susan Calvin"}), IsTrue)
	c.Assert(equals(Record{int32(6), "Daneel Olivaw"}), IsFalse)
	less := FieldLess(t, "id", int32(6))
	c.Assert(less(Record{int32(5), "Susan Calvin"}), IsTrue)
	c.Assert(less(Record{int32(6), "Daneel Olivaw"}), IsFalse)
}

package executor

import (
	"github.com/robot-dreams/zdb2"
	. "gopkg.in/check.v1"
)

type DistinctSuite struct{}

var _ = Suite(&DistinctSuite{})

func (s *DistinctSuite) TestDistinct(c *C) {
	t := &zdb2.TableHeader{
		Name: "users",
		Fields: []*zdb2.Field{
			{"first_name", zdb2.String},
			{"last_name", zdb2.String},
			{"username", zdb2.String},
		},
	}
	input := []zdb2.Record{
		{"Rob", "Pike", "rob"},
		{"Rob", "Pike", "rob"},
		{"Robert", "Griesemer", "gri"},
	}
	distinct := NewDistinct(NewInMemoryScan(t, input))
	expected := []zdb2.Record{
		{"Rob", "Pike", "rob"},
		{"Robert", "Griesemer", "gri"},
	}
	zdb2.CheckIterator(c, distinct, expected)
}

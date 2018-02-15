package executor

import (
	"github.com/robot-dreams/zdb2"
	. "gopkg.in/check.v1"
)

type LimitSuite struct{}

var _ = Suite(&LimitSuite{})

func (s *LimitSuite) TestLimit(c *C) {
	t := &zdb2.TableHeader{
		Name: "users",
		Fields: []*zdb2.Field{
			{"first_name", zdb2.String},
			{"last_name", zdb2.String},
			{"username", zdb2.String},
		},
	}
	records := []zdb2.Record{
		{"Rob", "Pike", "rob"},
		{"Ken", "Thompson", "ken"},
		{"Robert", "Griesemer", "gri"},
	}
	limit := NewLimit(NewInMemoryScan(t, records), 2)
	expected := []zdb2.Record{
		{"Rob", "Pike", "rob"},
		{"Ken", "Thompson", "ken"},
	}
	zdb2.CheckIterator(c, limit, expected)

	// If the limit is greater than the number of Records in the input
	// Iterator, then all elements should be returned.
	limit = NewLimit(NewInMemoryScan(t, records), 4)
	zdb2.CheckIterator(c, limit, records)
}

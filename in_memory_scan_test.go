package zdb2

import (
	. "gopkg.in/check.v1"
)

type InMemoryScanSuite struct{}

var _ = Suite(&InMemoryScanSuite{})

func (s *InMemoryScanSuite) TestInMemoryScan(c *C) {
	t := &TableHeader{
		Name: "users",
		Fields: []*Field{
			{"id", Int32},
			{"name", String},
		},
	}
	records := []Record{
		{1, "ewd"},
		{2, "dmr"},
		{3, "rob"},
		{4, "ken"},
		{5, "gri"},
	}
	CheckIterator(c, NewInMemoryScan(t, records), records)
}

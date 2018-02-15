package executor

import (
	"github.com/robot-dreams/zdb2"
	. "gopkg.in/check.v1"
)

type InMemoryScanSuite struct{}

var _ = Suite(&InMemoryScanSuite{})

func (s *InMemoryScanSuite) TestInMemoryScan(c *C) {
	t := &zdb2.TableHeader{
		Name: "users",
		Fields: []*zdb2.Field{
			{"id", zdb2.Int32},
			{"name", zdb2.String},
		},
	}
	records := []zdb2.Record{
		{1, "ewd"},
		{2, "dmr"},
		{3, "rob"},
		{4, "ken"},
		{5, "gri"},
	}
	zdb2.CheckIterator(c, NewInMemoryScan(t, records), records)
}

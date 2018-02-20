package executor

import (
	"github.com/robot-dreams/zdb2"
	. "gopkg.in/check.v1"
)

type SelectionSuite struct{}

var _ = Suite(&SelectionSuite{})

func (s *SelectionSuite) TestSelection(c *C) {
	t := &zdb2.TableHeader{
		Name: "users",
		Fields: []*zdb2.Field{
			{"id", zdb2.Int32},
			{"first_name", zdb2.String},
			{"last_name", zdb2.String},
			{"username", zdb2.String},
		},
	}
	records := []zdb2.Record{
		{int32(0), "Rob", "Pike", "rob"},
		{int32(1), "Ken", "Thompson", "ken"},
		{int32(2), "Robert", "Griesemer", "gri"},
	}
	selection := NewSelection(
		zdb2.NewInMemoryScan(t, records),
		zdb2.FieldEquals(t, "last_name", "Thompson"))
	expected := []zdb2.Record{
		{int32(1), "Ken", "Thompson", "ken"},
	}
	zdb2.CheckIterator(c, selection, expected)
	selection = NewSelection(
		zdb2.NewInMemoryScan(t, records),
		zdb2.FieldLess(t, "id", int32(2)))
	expected = []zdb2.Record{
		{int32(0), "Rob", "Pike", "rob"},
		{int32(1), "Ken", "Thompson", "ken"},
	}
	zdb2.CheckIterator(c, selection, expected)
}

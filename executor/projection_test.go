package executor

import (
	"github.com/robot-dreams/zdb2"
	. "gopkg.in/check.v1"
)

type ProjectionSuite struct{}

var _ = Suite(&ProjectionSuite{})

func (s *ProjectionSuite) TestProjection(c *C) {
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
	projection := NewProjection(
		NewInMemoryScan(t, records),
		[]string{"first_name", "username"})
	c.Assert(
		projection.TableHeader(),
		DeepEquals,
		&zdb2.TableHeader{
			Name: "projection(users, [first_name,username])",
			Fields: []*zdb2.Field{
				{"first_name", zdb2.String},
				{"username", zdb2.String},
			},
		})
	expected := []zdb2.Record{
		{"Rob", "rob"},
		{"Ken", "ken"},
		{"Robert", "gri"},
	}
	zdb2.CheckIterator(c, projection, expected)
}

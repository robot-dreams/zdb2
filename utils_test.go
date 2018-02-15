package zdb2

import (
	. "gopkg.in/check.v1"
)

type UtilsSuite struct{}

var _ = Suite(&UtilsSuite{})

func (s *UtilsSuite) TestJoinedHeader(c *C) {
	t1 := &TableHeader{
		Name: "users",
		Fields: []*Field{
			{"id", Int32},
			{"name", String},
		},
	}
	t2 := &TableHeader{
		Name: "logins",
		Fields: []*Field{
			{"user_id", Int32},
			{"timestamp", Int32},
			{"client", String},
		},
	}
	joined, err := JoinedHeader(t1, t2, "id", "user_id")
	c.Assert(err, IsNil)
	c.Assert(
		joined,
		DeepEquals,
		&TableHeader{
			Name: "(users.id = logins.user_id)",
			Fields: []*Field{
				{"users.id", Int32},
				{"users.name", String},
				{"logins.user_id", Int32},
				{"logins.timestamp", Int32},
				{"logins.client", String},
			},
		})
}

func (s *UtilsSuite) TestFieldPosition(c *C) {
	t := &TableHeader{
		Name: "users",
		Fields: []*Field{
			{"id", Int32},
			{"name", String},
		},
	}
	c.Assert(t.FieldPosition("id"), Equals, 0)
	c.Assert(t.FieldPosition("name"), Equals, 1)
	c.Assert(t.FieldPosition("birthday"), Equals, -1)
}

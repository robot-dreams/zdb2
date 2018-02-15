package executor

import (
	"io"

	"github.com/robot-dreams/zdb2"
	. "gopkg.in/check.v1"
)

type HybridHashJoinSuite struct{}

var _ = Suite(&HybridHashJoinSuite{})

func (s *HybridHashJoinSuite) TestHybridHashJoin(c *C) {
	userTable := &zdb2.TableHeader{
		Name: "user",
		Fields: []*zdb2.Field{
			{"username", zdb2.String},
			{"id", zdb2.Int32},
		},
	}
	userRecords := []zdb2.Record{
		{"esr", int32(0)},
		{"rob", int32(1)},
		{"ken", int32(2)},
		{"gri", int32(3)},
		{"dmr", int32(4)},
		{"ewd", int32(5)},
		{"rms", int32(6)},
	}
	loginTable := &zdb2.TableHeader{
		Name: "login",
		Fields: []*zdb2.Field{
			{"user_id", zdb2.Int32},
			{"timestamp", zdb2.Int32},
		},
	}
	var loginRecords []zdb2.Record
	for ts := int32(0); ts < int32(100); ts++ {
		loginRecords = append(loginRecords, zdb2.Record{ts % 7, ts})
	}
	joined, err := NewHybridHashJoin(
		NewInMemoryScan(userTable, userRecords),
		NewInMemoryScan(loginTable, loginRecords),
		"id",
		"user_id",
		0.3,
		3)
	for i := 0; i < len(loginRecords); i++ {
		record, err := joined.Next()
		c.Assert(err, IsNil)
		// id == user_id
		c.Assert(record[1], Equals, record[2])
	}
	_, err = joined.Next()
	c.Assert(err, Equals, io.EOF)
	err = joined.Close()
	c.Assert(err, IsNil)

	// Make sure we can handle duplicates.
	userRecords = append(userRecords, userRecords...)
	joined, err = NewHybridHashJoin(
		NewInMemoryScan(userTable, userRecords),
		NewInMemoryScan(loginTable, loginRecords),
		"id",
		"user_id",
		0.3,
		3)
	// There should be 2x as many records now, since each login record
	// joins with two user records.
	for i := 0; i < 2*len(loginRecords); i++ {
		record, err := joined.Next()
		c.Assert(err, IsNil)
		// id == user_id
		c.Assert(record[1], Equals, record[2])
	}
	_, err = joined.Next()
	c.Assert(err, Equals, io.EOF)
	err = joined.Close()
	c.Assert(err, IsNil)
}

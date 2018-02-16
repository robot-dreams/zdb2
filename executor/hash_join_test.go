package executor

import (
	"io"

	"github.com/robot-dreams/zdb2"
	. "gopkg.in/check.v1"
)

type HashJoinSuite struct{}

var _ = Suite(&HashJoinSuite{})

type hashJoinConstructor func(
	r, s zdb2.Iterator,
	rJoinField, sJoinField string,
) (zdb2.Iterator, error)

func runHashJoinTest(c *C, newHashJoin hashJoinConstructor) {
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
	joined, err := newHashJoin(
		NewInMemoryScan(userTable, userRecords),
		NewInMemoryScan(loginTable, loginRecords),
		"id",
		"user_id")
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
	joined, err = newHashJoin(
		NewInMemoryScan(userTable, userRecords),
		NewInMemoryScan(loginTable, loginRecords),
		"id",
		"user_id")
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

func (s *HashJoinSuite) TestHashJoin(c *C) {
	for _, newHashJoin := range []hashJoinConstructor{
		func(r, s zdb2.Iterator, rJoinField, sJoinField string) (zdb2.Iterator, error) {
			return NewHashJoinClassic(r, s, rJoinField, sJoinField)
		},
		func(r, s zdb2.Iterator, rJoinField, sJoinField string) (zdb2.Iterator, error) {
			return NewHashJoinHybrid(r, s, rJoinField, sJoinField, true, 0.3, 3)
		},
	} {
		runHashJoinTest(c, newHashJoin)
	}
}

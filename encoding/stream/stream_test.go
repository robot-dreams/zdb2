package stream

import (
	. "gopkg.in/check.v1"

	"github.com/robot-dreams/zdb2"
)

type StreamSuite struct{}

var _ = Suite(&StreamSuite{})

func (s *StreamSuite) TestStream(c *C) {
	expectedTableHeader := &zdb2.TableHeader{
		Name: "movies",
		Fields: []*zdb2.Field{
			{"title", zdb2.String},
			{"rating", zdb2.Float64},
			{"views", zdb2.Int32},
		},
	}
	expectedRecords := []zdb2.Record{
		{"Leon: The Professional", 4.6, int32(2)},
		{"Gattaca", 4.5, int32(2)},
		{"Hackers", 3.7, int32(3)},
		{"Inside Out", 4.7, int32(3)},
	}

	// Persist the Table to a file.
	path := c.MkDir() + "/movies.zt"
	recordWriter, err := NewWrite(path, expectedTableHeader)
	c.Assert(err, IsNil)
	for _, record := range expectedRecords {
		err = recordWriter.WriteRecord(record)
		c.Assert(err, IsNil)
	}
	err = recordWriter.Close()
	c.Assert(err, IsNil)

	// The Records we read back should match the Records we wrote.
	// Using a scan should produce the same result.
	scan, err := NewScan(path)
	c.Assert(err, IsNil)
	zdb2.CheckIterator(c, scan, expectedRecords)
}

package stream

import (
	"bufio"
	"io"
	"os"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	"github.com/robot-dreams/zdb2"
)

type StreamSuite struct{}

var _ = Suite(&StreamSuite{})

func (s *StreamSuite) TestTable(c *C) {
	expectedHeader := &Header{
		Name: "movies",
		FieldHeaders: []*FieldHeader{
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
	f, err := os.Create(path)
	c.Assert(err, IsNil)
	w := bufio.NewWriter(f)
	err = expectedHeader.Write(w)
	c.Assert(err, IsNil)
	for _, record := range expectedRecords {
		err = expectedHeader.WriteRecord(w, record)
		c.Assert(err, IsNil)
	}
	err = w.Flush()
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err, IsNil)

	// The Records we read back should match the Records we wrote.
	f, err = os.Open(path)
	c.Assert(err, IsNil)
	r := bufio.NewReader(f)
	header, err := ReadHeader(r)
	c.Assert(err, IsNil)
	c.Assert(header, DeepEquals, expectedHeader)
	for _, expected := range expectedRecords {
		record, err := header.ReadRecord(r)
		c.Assert(err, IsNil)
		c.Assert(record.Equals(expected), IsTrue)
	}
	_, err = header.ReadRecord(r)
	c.Assert(err, Equals, io.EOF)
}

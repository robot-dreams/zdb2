package stream

import (
	"strconv"

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

	// Persist the table to a file.
	path := c.MkDir() + "/movies.zt"
	w, err := NewWrite(path, expectedTableHeader)
	c.Assert(err, IsNil)
	for _, record := range expectedRecords {
		err = w.WriteRecord(record)
		c.Assert(err, IsNil)
	}
	err = w.Close()
	c.Assert(err, IsNil)

	// The Records we read back should match the Records we wrote.
	scan, err := NewScan(path)
	c.Assert(err, IsNil)
	zdb2.CheckIterator(c, scan, expectedRecords)
}

func (s *StreamSuite) TestPartitionedWrite(c *C) {
	expectedTableHeader := &zdb2.TableHeader{
		Name: "movies",
		Fields: []*zdb2.Field{
			{"title", zdb2.String},
			{"rating", zdb2.Float64},
		},
	}
	expectedRecords := []zdb2.Record{
		{"The Shawshank Redemption", 9.3},
		{"The Godfather", 9.2},
		{"The Dark Knight", 9.0},
		{"The Godfather: Part II", 9.0},
		{"Pulp Fiction", 8.9},
		{"Schindler's List", 8.9},
		{"The Lord of the Rings: The Return of the King", 8.9},
		{"12 Angry Men", 8.9},
		{"The Good, the Bad and the Ugly", 8.9},
		{"The Lord of the Rings: The Fellowship of the Ring", 8.8},
	}
	numPartitions := 3
	dir := c.MkDir()
	paths := make([]string, numPartitions)
	for i := 0; i < numPartitions; i++ {
		paths[i] = dir + "/movies-" + strconv.Itoa(i)
	}
	partitionedWriter, err := NewPartitionedWrite(paths, expectedTableHeader)
	c.Assert(err, IsNil)
	for i, record := range expectedRecords {
		err = partitionedWriter.WriteRecordToPartition(record, i%numPartitions)
		c.Assert(err, IsNil)
	}
	err = partitionedWriter.Close()
	c.Assert(err, IsNil)
	for i := 0; i < numPartitions; i++ {
		var expectedPartitionRecords []zdb2.Record
		for j, record := range expectedRecords {
			if j%numPartitions == i {
				expectedPartitionRecords = append(expectedPartitionRecords, record)
			}
		}
		scan, err := NewScan(paths[i])
		c.Assert(err, IsNil)
		zdb2.CheckIterator(c, scan, expectedPartitionRecords)
	}
}

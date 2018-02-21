package heap_file

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	"github.com/dropbox/godropbox/math2/rand2"
	"github.com/robot-dreams/zdb2"
)

type HeapFileSuite struct{}

var _ = Suite(&HeapFileSuite{})

func (s *HeapFileSuite) TestCreateAndOpen(c *C) {
	path := c.MkDir() + "/heap_file_test"
	expectedTableHeader := &zdb2.TableHeader{
		Name: "movies",
		Fields: []*zdb2.Field{
			{"title", zdb2.String},
			{"rating", zdb2.Float64},
			{"views", zdb2.Int32},
		},
	}
	hf, err := NewHeapFile(path, expectedTableHeader)
	c.Assert(err, IsNil)
	// Calling Close() multiple times is valid.
	err = hf.Close()
	c.Assert(err, IsNil)
	err = hf.Close()
	c.Assert(err, IsNil)

	// Trying to create a new heap file at an existing path should fail.
	hf, err = NewHeapFile(path, expectedTableHeader)
	c.Assert(err, NotNil)

	// Trying to open a heap file at a nonexistent path should fail.
	hf, err = OpenHeapFile(path + "_nonexistent")
	c.Assert(err, NotNil)

	hf, err = OpenHeapFile(path)
	c.Assert(err, IsNil)
	c.Assert(hf.lastPage.t, DeepEquals, expectedTableHeader)
	// Calling Close() multiple times is valid.
	err = hf.Close()
	c.Assert(err, IsNil)
	err = hf.Close()
	c.Assert(err, IsNil)
}

func (s *HeapFileSuite) TestHeapFile(c *C) {
	path := c.MkDir() + "/heap_file_test"
	t := &zdb2.TableHeader{
		Name: "movies",
		Fields: []*zdb2.Field{
			{"title", zdb2.String},
			{"rating", zdb2.Float64},
			{"views", zdb2.Int32},
		},
	}
	hf, err := NewHeapFile(path, t)
	c.Assert(err, IsNil)

	// Define a collection of records to draw from (we're going to repeat these
	// a lot).
	records := []zdb2.Record{
		{"Leon: The Professional", 4.6, int32(2)},
		{"Gattaca", 4.5, int32(2)},
		{"Hackers", 3.7, int32(3)},
		{"Inside Out", 4.7, int32(3)},
	}

	// Insert a lot of records.
	numToInsert := 5000
	expectedRecords := make([]zdb2.Record, numToInsert)
	recordIDs := make([]zdb2.RecordID, numToInsert)
	for i := 0; i < numToInsert; i++ {
		record := records[i%len(records)]
		recordID, err := hf.Insert(record)
		c.Assert(err, IsNil)
		expectedRecords[i] = record
		recordIDs[i] = recordID
	}

	// Make sure we've added enough test data to check an "interesting" case.
	c.Assert(hf.bf.NumBlocks > 2, IsTrue)

	// Make sure that some of the records we've added can be found.
	start := rand2.Intn(len(recordIDs) - 10)
	for i := start; i < start+10; i++ {
		record, err := hf.Get(recordIDs[i])
		c.Assert(err, IsNil)
		c.Assert(record.Equals(records[i%len(records)]), IsTrue)
	}

	// Delete these records.
	for i := start; i < start+10; i++ {
		err = hf.Delete(recordIDs[i])
		c.Assert(err, IsNil)

		// Deletes should be idempotent.
		err = hf.Delete(recordIDs[i])
		c.Assert(err, IsNil)
	}

	// Account for the deletes (we'll be checking against expectedRecords).
	expectedRecords = append(
		expectedRecords[:start],
		expectedRecords[start+10:]...)

	// Trying to load deleted records should return (nil, nil).
	for i := start; i < start+10; i++ {
		record, err := hf.Get(recordIDs[i])
		c.Assert(err, IsNil)
		c.Assert(record, IsNil)
	}

	// Trying to load a record that doesn't exist should fail.
	_, err = hf.Get(zdb2.RecordID{
		PageID: 0,
		SlotID: 31337,
	})
	c.Assert(err, NotNil)
	_, err = hf.Get(zdb2.RecordID{
		PageID: 31337,
		SlotID: 0,
	})
	c.Assert(err, NotNil)

	// Flush all updates to disk.
	err = hf.Close()
	c.Assert(err, IsNil)

	// Make sure that scanning also works as expected.
	heapFileScan, err := NewFileScan(path)
	c.Assert(err, IsNil)
	zdb2.CheckIterator(c, heapFileScan, expectedRecords)
}

func (s *HeapFileSuite) TestBulkLoad(c *C) {
	path := c.MkDir() + "/heap_file_test"
	t := &zdb2.TableHeader{
		Name: "movies",
		Fields: []*zdb2.Field{
			{"title", zdb2.String},
			{"rating", zdb2.Float64},
			{"views", zdb2.Int32},
		},
	}

	// Define a collection of records to draw from (we're going to repeat these
	// a lot).
	records := []zdb2.Record{
		{"Leon: The Professional", 4.6, int32(2)},
		{"Gattaca", 4.5, int32(2)},
		{"Hackers", 3.7, int32(3)},
		{"Inside Out", 4.7, int32(3)},
	}

	// Bulk load a lot of records.
	numToBulkLoad := 100000
	expectedRecords := make([]zdb2.Record, numToBulkLoad)
	for i := 0; i < numToBulkLoad; i++ {
		expectedRecords[i] = records[i%len(records)]
	}
	err := BulkLoadNewHeapFile(path, zdb2.NewInMemoryScan(t, expectedRecords))
	c.Assert(err, IsNil)
}

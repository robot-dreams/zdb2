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
	t, err := hf.lastPage.getTableHeader()
	c.Assert(err, IsNil)
	c.Assert(t, DeepEquals, expectedTableHeader)
	// Calling Close() multiple times is valid.
	err = hf.Close()
	c.Assert(err, IsNil)
	err = hf.Close()
	c.Assert(err, IsNil)
}

func (s *HeapFileSuite) TestInsertAndGet(c *C) {
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

	// Insert the expected records.
	records := []zdb2.Record{
		{"Leon: The Professional", 4.6, int32(2)},
		{"Gattaca", 4.5, int32(2)},
		{"Hackers", 3.7, int32(3)},
		{"Inside Out", 4.7, int32(3)},
	}
	numToInsert := 5000
	recordIDs := make([]zdb2.RecordID, numToInsert)
	for i := 0; i < numToInsert; i++ {
		recordID, err := hf.Insert(records[i%len(records)])
		c.Assert(err, IsNil)
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
}

func (s *HeapFileSuite) TestDelete(c *C) {
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

	// Insert the expected records.
	records := []zdb2.Record{
		{"Leon: The Professional", 4.6, int32(2)},
		{"Gattaca", 4.5, int32(2)},
		{"Hackers", 3.7, int32(3)},
		{"Inside Out", 4.7, int32(3)},
	}
	numToInsert := 5000
	recordIDs := make([]zdb2.RecordID, numToInsert)
	for i := 0; i < numToInsert; i++ {
		recordID, err := hf.Insert(records[i%len(records)])
		c.Assert(err, IsNil)
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

	// Trying to load deleted records should return (nil, nil).
	for i := start; i < start+10; i++ {
		record, err := hf.Get(recordIDs[i])
		c.Assert(err, IsNil)
		c.Assert(record, IsNil)
	}
}

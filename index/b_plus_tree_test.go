package index

import (
	. "gopkg.in/check.v1"
)

type BPlusTreeSuite struct{}

var _ = Suite(&BPlusTreeSuite{})

/*
func checkIterator(c *C, iter Iterator, expected []Entry) {
	// Ensure that the Iterator contains exactly the expected Records.
	for _, entry := range expected {
		actual, err := iter.Next()
		c.Assert(err, IsNil)
		c.Assert(actual, DeepEquals, entry)
	}
	_, err := iter.Next()
	c.Assert(err, Equals, io.EOF)
	// Repeated calls to Next should continue to return io.EOF after the
	// reaching the end of the Iterator.
	_, err = iter.Next()
	c.Assert(err, Equals, io.EOF)
	_, err = iter.Next()
	c.Assert(err, Equals, io.EOF)
}
*/

func (s *BPlusTreeSuite) TestBPlusTree(c *C) {
	path := c.MkDir() + "/b_plus_tree"
	tree, err := NewBPlusTree(path)
	c.Assert(err, IsNil)
	expectedEntry := func(key int32) Entry {
		return Entry{
			Key: key,
			RID: RecordID{
				PageID: key,
				SlotID: uint16(key),
			},
		}
	}
	d := int32(5)
	numKeys := 1000
	keys := make(map[int32]struct{})
	for i := 0; i < numKeys; i++ {
		keys[int32(i)*d] = struct{}{}
	}
	// Reading the keys out of a map randomizes the iteration order each time we
	// run the test and lets us check more cases.
	for key := range keys {
		err = tree.AddEntry(expectedEntry(key))
		c.Assert(err, IsNil)
	}
	for i := 0; i < numKeys; i++ {
		entry, err := tree.FindEqual(int32(i) * d)
		c.Assert(err, IsNil)
		c.Assert(entry, DeepEquals, expectedEntry(int32(i)*d))
	}
	err = tree.Close()
	c.Assert(err, IsNil)
}

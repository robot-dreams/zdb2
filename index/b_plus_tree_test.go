package index

import (
	"io"

	. "gopkg.in/check.v1"
)

type BPlusTreeSuite struct{}

var _ = Suite(&BPlusTreeSuite{})

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

func (s *BPlusTreeSuite) TestBPlusTree(c *C) {
	path := c.MkDir() + "/b_plus_tree"
	tree, err := OpenBPlusTree(path)
	c.Assert(err, IsNil)
	expectedEntry := func(key int32, offset int) Entry {
		return Entry{
			Key: key,
			RID: RecordID{
				PageID: int32(offset),
				SlotID: uint16(offset),
			},
		}
	}
	d := int32(5)
	numKeys := 100
	numEntriesPerKey := 10
	keys := make(map[int32]struct{})
	for i := 0; i < numKeys; i++ {
		keys[int32(i)*d] = struct{}{}
	}

	// Reading the keys out of a map randomizes the iteration order each time we
	// run the test and lets us check more cases.
	for key := range keys {
		for i := 0; i < numEntriesPerKey; i++ {
			err = tree.AddEntry(expectedEntry(key, i))
			c.Assert(err, IsNil)
		}
	}
	for i := 0; i < numKeys; i++ {
		// All of these should be found.
		iter, err := tree.FindEqual(int32(i) * d)
		c.Assert(err, IsNil)
		var expectedEntries []Entry
		for j := 0; j < numEntriesPerKey; j++ {
			expectedEntries = append(
				expectedEntries,
				expectedEntry(int32(i)*d, j))
		}
		checkIterator(c, iter, expectedEntries)

		// None of these should be found.
		iter, err = tree.FindEqual(int32(i)*d + 1)
		c.Assert(err, IsNil)
		checkIterator(c, iter, nil)
	}

	// Use FindGreaterEqual to iterate through the second half of the entries.
	start := numKeys / 2
	iter, err := tree.FindGreaterEqual(int32(start) * d)
	c.Assert(err, IsNil)
	var expectedEntries []Entry
	for i := start; i < numKeys; i++ {
		for j := 0; j < numEntriesPerKey; j++ {
			expectedEntries = append(
				expectedEntries,
				expectedEntry(int32(i)*d, j))
		}
	}
	checkIterator(c, iter, expectedEntries)

	// If the key is greater than anything ever added, then there should be no
	// entries in the returned iterator.
	iter, err = tree.FindGreaterEqual(int32(numKeys) * d)
	c.Assert(err, IsNil)
	checkIterator(c, iter, nil)

	err = tree.Close()
	c.Assert(err, IsNil)
}

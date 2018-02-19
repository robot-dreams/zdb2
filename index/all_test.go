package index

import (
	"io"
	"testing"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

func Test(t *testing.T) {
	// Use smaller block size to check more interesting cases.
	oldBlockSize := blockSize
	setBlockSize(1 << 6)
	defer setBlockSize(oldBlockSize)

	// Initialize gocheck.
	TestingT(t)
}

// Distance between keys for test entries.
var keyDelta int32 = 5

func generateTestEntry(key int32, offset int) Entry {
	return Entry{
		Key: key,
		RID: RecordID{
			PageID: int32(offset),
			SlotID: uint16(offset),
		},
	}
}

func generateSortedTestEntries(numKeys, numEntriesPerKey int) []Entry {
	testEntries := make([]Entry, 0, numKeys*numEntriesPerKey)
	for i := 0; i < numKeys; i++ {
		for j := 0; j < numEntriesPerKey; j++ {
			testEntries = append(
				testEntries,
				generateTestEntry(int32(i)*keyDelta, j))
		}
	}
	return testEntries
}

func checkTestEntries(c *C, tree *bPlusTree, numKeys int, numEntriesPerKey int) {
	for i := 0; i < numKeys; i++ {
		// All of these should be found.
		iter, err := tree.FindEqual(int32(i) * keyDelta)
		c.Assert(err, IsNil)
		var expectedEntries []Entry
		for j := 0; j < numEntriesPerKey; j++ {
			expectedEntries = append(
				expectedEntries,
				generateTestEntry(int32(i)*keyDelta, j))
		}
		checkIterator(c, iter, expectedEntries)

		// None of these should be found.
		iter, err = tree.FindEqual(int32(i)*keyDelta + 1)
		c.Assert(err, IsNil)
		checkIterator(c, iter, nil)
	}

	// Use FindGreaterEqual to iterate through the second half of the entries.
	start := numKeys / 2
	iter, err := tree.FindGreaterEqual(int32(start) * keyDelta)
	c.Assert(err, IsNil)
	var expectedEntries []Entry
	for i := start; i < numKeys; i++ {
		for j := 0; j < numEntriesPerKey; j++ {
			expectedEntries = append(
				expectedEntries,
				generateTestEntry(int32(i)*keyDelta, j))
		}
	}
	checkIterator(c, iter, expectedEntries)

	// If the key is greater than anything ever added, then there should be no
	// entries in the returned iterator.
	iter, err = tree.FindGreaterEqual(int32(numKeys) * keyDelta)
	c.Assert(err, IsNil)
	checkIterator(c, iter, nil)
}

func checkIterator(c *C, iter Iterator, expected []Entry) {
	var actual []Entry
	for {
		entry, err := iter.Next()
		if err == io.EOF {
			break
		}
		c.Assert(err, IsNil)
		actual = append(actual, entry)
	}

	// Repeated calls to Next should continue to return io.EOF after the
	// reaching the end of the Iterator.
	_, err := iter.Next()
	c.Assert(err, Equals, io.EOF)
	_, err = iter.Next()
	c.Assert(err, Equals, io.EOF)

	// The correct number of entries should have been returned.
	c.Assert(len(actual), Equals, len(expected))

	// The returned entries should be sorted by key.
	for i := 1; i < len(actual); i++ {
		c.Assert(actual[i-1].Key <= actual[i-1].Key, IsTrue)
	}

	// The set of returned entries should match the expected entries.
	expectedSet := make(map[Entry]struct{})
	for _, entry := range expected {
		expectedSet[entry] = struct{}{}
	}
	for _, entry := range actual {
		_, ok := expectedSet[entry]
		c.Assert(ok, IsTrue)
	}
}

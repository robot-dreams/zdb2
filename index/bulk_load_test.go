package index

import (
	. "gopkg.in/check.v1"
)

type BulkLoadSuite struct{}

var _ = Suite(&BulkLoadSuite{})

func (s *BulkLoadSuite) TestBulkLoad(c *C) {
	path := c.MkDir() + "/bulk_load"
	numKeys := 100
	numEntriesPerKey := 10
	sortedTestEntries := generateSortedTestEntries(numKeys, numEntriesPerKey)
	tree, err := BulkLoadNewBPlusTree(path, sortedTestEntries)
	c.Assert(err, IsNil)
	checkTestEntries(c, tree, numKeys, numEntriesPerKey)
	err = tree.Close()
	c.Assert(err, IsNil)
}

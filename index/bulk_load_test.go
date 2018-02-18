package index

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

type BulkLoadSuite struct{}

var _ = Suite(&BulkLoadSuite{})

func (s *BulkLoadSuite) TestBulkLoad(c *C) {
	numKeys := 100
	numEntriesPerKey := 10
	sortedTestEntries := generateSortedTestEntries(numKeys, numEntriesPerKey)

	prevNumBlocks := int32(0)
	for _, loadingFactor := range []float64{1.0, 0.7, 0.4, 0.3} {
		tree, err := BulkLoadNewBPlusTree(
			c.MkDir()+"/bulk_load_test",
			sortedTestEntries,
			loadingFactor)
		c.Assert(err, IsNil)
		checkTestEntries(c, tree, numKeys, numEntriesPerKey)
		err = tree.Close()
		c.Assert(err, IsNil)

		// Since we've ordered the loading factors in descending order, the
		// number of blocks should be nondecreasing.
		c.Assert(tree.bf.numBlocks >= prevNumBlocks, IsTrue)
		prevNumBlocks = tree.bf.numBlocks
	}
}

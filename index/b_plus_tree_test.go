package index

import (
	"github.com/dropbox/godropbox/math2/rand2"
	. "gopkg.in/check.v1"
)

type BPlusTreeSuite struct{}

var _ = Suite(&BPlusTreeSuite{})

type entryShuffle []Entry

var _ rand2.Swapper = (entryShuffle)(nil)

func (e entryShuffle) Len() int      { return len(e) }
func (e entryShuffle) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

func (s *BPlusTreeSuite) TestBPlusTree(c *C) {
	path := c.MkDir() + "/b_plus_tree_test"
	tree, err := OpenBPlusTree(path)
	c.Assert(err, IsNil)
	numKeys := 100
	numEntriesPerKey := 10
	testEntries := generateSortedTestEntries(numKeys, numEntriesPerKey)

	// Add test entries in random order.
	rand2.Shuffle(entryShuffle(testEntries))
	for _, entry := range testEntries {
		err = tree.AddEntry(entry)
		c.Assert(err, IsNil)
	}

	checkTestEntries(c, tree, numKeys, numEntriesPerKey)
	err = tree.Close()
	c.Assert(err, IsNil)
}

package index

import (
	"math"

	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2/block_file"
)

func min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func BulkLoadNewBPlusTree(
	path string,
	sortedEntries []Entry,
	loadingFactor float64,
) (*bPlusTree, error) {
	if len(sortedEntries) == 0 {
		return nil, errors.New("No entries to bulk load")
	}
	if loadingFactor <= 0 || loadingFactor > 1 {
		return nil, errors.Newf(
			"Loading factor must be in (0, 1]; got %v",
			loadingFactor)
	}
	if math.Floor(loadingFactor*float64(maxLeafNodeEntries)) == 0 {
		return nil, errors.Newf(
			"Loading factor %v would result in no entries per leaf node",
			loadingFactor)
	}
	bf, err := block_file.NewBlockFile(path, blockSize)
	if err != nil {
		return nil, err
	}
	if bf.NumBlocks > 0 {
		return nil, errors.Newf(
			"Cannot bulk load into non-empty B+ tree file at %v",
			path)
	}
	rootBlockID, err := bf.AllocateBlock()
	if err != nil {
		return nil, err
	}
	leafRouters, err := bulkLoadSequentialLeafNodes(
		bf,
		sortedEntries,
		loadingFactor)
	if err != nil {
		return nil, err
	}
	root := &internalNode{
		bf:               bf,
		blockID:          rootBlockID,
		subtreeHeight:    1,
		underflowBlockID: leafRouters[0].blockID,
	}
	err = root.flush()
	if err != nil {
		return nil, err
	}
	cachedRightmostPath := map[int32]*internalNode{
		0: root,
	}
	for i := 1; i < len(leafRouters); i++ {
		splitRouter, err := root.bulkLoadHelper(
			leafRouters[i],
			cachedRightmostPath)
		if err != nil {
			return nil, err
		}
		if splitRouter != nil {
			newRoot, err := handleRootSplit(bf, root, *splitRouter)
			if err != nil {
				return nil, err
			}
			cachedRightmostPath[0] = newRoot
			root = newRoot
		}
	}
	for _, in := range cachedRightmostPath {
		err = in.flush()
		if err != nil {
			return nil, err
		}
	}
	return &bPlusTree{
		bf:   bf,
		root: root,
	}, nil
}

// Preconditions:
// - Aside from the root (at blockID 0), no other nodes have been created
// - loadingFactor is in (0, 1]
func bulkLoadSequentialLeafNodes(
	bf *block_file.BlockFile,
	sortedEntries []Entry,
	loadingFactor float64,
) ([]router, error) {
	numEntriesPerLeafNode := int(math.Floor(
		loadingFactor * float64(maxLeafNodeEntries)))
	leafRouters := make(
		[]router,
		0,
		len(sortedEntries)/numEntriesPerLeafNode)
	prevDuplicateOverflow := false
	for len(sortedEntries) > 0 {
		leafNode, err := bulkLoadLeafNode(
			bf,
			sortedEntries,
			numEntriesPerLeafNode)
		if err != nil {
			return nil, err
		}
		if !prevDuplicateOverflow {
			leafRouters = append(
				leafRouters,
				router{
					key:     leafNode.sortedEntries[0].Key,
					blockID: leafNode.blockID,
				})
		}
		prevDuplicateOverflow = leafNode.duplicateOverflow
		sortedEntries = sortedEntries[len(leafNode.sortedEntries):]
	}
	return leafRouters, nil
}

// Preconditions:
// - Aside from the root (at blockID 0), no internal nodes have been created
// - All entries in remainingSortedEntries have keys greater than entries from
//   previous calls to bulkLoadLeafNode
// - numEntriesPerLeafNode is in [1, maxLeafNodeEntries]
func bulkLoadLeafNode(
	bf *block_file.BlockFile,
	remainingSortedEntries []Entry,
	numEntriesPerLeafNode int,
) (*leafNode, error) {
	var blockID int32
	var prevBlockID int32
	var nextBlockID int32
	var sortedEntries []Entry
	var duplicateOverflow bool

	blockID, err := bf.AllocateBlock()
	if err != nil {
		return nil, err
	}

	if blockID == 1 {
		prevBlockID = block_file.InvalidBlockID
	} else {
		prevBlockID = blockID - 1
	}

	if len(remainingSortedEntries) <= numEntriesPerLeafNode {
		nextBlockID = block_file.InvalidBlockID
	} else {
		nextBlockID = blockID + 1
	}

	n := min(numEntriesPerLeafNode, len(remainingSortedEntries))
	sortedEntries = remainingSortedEntries[:n]

	if len(remainingSortedEntries) <= n {
		duplicateOverflow = false
	} else if remainingSortedEntries[n-1].Key == remainingSortedEntries[n].Key {
		duplicateOverflow = true
	}

	leaf := &leafNode{
		bf:                bf,
		blockID:           blockID,
		prevBlockID:       prevBlockID,
		nextBlockID:       nextBlockID,
		sortedEntries:     sortedEntries,
		duplicateOverflow: duplicateOverflow,
	}
	err = leaf.flush()
	if err != nil {
		return nil, err
	}
	return leaf, nil
}

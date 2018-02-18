package index

import "github.com/dropbox/godropbox/errors"

func min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func BulkLoadNewBPlusTree(path string, sortedEntries []Entry) (*bPlusTree, error) {
	if len(sortedEntries) == 0 {
		return nil, errors.New("No entries to bulk load")
	}
	bf, err := newBlockFile(path)
	if err != nil {
		return nil, err
	}
	if bf.numBlocks > 0 {
		return nil, errors.Newf(
			"Cannot bulk load into non-empty B+ tree file at %v",
			path)
	}
	rootBlockID, err := bf.allocateBlock()
	if err != nil {
		return nil, err
	}
	leafRouters, err := bulkLoadSequentialLeafNodes(bf, sortedEntries)
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
	for i := 1; i < len(leafRouters); i++ {
		splitRouter, err := root.bulkLoadHelper(leafRouters[i])
		if err != nil {
			return nil, err
		}
		if splitRouter != nil {
			newRoot, err := handleRootSplit(bf, root, *splitRouter)
			if err != nil {
				return nil, err
			}
			root = newRoot
		}
	}
	return &bPlusTree{
		bf:   bf,
		root: root,
	}, nil
}

// Preconditions:
// - Aside from the root (at blockID 0), no other nodes have been created
func bulkLoadSequentialLeafNodes(
	bf *blockFile,
	sortedEntries []Entry,
) ([]router, error) {
	leafRouters := make(
		[]router,
		0,
		len(sortedEntries)/maxLeafNodeEntries)
	prevDuplicateOverflow := false
	for len(sortedEntries) > 0 {
		leafNode, err := bulkLoadLeafNode(bf, sortedEntries)
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
func bulkLoadLeafNode(
	bf *blockFile,
	remainingSortedEntries []Entry,
) (*leafNode, error) {
	var blockID int32
	var prevBlockID int32
	var nextBlockID int32
	var sortedEntries []Entry
	var duplicateOverflow bool

	blockID, err := bf.allocateBlock()
	if err != nil {
		return nil, err
	}

	if blockID == 1 {
		prevBlockID = invalidBlockID
	} else {
		prevBlockID = blockID - 1
	}

	if len(remainingSortedEntries) <= maxLeafNodeEntries {
		nextBlockID = invalidBlockID
	} else {
		nextBlockID = blockID + 1
	}

	n := min(maxLeafNodeEntries, len(remainingSortedEntries))
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

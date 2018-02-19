package index

func handleRootSplit(
	bf *BlockFile,
	root *internalNode,
	splitRouter router,
) (*internalNode, error) {
	// We always keep the root at block 0, so if the old root was just split,
	// we move the old root to a new block, create a new root, and write the new
	// root at block 0.
	newBlockID, err := bf.AllocateBlock()
	if err != nil {
		return nil, err
	}
	// Note that the node's blockID is not part of the marshaled representation.
	err = bf.WriteBlock(root.marshal(), newBlockID)
	if err != nil {
		return nil, err
	}
	newRoot := &internalNode{
		bf:               bf,
		blockID:          0,
		subtreeHeight:    root.subtreeHeight + 1,
		underflowBlockID: newBlockID,
		sortedRouters:    []router{splitRouter},
	}
	err = newRoot.flush()
	if err != nil {
		return nil, err
	}
	return newRoot, nil
}

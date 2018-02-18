package index

type bPlusTree struct {
	bf   *blockFile
	root *internalNode
}

func OpenBPlusTree(path string) (*bPlusTree, error) {
	bf, err := newBlockFile(path)
	if err != nil {
		return nil, err
	}
	var root *internalNode
	if bf.numBlocks == 0 {
		rootBlockID, err := bf.allocateBlock()
		if err != nil {
			return nil, err
		}
		leafBlockID, err := bf.allocateBlock()
		if err != nil {
			return nil, err
		}
		root = &internalNode{
			bf:               bf,
			blockID:          rootBlockID,
			subtreeHeight:    1,
			underflowBlockID: leafBlockID,
		}
		leaf := &leafNode{
			bf:          bf,
			blockID:     leafBlockID,
			prevBlockID: invalidBlockID,
			nextBlockID: invalidBlockID,
		}
		err = root.flush()
		if err != nil {
			return nil, err
		}
		err = leaf.flush()
		if err != nil {
			return nil, err
		}
	} else {
		n, err := readNode(bf, 0)
		if err != nil {
			return nil, err
		}
		root = n.(*internalNode)
	}
	return &bPlusTree{
		bf:   bf,
		root: root,
	}, nil
}

func (b *bPlusTree) AddEntry(entry Entry) error {
	p, err := b.root.addEntry(entry)
	if err != nil {
		return err
	}
	// We always keep the root at block 0, so if the old root was just split,
	// we move the old root to a new block, create a new root, and write the new
	// root at block 0.
	if p != nil {
		newBlockID, err := b.bf.allocateBlock()
		if err != nil {
			return err
		}
		// Note that the node's blockID is not included after marshaling.
		err = b.bf.writeBlock(b.root.marshal(), newBlockID)
		if err != nil {
			return err
		}
		newRoot := &internalNode{
			bf:               b.bf,
			blockID:          0,
			subtreeHeight:    b.root.subtreeHeight + 1,
			underflowBlockID: newBlockID,
			sortedRouters:    []router{*p},
		}
		err = newRoot.flush()
		if err != nil {
			return err
		}
		b.root = newRoot
	}
	return nil
}

func (b *bPlusTree) FindEqual(key int32) (Iterator, error) {
	return b.root.findEqual(key)
}

func (b *bPlusTree) FindGreaterEqual(key int32) (Iterator, error) {
	return b.root.findGreaterEqual(key)
}

func (b *bPlusTree) Close() error {
	err := b.root.flush()
	if err != nil {
		return err
	}
	return b.bf.close()
}

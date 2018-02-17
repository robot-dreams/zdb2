package index

type bPlusTree struct {
	bf   *blockFile
	root node
}

func NewBPlusTree(path string) (*bPlusTree, error) {
	bf, err := newBlockFile(path)
	if err != nil {
		return nil, err
	}
	var root node
	if bf.numBlocks == 0 {
		blockID, err := bf.allocateBlock()
		if err != nil {
			return nil, err
		}
		root = &leafNode{
			bf:          bf,
			blockID:     blockID,
			prevBlockID: invalidBlockID,
			nextBlockID: invalidBlockID,
		}
		err = root.flush()
		if err != nil {
			return nil, err
		}
	} else {
		root, err = readNode(bf, 0)
		if err != nil {
			return nil, err
		}
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

func (b *bPlusTree) FindEqual(key int32) (Entry, error) {
	return b.root.findEqual(key)
}

func (b *bPlusTree) Close() error {
	err := b.root.flush()
	if err != nil {
		return err
	}
	return b.bf.close()
}

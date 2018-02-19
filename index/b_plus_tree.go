package index

type bPlusTree struct {
	bf   *BlockFile
	root *internalNode
}

func OpenBPlusTree(path string) (*bPlusTree, error) {
	bf, err := NewBlockFile(path)
	if err != nil {
		return nil, err
	}
	var root *internalNode
	if bf.NumBlocks == 0 {
		rootBlockID, err := bf.AllocateBlock()
		if err != nil {
			return nil, err
		}
		leafBlockID, err := bf.AllocateBlock()
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
	splitRouter, err := b.root.addEntry(entry)
	if err != nil {
		return err
	}
	if splitRouter != nil {
		newRoot, err := handleRootSplit(b.bf, b.root, *splitRouter)
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
	return b.bf.Close()
}

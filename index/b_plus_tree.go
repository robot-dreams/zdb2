package index

import (
	"github.com/robot-dreams/zdb2/block_file"
)

type BPlusTree struct {
	bf   *block_file.BlockFile
	root *internalNode
}

func OpenBPlusTree(path string) (*BPlusTree, error) {
	bf, err := block_file.OpenBlockFile(path, blockSize)
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
			prevBlockID: block_file.InvalidBlockID,
			nextBlockID: block_file.InvalidBlockID,
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
	return &BPlusTree{
		bf:   bf,
		root: root,
	}, nil
}

func (b *BPlusTree) AddEntry(entry Entry) error {
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

func (b *BPlusTree) FindEqual(key int32) (Iterator, error) {
	return b.root.findEqual(key)
}

func (b *BPlusTree) FindGreaterEqual(key int32) (Iterator, error) {
	return b.root.findGreaterEqual(key)
}

func (b *BPlusTree) Close() error {
	err := b.root.flush()
	if err != nil {
		return err
	}
	return b.bf.Close()
}

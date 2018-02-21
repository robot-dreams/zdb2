package index

import "github.com/robot-dreams/zdb2/block_file"

func handleRootSplit(
	bf *block_file.BlockFile,
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

type ByKey []Entry

func (b ByKey) Len() int           { return len(b) }
func (b ByKey) Less(i, j int) bool { return b[i].Key < b[j].Key }
func (b ByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

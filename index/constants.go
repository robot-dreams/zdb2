package index

import (
	"encoding/binary"
)

const (
	invalidBlockID = -1
	blockSize      = 1 << 6

	// Leaf nodes
	leafNodeHeaderSize = 12
	entrySize          = 10
	maxLeafNodeEntries = (blockSize - leafNodeHeaderSize) / entrySize

	// Internal nodes
	internalNodeHeaderSize = 8
	routerSize             = 8
	maxInternalNodeRouters = (blockSize - internalNodeHeaderSize) / routerSize
)

type blockType uint16

const (
	blockType_Unknown blockType = iota
	blockType_LeafNode
	blockType_InternalNode
)

var byteOrder = binary.LittleEndian

package index

import (
	"encoding/binary"
)

const (
	invalidBlockID = -1

	// Leaf nodes
	leafNodeHeaderSize = 12
	leafNodeFooterSize = 1
	entrySize          = 10

	// Internal nodes
	internalNodeHeaderSize = 12
	routerSize             = 8
)

// Use var instead of const so that tests can modify these values (and check
// "interesting" cases without taking too long).
var (
	blockSize              int = 1 << 16
	maxLeafNodeEntries     int
	maxInternalNodeRouters int
)

func setBlockSize(blockSize int) {
	maxLeafNodeEntries = (blockSize - leafNodeHeaderSize - leafNodeFooterSize) / entrySize
	maxInternalNodeRouters = (blockSize - internalNodeHeaderSize) / routerSize
}

func init() {
	setBlockSize(blockSize)
}

type blockType uint16

const (
	blockType_Unknown blockType = iota
	blockType_LeafNode
	blockType_InternalNode
)

var byteOrder = binary.LittleEndian

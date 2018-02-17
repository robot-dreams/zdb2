package index

import "encoding/binary"

const (
	invalidBlockID = -1
	entrySize      = 10
)

type blockType uint16

const (
	blockType_Unknown blockType = iota
	blockType_Leaf
	blockType_Internal
)

var byteOrder = binary.LittleEndian

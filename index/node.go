package index

import (
	"bytes"
	"encoding/binary"

	"github.com/dropbox/godropbox/errors"
)

// A router points to a node whose descendents' entries are all greater than or
// equal to the given key.
type router struct {
	Key     int32
	BlockID int32
}

type node interface {
	// Precondition: the blockType value (uint16) has already been consumed, so
	// there are only blockSize - 2 bytes left.
	unmarshal(buf *bytes.Reader) error

	marshal() []byte

	flush() error

	// addEntry will write changes to the block buffer before returning.  If
	// adding an entry caused the node to split, then the returned router will
	// be non-nil.
	addEntry(Entry) (*router, error)

	findEqual(key int32) (Entry, error)
}

func readNode(bf *blockFile, blockID int32) (node, error) {
	b := make([]byte, blockSize)
	err := bf.readBlock(b, blockID)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewReader(b)
	var bt blockType
	err = binary.Read(buf, byteOrder, &bt)
	if err != nil {
		return nil, err
	}
	var result node
	switch bt {
	case blockType_LeafNode:
		result = &leafNode{
			bf:      bf,
			blockID: blockID,
		}
	case blockType_InternalNode:
		result = &internalNode{
			bf:      bf,
			blockID: blockID,
		}
	default:
		return nil, errors.Newf("Unknown blockType %d", bt)
	}
	err = result.unmarshal(buf)
	if err != nil {
		return nil, err
	}
	return result, nil
}

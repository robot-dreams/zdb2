package index

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type internalNode struct {
	bf               *blockFile
	blockID          int32
	underflowBlockID int32
	sortedRouters    []router
}

var _ node = (*internalNode)(nil)

func (in *internalNode) unmarshal(buf *bytes.Reader) error {
	var numRouters uint16
	for _, value := range []interface{}{
		&numRouters,
		&in.underflowBlockID,
	} {
		err := binary.Read(buf, byteOrder, value)
		if err != nil {
			return err
		}
	}
	in.sortedRouters = make([]router, numRouters)
	for i := 0; i < int(numRouters); i++ {
		for _, value := range []interface{}{
			&in.sortedRouters[i].Key,
			&in.sortedRouters[i].BlockID,
		} {
			err := binary.Read(buf, byteOrder, value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (in *internalNode) marshal() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, blockSize))
	for _, value := range []interface{}{
		blockType_InternalNode,
		uint16(len(in.sortedRouters)),
		in.underflowBlockID,
	} {
		// err is always nil when writing to a bytes.Buffer.
		_ = binary.Write(buf, byteOrder, value)
	}
	for _, router := range in.sortedRouters {
		_ = binary.Write(buf, byteOrder, router.Key)
		_ = binary.Write(buf, byteOrder, router.BlockID)
	}
	return buf.Bytes()[:blockSize]
}

func (in *internalNode) flush() error {
	return in.bf.writeBlock(in.marshal(), in.blockID)
}

// Splits the receiver into two internal nodes; modifies receiver in place and
// returns a router corresponding to the new internal node.  Both the receiver
// and the new internal node will be flushed to disk before returning.
//
// Precondition: the receiver is full
func (in *internalNode) split() (*router, error) {
	newBlockID, err := in.bf.allocateBlock()
	if err != nil {
		return nil, err
	}
	midpoint := len(in.sortedRouters) / 2
	lSortedRouters := in.sortedRouters[:midpoint]
	rSortedRouters := in.sortedRouters[midpoint+1:]
	// Unlike the leaf node case, we directly transfer the midpoint router
	// upwards (instead of copying it).
	midpointRouter := in.sortedRouters[midpoint]

	// Update and flush in.
	in.sortedRouters = lSortedRouters
	err = in.flush()
	if err != nil {
		return nil, err
	}

	// Create and flush new internal node.
	newInternalNode := &internalNode{
		bf:               in.bf,
		blockID:          newBlockID,
		underflowBlockID: midpointRouter.BlockID,
		sortedRouters:    rSortedRouters,
	}
	err = newInternalNode.flush()
	if err != nil {
		return nil, err
	}

	// Returned router corresponds to new internal node.
	return &router{
		Key:     midpointRouter.Key,
		BlockID: newBlockID,
	}, nil
}

func (in *internalNode) findSmallestIndexWithGreaterKey(key int32) int {
	return sort.Search(
		len(in.sortedRouters),
		func(i int) bool {
			return in.sortedRouters[i].Key > key
		})
}

func (in *internalNode) childNodeForKey(key int32) (node, error) {
	i := in.findSmallestIndexWithGreaterKey(key)

	// The child node we're looking for is immediately to the left of the router
	// at the index we just found.
	var childBlockID int32
	if i == 0 {
		childBlockID = in.underflowBlockID
	} else {
		childBlockID = in.sortedRouters[i-1].BlockID
	}
	return readNode(in.bf, childBlockID)
}

func (in *internalNode) addEntry(entry Entry) (*router, error) {
	childNode, err := in.childNodeForKey(entry.Key)
	if err != nil {
		return nil, err
	}
	childRouter, err := childNode.addEntry(entry)
	if err != nil {
		return nil, err
	}

	// If adding an entry didn't cause the child node to split, then we're done.
	if childRouter == nil {
		return nil, nil
	}

	// Add the new router, and split if doing so caused the number of routers to
	// exceed the per-node maximum.
	i := in.findSmallestIndexWithGreaterKey(childRouter.Key)
	if i == len(in.sortedRouters) {
		// Just add the new router to the end.
		in.sortedRouters = append(in.sortedRouters, *childRouter)
	} else {
		n := len(in.sortedRouters)

		// Extend the slice.
		in.sortedRouters = append(in.sortedRouters, router{})

		// Slide routers with a greater key to the right.
		copy(in.sortedRouters[i+1:], in.sortedRouters[i:n])

		// Insert the new router at the correct position.
		in.sortedRouters[i] = *childRouter
	}
	if len(in.sortedRouters) > maxInternalNodeRouters {
		return in.split()
	} else {
		return nil, in.flush()
	}
}

func (in *internalNode) findEqual(key int32) (Iterator, error) {
	childNode, err := in.childNodeForKey(key)
	if err != nil {
		return nil, err
	}
	return childNode.findEqual(key)
}

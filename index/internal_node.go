package index

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type internalNode struct {
	bf               *blockFile
	blockID          int32
	subtreeHeight    int32
	underflowBlockID int32
	sortedRouters    []router
}

var _ node = (*internalNode)(nil)

func (in *internalNode) unmarshal(buf *bytes.Reader) error {
	var numRouters uint16
	for _, value := range []interface{}{
		&numRouters,
		&in.subtreeHeight,
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
			&in.sortedRouters[i].key,
			&in.sortedRouters[i].blockID,
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
		in.subtreeHeight,
		in.underflowBlockID,
	} {
		// err is always nil when writing to a bytes.Buffer.
		_ = binary.Write(buf, byteOrder, value)
	}
	for _, router := range in.sortedRouters {
		_ = binary.Write(buf, byteOrder, router.key)
		_ = binary.Write(buf, byteOrder, router.blockID)
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
func (in *internalNode) splitAndFlush() (*router, error) {
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
		subtreeHeight:    in.subtreeHeight,
		underflowBlockID: midpointRouter.blockID,
		sortedRouters:    rSortedRouters,
	}
	err = newInternalNode.flush()
	if err != nil {
		return nil, err
	}

	// Returned router corresponds to new internal node.
	return &router{
		key:     midpointRouter.key,
		blockID: newBlockID,
	}, nil
}

func (in *internalNode) findSmallestIndexWithGreaterKey(key int32) int {
	return sort.Search(
		len(in.sortedRouters),
		func(i int) bool {
			return in.sortedRouters[i].key > key
		})
}

func (in *internalNode) childNodeForKey(key int32) (node, error) {
	i := in.findSmallestIndexWithGreaterKey(key)
	return in.childNodeAtIndex(i - 1)
}

func (in *internalNode) childNodeAtIndex(i int) (node, error) {
	var childBlockID int32
	if i == -1 {
		childBlockID = in.underflowBlockID
	} else {
		childBlockID = in.sortedRouters[i].blockID
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
	i := in.findSmallestIndexWithGreaterKey(childRouter.key)
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
		return in.splitAndFlush()
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

func (in *internalNode) findGreaterEqual(key int32) (Iterator, error) {
	childNode, err := in.childNodeForKey(key)
	if err != nil {
		return nil, err
	}
	return childNode.findGreaterEqual(key)
}

func (in *internalNode) bulkLoadHelper(leafRouter router) (*router, error) {
	appendRouter := func(childRouter router) (*router, error) {
		in.sortedRouters = append(in.sortedRouters, childRouter)
		if len(in.sortedRouters) > maxInternalNodeRouters {
			return in.splitAndFlush()
		} else {
			return nil, in.flush()
		}
	}

	// The base case is when the receiver's children are already leaf nodes.
	if in.subtreeHeight == 1 {
		return appendRouter(leafRouter)
	}

	// Recurse towards the receiver's right-most child.
	childNode, err := in.childNodeAtIndex(len(in.sortedRouters) - 1)
	if err != nil {
		return nil, err
	}
	childRouter, err := childNode.(*internalNode).bulkLoadHelper(leafRouter)
	if err != nil {
		return nil, err
	}

	// If adding a router didn't cause the child node to split, then we're done.
	if childRouter == nil {
		return nil, nil
	}

	return appendRouter(*childRouter)
}

package index

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/robot-dreams/zdb2"
)

const leafBlockHeaderSize = 12
const maxLeafBlockEntries = (zdb2.BlockSize - leafBlockHeaderSize) / entrySize

type leafBlock struct {
	blockID       int32
	prevBlockID   int32
	nextBlockID   int32
	sortedEntries []Entry
}

// Precondition: the blockType value (uint16) has already been consumed, so
// there are only zdb2.BlockSize - 2 bytes left.
func newLeafBlock(buf *bytes.Reader, blockID int32) *leafBlock {
	var prevBlockID int32
	var nextBlockID int32
	var numEntries uint16
	for _, value := range []interface{}{
		&prevBlockID,
		&nextBlockID,
		&numEntries,
	} {
		// err is always nil if the bytes.Reader has the expected size.
		binary.Read(buf, byteOrder, value)
	}
	sortedEntries := make([]Entry, int(numEntries))
	for i := range sortedEntries {
		binary.Read(buf, byteOrder, &sortedEntries[i])
	}
	return &leafBlock{
		blockID:       blockID,
		prevBlockID:   prevBlockID,
		nextBlockID:   nextBlockID,
		sortedEntries: sortedEntries,
	}
}

func (lb *leafBlock) marshal() ([]byte, int32) {
	buf := bytes.NewBuffer(make([]byte, 0, zdb2.BlockSize))
	for _, value := range []interface{}{
		blockType_Leaf,
		lb.prevBlockID,
		lb.nextBlockID,
		uint16(len(lb.sortedEntries)),
	} {
		// err is always nil when writing to a bytes.Buffer.
		binary.Write(buf, byteOrder, value)
	}
	for _, entry := range lb.sortedEntries {
		// TODO: Using reflect-based encoding might be too slow.
		binary.Write(buf, byteOrder, entry)
	}
	return buf.Bytes()[:zdb2.BlockSize], lb.blockID
}

// Splits lb into two blocks; modifies lb in place and returns the new block
// (which contains larger keys), together with the smallest key that appears in
// the new block.  If there were an odd number of elements in lb originally,
// then the extra element will go to the new block.
//
// Precondition: lb is not empty
func (lb *leafBlock) split(newBlockID int32) (*leafBlock, int32) {
	midpoint := len(lb.sortedEntries) / 2
	lSortedEntries := lb.sortedEntries[:midpoint]
	rSortedEntries := lb.sortedEntries[midpoint:]
	newBlock := &leafBlock{
		blockID:       newBlockID,
		prevBlockID:   lb.blockID,
		nextBlockID:   lb.nextBlockID,
		sortedEntries: rSortedEntries,
	}
	lb.nextBlockID = newBlockID
	lb.sortedEntries = lSortedEntries
	return newBlock, newBlock.sortedEntries[0].Key
}

// Precondition: len(lb.sortedEntries) < maxLeafBlockEntries
func (lb *leafBlock) addEntry(entry Entry) {
	n := len(lb.sortedEntries)
	i := lb.findGreater(entry.Key)
	if i == n {
		// Just add the new entry to the end.
		lb.sortedEntries = append(lb.sortedEntries, entry)
	} else {
		// Extend the slice.
		lb.sortedEntries = append(lb.sortedEntries, Entry{})

		// Slide elements with a greater key to the right.
		copy(lb.sortedEntries[i+1:], lb.sortedEntries[i:n])

		// Insert the new entry at the correct position.
		lb.sortedEntries[i] = entry
	}
}

// Returns the smallest i such that lb.sortedEntries[i].Key == key; if no such i
// exists, then the return value is len(lb.sortedEntries).
func (lb *leafBlock) findEqual(key int32) int {
	equalKey := func(i int) bool {
		return lb.sortedEntries[i].Key == key
	}
	return sort.Search(len(lb.sortedEntries), equalKey)
}

// Returns the smallest i such that lb.sortedEntries[i].Key > key; if no such i
// exists, then the return value is len(lb.sortedEntries).
func (lb *leafBlock) findGreater(key int32) int {
	greaterKey := func(i int) bool {
		return lb.sortedEntries[i].Key > key
	}
	return sort.Search(len(lb.sortedEntries), greaterKey)
}

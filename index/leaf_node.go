package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
)

type leafNode struct {
	bf            *blockFile
	blockID       int32
	prevBlockID   int32
	nextBlockID   int32
	sortedEntries []Entry
}

var _ node = (*leafNode)(nil)

func (ln *leafNode) unmarshal(buf *bytes.Reader) error {
	var numEntries uint16
	for _, value := range []interface{}{
		&ln.prevBlockID,
		&ln.nextBlockID,
		&numEntries,
	} {
		err := binary.Read(buf, byteOrder, value)
		if err != nil {
			return err
		}
	}
	ln.sortedEntries = make([]Entry, numEntries)
	for i := 0; i < int(numEntries); i++ {
		err := binary.Read(buf, byteOrder, &ln.sortedEntries[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (ln *leafNode) marshal() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, blockSize))
	for _, value := range []interface{}{
		blockType_LeafNode,
		ln.prevBlockID,
		ln.nextBlockID,
		uint16(len(ln.sortedEntries)),
	} {
		// err is always nil when writing to a bytes.Buffer.
		_ = binary.Write(buf, byteOrder, value)
	}
	for _, entry := range ln.sortedEntries {
		// TODO: Using reflect-based encoding might be too slow.
		_ = binary.Write(buf, byteOrder, entry)
	}
	return buf.Bytes()[:blockSize]
}

func (ln *leafNode) flush() error {
	return ln.bf.writeBlock(ln.marshal(), ln.blockID)
}

// Splits ln into two leaf nodes; modifies ln in place and returns a router
// corresponding to the new leaf node.  Both ln and the new leaf node will be
// flushed to disk before returning.
//
// Precondition: ln is not empty
func (ln *leafNode) split() (*router, error) {
	newBlockID, err := ln.bf.allocateBlock()
	if err != nil {
		return nil, err
	}
	midpoint := len(ln.sortedEntries) / 2
	lSortedEntries := ln.sortedEntries[:midpoint]
	rSortedEntries := ln.sortedEntries[midpoint:]

	// Update and flush ln.
	ln.sortedEntries = lSortedEntries
	ln.nextBlockID = newBlockID
	err = ln.flush()
	if err != nil {
		return nil, err
	}

	// Create and flush new leaf node.
	newLeafNode := &leafNode{
		bf:            ln.bf,
		blockID:       newBlockID,
		prevBlockID:   ln.blockID,
		nextBlockID:   ln.nextBlockID,
		sortedEntries: rSortedEntries,
	}
	err = newLeafNode.flush()
	if err != nil {
		return nil, err
	}

	// Returned router corresponds to new leaf node.
	return &router{
		Key:     newLeafNode.sortedEntries[0].Key,
		BlockID: newBlockID,
	}, nil
}

func (ln *leafNode) findSmallestIndexWithGreaterEqualKey(key int32) int {
	return sort.Search(
		len(ln.sortedEntries),
		func(i int) bool {
			return ln.sortedEntries[i].Key >= key
		})
}

func (ln *leafNode) findSmallestIndexWithGreaterKey(key int32) int {
	return sort.Search(
		len(ln.sortedEntries),
		func(i int) bool {
			return ln.sortedEntries[i].Key > key
		})
}

func (ln *leafNode) addEntry(entry Entry) (*router, error) {
	i := ln.findSmallestIndexWithGreaterKey(entry.Key)
	if i == len(ln.sortedEntries) {
		// Just add the new entry to the end.
		ln.sortedEntries = append(ln.sortedEntries, entry)
	} else {
		n := len(ln.sortedEntries)

		// Extend the slice.
		ln.sortedEntries = append(ln.sortedEntries, Entry{})

		// Slide entries with a greater key to the right.
		copy(ln.sortedEntries[i+1:], ln.sortedEntries[i:n])

		// Insert the new entry at the correct position.
		ln.sortedEntries[i] = entry
	}
	if len(ln.sortedEntries) > maxLeafNodeEntries {
		return ln.split()
	} else {
		return nil, ln.flush()
	}
}

// Precondition: ln.nextBlockID is either invalidBlockID or the blockID of a
// valid leaf node.
func (ln *leafNode) nextLeafNode() (*leafNode, error) {
	if ln.nextBlockID == invalidBlockID {
		return nil, io.EOF
	}
	result, err := readNode(ln.bf, ln.nextBlockID)
	if err != nil {
		return nil, err
	}
	return result.(*leafNode), nil
}

func (ln *leafNode) findEqual(key int32) (Entry, error) {
	i := ln.findSmallestIndexWithGreaterEqualKey(key)
	if i == len(ln.sortedEntries) {
		return Entry{}, io.EOF
	}
	return ln.sortedEntries[i], nil
}

/*
type leafNodeIterator struct {
	ln             *leafNode
	position       int
	entryPredicate func(Entry) bool
}

func (iter *leafNodeIterator) Next() (Entry, error) {
	for iter.position == len(iter.ln.sortedEntries) {
		ln, err := iter.ln.nextLeafNode()
		if err != nil {
			return Entry{}, err
		}
		iter.position = 0
		iter.ln = ln
	}
	fmt.Println(iter.position, iter.ln.sortedEntries[iter.position])
	entry := iter.ln.sortedEntries[iter.position]
	if !iter.entryPredicate(entry) {
		return Entry{}, io.EOF
	} else {
		iter.position++
		return entry, nil
	}
}
*/

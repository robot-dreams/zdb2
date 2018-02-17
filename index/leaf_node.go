package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
)

type leafNode struct {
	bf                *blockFile
	blockID           int32
	prevBlockID       int32
	nextBlockID       int32
	sortedEntries     []Entry
	duplicateOverflow bool
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
		for _, value := range []interface{}{
			&ln.sortedEntries[i].Key,
			&ln.sortedEntries[i].RID.PageID,
			&ln.sortedEntries[i].RID.SlotID,
		} {
			err := binary.Read(buf, byteOrder, value)
			if err != nil {
				return err
			}
		}
	}
	return binary.Read(buf, byteOrder, &ln.duplicateOverflow)
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
		_ = binary.Write(buf, byteOrder, entry.Key)
		_ = binary.Write(buf, byteOrder, entry.RID.PageID)
		_ = binary.Write(buf, byteOrder, entry.RID.SlotID)
	}
	_ = binary.Write(buf, byteOrder, ln.duplicateOverflow)
	return buf.Bytes()[:blockSize]
}

func (ln *leafNode) flush() error {
	return ln.bf.writeBlock(ln.marshal(), ln.blockID)
}

// Splits ln into two leaf nodes; ln is modified in place.  If the split
// happened between two entries with different keys, then a router will be
// returned corresponding to the new leaf node.  Both ln and the new leaf node
// will be flushed to disk before returning.
//
// Precondition: ln is full
func (ln *leafNode) split() (*router, error) {
	newBlockID, err := ln.bf.allocateBlock()
	if err != nil {
		return nil, err
	}
	midpoint := len(ln.sortedEntries) / 2
	lSortedEntries := ln.sortedEntries[:midpoint]
	rSortedEntries := ln.sortedEntries[midpoint:]

	// Create and flush new leaf node.
	newLeafNode := &leafNode{
		bf:                ln.bf,
		blockID:           newBlockID,
		prevBlockID:       ln.blockID,
		nextBlockID:       ln.nextBlockID,
		sortedEntries:     rSortedEntries,
		duplicateOverflow: ln.duplicateOverflow,
	}
	err = newLeafNode.flush()
	if err != nil {
		return nil, err
	}

	// Update and flush ln.
	ln.sortedEntries = lSortedEntries
	ln.nextBlockID = newBlockID
	lFinalEntry := lSortedEntries[midpoint-1]
	rFirstEntry := rSortedEntries[0]
	if lFinalEntry.Key == rFirstEntry.Key {
		ln.duplicateOverflow = true
	}
	err = ln.flush()
	if err != nil {
		return nil, err
	}

	if ln.duplicateOverflow {
		// If we split between two nodes with the same key, then the parent
		// shouldn't be updated; thus we don't return a router.
		return nil, nil
	} else {
		// Returned router corresponds to new leaf node.
		return &router{
			key:     newLeafNode.sortedEntries[0].Key,
			blockID: newBlockID,
		}, nil
	}
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
		if ln.duplicateOverflow {
			next, err := ln.nextLeafNode()
			if err != nil {
				return nil, err
			}
			return next.addEntry(entry)
		} else {
			// Just add the new entry to the end.
			ln.sortedEntries = append(ln.sortedEntries, entry)
		}
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

func (ln *leafNode) findEqual(key int32) (Iterator, error) {
	position := ln.findSmallestIndexWithGreaterEqualKey(key)
	if position == len(ln.sortedEntries) {
		if ln.duplicateOverflow {
			next, err := ln.nextLeafNode()
			if err != nil {
				return nil, err
			}
			return next.findEqual(key)
		} else {
			return EmptyIterator{}, nil
		}
	}
	return &leafNodeIterator{
		ln:       ln,
		position: position,
		entryPredicate: func(entry Entry) bool {
			return entry.Key == key
		},
	}, nil
}

func (ln *leafNode) findGreaterEqual(key int32) (Iterator, error) {
	position := ln.findSmallestIndexWithGreaterEqualKey(key)
	if position == len(ln.sortedEntries) {
		next, err := ln.nextLeafNode()
		if err != nil {
			return nil, err
		}
		return next.findGreaterEqual(key)
	}
	return &leafNodeIterator{
		ln:       ln,
		position: position,
	}, nil
}

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
	entry := iter.ln.sortedEntries[iter.position]
	if iter.entryPredicate != nil && !iter.entryPredicate(entry) {
		return Entry{}, io.EOF
	} else {
		iter.position++
		return entry, nil
	}
}

package heap_file

import (
	"bytes"
	"encoding/binary"

	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/block_file"
)

type heapPage struct {
	pageID   int32
	t        *zdb2.TableHeader
	numSlots uint16
	data     [pageSize]byte
}

func newHeapPage(
	bf *block_file.BlockFile,
	t *zdb2.TableHeader,
) (*heapPage, error) {
	pageID, err := bf.AllocateBlock()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = zdb2.WriteTableHeader(&buf, t)
	if err != nil {
		return nil, err
	}
	b := buf.Bytes()
	var data [pageSize]byte
	copy(data[:len(b)], b)
	hp := &heapPage{
		pageID:   pageID,
		t:        t,
		numSlots: 0,
		data:     data,
	}
	hp.addEntryToLookupTable(uint16(len(b)))
	return hp, nil
}

func loadHeapPage(
	bf *block_file.BlockFile,
	pageID int32,
) (*heapPage, error) {
	var data [pageSize]byte
	err := bf.ReadBlock(data[:], pageID)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data[:])
	t, err := zdb2.ReadTableHeader(r)
	if err != nil {
		return nil, err
	}
	var numSlots uint16
	r = bytes.NewReader(data[pageSize-lookupTableFooterWidth:])
	err = binary.Read(r, zdb2.ByteOrder, &numSlots)
	if err != nil {
		return nil, err
	}
	return &heapPage{
		pageID:   pageID,
		t:        t,
		numSlots: numSlots,
		data:     data,
	}, nil
}

// The end of the page contains a lookup table of offsets into the page where
// records can be found.  lookupTableOffset returns the offset into the page
// where this lookup table starts.
func (hp *heapPage) lookupTableOffset() uint16 {
	// There are hp.numSlots + 1 entries because we also store the offset of the
	// next available slot in the lookup table.
	lookupTableEntriesWidth := lookupTableEntryWidth * (hp.numSlots + 1)
	return pageSize - lookupTableFooterWidth - lookupTableEntriesWidth
}

// lookupOffset returns the offset into the page where the offset for the
// record with the given slotID can be found.
//
// Precondition: slotID is in [0, numSlots]
func (hp *heapPage) lookupOffset(slotID uint16) uint16 {
	return hp.lookupTableOffset() + lookupTableEntryWidth*(hp.numSlots-slotID)
}

func (hp *heapPage) addEntryToLookupTable(offset uint16) {
	var buf bytes.Buffer
	binary.Write(&buf, zdb2.ByteOrder, offset)
	i := hp.lookupTableOffset() - lookupTableEntryWidth
	copy(hp.data[i:i+lookupTableEntryWidth], buf.Bytes())
}

// Update both hp.numSlots and the encoded version in the footer.
func (hp *heapPage) incrementNumSlots() {
	hp.numSlots++
	var buf bytes.Buffer
	binary.Write(&buf, zdb2.ByteOrder, hp.numSlots)
	copy(hp.data[pageSize-lookupTableFooterWidth:], buf.Bytes())
}

// Returns the offset into hp.data at which the record with the given slotID is
// stored (if slotID < numSlots), or should be stored (if slotID == numSlots).
//
// Precondition: slotID is in [0, numSlots]
func (hp *heapPage) recordOffset(slotID uint16) uint16 {
	i := int(hp.lookupOffset(slotID))
	r := bytes.NewReader(hp.data[i : i+lookupTableEntryWidth])
	var offset uint16
	_ = binary.Read(r, zdb2.ByteOrder, &offset)
	return offset
}

func (hp *heapPage) freeSpace() uint16 {
	// Recall that hp.recordOffset(hp.numSlots) gives the offset of the next
	// available slot (if we're adding a new record to the page).
	return hp.lookupTableOffset() - hp.recordOffset(hp.numSlots)
}

// If there was no room for the record in this page, then the return value will
// be (false, nil).
//
// Precondition: the input is a valid record for the table described by hp.t
func (hp *heapPage) insert(record zdb2.Record) (bool, error) {
	var buf bytes.Buffer
	// The heapPage representation includes a "tombstone" byte for each record
	// to indicate whether it's deleted.
	_ = binary.Write(&buf, zdb2.ByteOrder, false)
	err := hp.t.WriteRecord(&buf, record)
	if err != nil {
		return false, err
	}
	b := buf.Bytes()

	// Inserting a record also requires us to add an entry to the lookup table.
	if hp.freeSpace() < uint16(len(b))+lookupTableEntryWidth {
		return false, nil
	}
	i := int(hp.recordOffset(hp.numSlots))
	copy(hp.data[i:i+len(b)], b)
	hp.addEntryToLookupTable(uint16(i + len(b)))
	hp.incrementNumSlots()
	return true, nil
}

// Returns whether the record at the given slotID previously existed (i.e.
// slotID is in [0, hp.numSlots) and the tombstone wasn't already set).
func (hp *heapPage) delete(slotID uint16) (bool, error) {
	if slotID >= hp.numSlots {
		return false, errors.Newf(
			"Expected slotID in [0, %d); got %d",
			hp.numSlots,
			slotID)
	}
	var buf bytes.Buffer
	_ = binary.Write(&buf, zdb2.ByteOrder, true)
	b := buf.Bytes()
	i := int(hp.recordOffset(slotID))
	copy(hp.data[i:i+len(b)], b)
	return true, nil
}

func (hp *heapPage) get(slotID uint16) (zdb2.Record, error) {
	if slotID >= hp.numSlots {
		return nil, errors.Newf(
			"Expected slotID in [0, %d); got %d",
			hp.numSlots,
			slotID)
	}
	i := int(hp.recordOffset(slotID))
	j := int(hp.recordOffset(slotID + 1))
	r := bytes.NewReader(hp.data[i:j])
	var deleted bool
	err := binary.Read(r, zdb2.ByteOrder, &deleted)
	if err != nil {
		return nil, err
	}
	if deleted {
		return nil, nil
	}
	return hp.t.ReadRecord(r)
}

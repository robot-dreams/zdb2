package heap_file

import (
	"bytes"
	"encoding/binary"

	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/block_file"
)

type heapPage struct {
	pageID int32
	data   []byte
}

func (hp *heapPage) getTableHeader() (*zdb2.TableHeader, error) {
	r := bytes.NewReader(hp.data)
	t, err := zdb2.ReadTableHeader(r)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (hp *heapPage) setTableHeader(t *zdb2.TableHeader) (int, error) {
	var buf bytes.Buffer
	err := zdb2.WriteTableHeader(&buf, t)
	if err != nil {
		return 0, err
	}
	b := buf.Bytes()
	copy(hp.data, b)
	return len(b), nil
}

func (hp *heapPage) getUint16(offset int) uint16 {
	r := bytes.NewReader(hp.data[offset : offset+2])
	var result uint16
	_ = binary.Read(r, zdb2.ByteOrder, &result)
	return result
}

func (hp *heapPage) setUint16(offset int, value uint16) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, zdb2.ByteOrder, value)
	copy(hp.data[offset:offset+2], buf.Bytes())
}

func (hp *heapPage) getNextSlotOffset() uint16 {
	return hp.getUint16(pageSize - 4)
}

func (hp *heapPage) setNextSlotOffset(nextSlotOffset uint16) {
	hp.setUint16(pageSize-4, nextSlotOffset)
}

func (hp *heapPage) getNumSlots() uint16 {
	return hp.getUint16(pageSize - 2)
}

func (hp *heapPage) setNumSlots(numSlots uint16) {
	hp.setUint16(pageSize-2, numSlots)
}

func newHeapPage(
	bf *block_file.BlockFile,
	t *zdb2.TableHeader,
) (*heapPage, error) {
	pageID, err := bf.AllocateBlock()
	if err != nil {
		return nil, err
	}
	data := make([]byte, pageSize)
	hp := &heapPage{
		pageID: pageID,
		data:   data,
	}
	n, err := hp.setTableHeader(t)
	if err != nil {
		return nil, err
	}
	hp.setNextSlotOffset(uint16(n))
	hp.setNumSlots(0)
	return hp, nil
}

func loadHeapPage(
	bf *block_file.BlockFile,
	pageID int32,
) (*heapPage, error) {
	data := make([]byte, pageSize)
	err := bf.ReadBlock(data, pageID)
	if err != nil {
		return nil, err
	}
	return &heapPage{
		pageID: pageID,
		data:   data,
	}, nil
}

// Each page contains a lookup table of offsets into the page where records can
// be found.  lookupTableOffset returns the offset into the page where this
// lookup table starts.
func (hp *heapPage) lookupTableOffset() uint16 {
	lookupTableEntriesWidth := lookupTableEntryWidth * hp.getNumSlots()
	return pageSize - lookupTableFooterWidth - lookupTableEntriesWidth
}

// lookupOffset returns the offset into the page where the offset for the
// record with the given slotID can be found.
//
// Precondition: slotID is in [0, numSlots)
func (hp *heapPage) lookupOffset(slotID uint16) uint16 {
	n := hp.getNumSlots() - slotID - 1
	return hp.lookupTableOffset() + n*lookupTableEntryWidth
}

func (hp *heapPage) extendLookupTable(offset uint16) {
	var buf bytes.Buffer
	binary.Write(&buf, zdb2.ByteOrder, offset)
	i := hp.lookupTableOffset() - lookupTableEntryWidth
	copy(hp.data[i:i+lookupTableEntryWidth], buf.Bytes())
	hp.setNumSlots(hp.getNumSlots() + 1)
}

// Returns the offset into hp.data at which the record with the given slotID is
// stored.
//
// Precondition: slotID is in [0, numSlots)
func (hp *heapPage) recordOffset(slotID uint16) uint16 {
	i := int(hp.lookupOffset(slotID))
	r := bytes.NewReader(hp.data[i : i+lookupTableEntryWidth])
	var offset uint16
	_ = binary.Read(r, zdb2.ByteOrder, &offset)
	return offset
}

func (hp *heapPage) freeSpace() uint16 {
	return hp.lookupTableOffset() - hp.getNextSlotOffset()
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
	t, err := hp.getTableHeader()
	if err != nil {
		return false, err
	}
	err = t.WriteRecord(&buf, record)
	if err != nil {
		return false, err
	}
	b := buf.Bytes()

	// Inserting a record also requires us to add an entry to the lookup table.
	if hp.freeSpace() < uint16(len(b))+lookupTableEntryWidth {
		return false, nil
	}
	nextSlotOffset := hp.getNextSlotOffset()
	copy(hp.data[nextSlotOffset:], b)
	hp.extendLookupTable(nextSlotOffset)
	hp.setNextSlotOffset(nextSlotOffset + uint16(len(b)))
	return true, nil
}

func (hp *heapPage) delete(slotID uint16) error {
	if slotID >= hp.getNumSlots() {
		return errors.Newf(
			"Expected slotID in [0, %d); got %d",
			hp.getNumSlots(),
			slotID)
	}
	var buf bytes.Buffer
	_ = binary.Write(&buf, zdb2.ByteOrder, true)
	b := buf.Bytes()
	i := int(hp.recordOffset(slotID))
	copy(hp.data[i:i+len(b)], b)
	return nil
}

func (hp *heapPage) get(slotID uint16) (zdb2.Record, error) {
	if slotID >= hp.getNumSlots() {
		return nil, errors.Newf(
			"Expected slotID in [0, %d); got %d",
			hp.getNumSlots(),
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
	t, err := hp.getTableHeader()
	if err != nil {
		return nil, err
	}
	return t.ReadRecord(r)
}

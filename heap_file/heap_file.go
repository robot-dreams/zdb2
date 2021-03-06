package heap_file

import (
	"io"

	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/block_file"
)

type heapFile struct {
	bf *block_file.BlockFile

	// Caching the last page lets us look up the TableHeader and perform bulk
	// inserts more efficiently.
	lastPage *heapPage

	closed bool
}

func NewHeapFile(path string, t *zdb2.TableHeader) (*heapFile, error) {
	bf, err := block_file.OpenBlockFile(path, pageSize)
	if err != nil {
		return nil, err
	}
	if bf.NumBlocks > 0 {
		return nil, errors.Newf(
			"Cannot create new heap file at non-empty file %v",
			path)
	}
	hp, err := newHeapPage(bf, t)
	if err != nil {
		return nil, err
	}
	return &heapFile{
		bf:       bf,
		lastPage: hp,
		closed:   false,
	}, nil
}

func BulkLoadNewHeapFile(
	path string,
	iter zdb2.Iterator,
) error {
	hf, err := NewHeapFile(path, iter.TableHeader())
	if err != nil {
		return err
	}
	for {
		record, err := iter.Next()
		if err == io.EOF {
			return hf.Close()
		} else if err != nil {
			return err
		}
		_, err = hf.Insert(record)
		if err != nil {
			return err
		}
	}
}

func OpenHeapFile(path string) (*heapFile, error) {
	bf, err := block_file.OpenBlockFile(path, pageSize)
	if err != nil {
		return nil, err
	}
	if bf.NumBlocks == 0 {
		return nil, errors.Newf(
			"Cannot open heap file from empty file at %v",
			path)
	}
	hp, err := loadHeapPage(bf, bf.NumBlocks-1)
	if err != nil {
		return nil, err
	}
	return &heapFile{
		bf:       bf,
		lastPage: hp,
		closed:   false,
	}, nil
}

func (hf *heapFile) TableHeader() *zdb2.TableHeader {
	return hf.lastPage.t
}

func (hf *heapFile) Insert(record zdb2.Record) (zdb2.RecordID, error) {
	for {
		ok, err := hf.lastPage.insert(record)
		if err != nil {
			return zdb2.RecordID{}, err
		}
		if ok {
			return zdb2.RecordID{
				PageID: hf.lastPage.pageID,
				SlotID: hf.lastPage.numSlots - 1,
			}, nil
		}
		err = hf.flush()
		if err != nil {
			return zdb2.RecordID{}, err
		}
		hp, err := newHeapPage(hf.bf, hf.lastPage.t)
		if err != nil {
			return zdb2.RecordID{}, err
		}
		hf.lastPage = hp
	}
}

func (hf *heapFile) loadPage(pageID int32) (*heapPage, error) {
	if pageID == hf.lastPage.pageID {
		return hf.lastPage, nil
	} else if pageID > hf.lastPage.pageID {
		return nil, errors.Newf(
			"Invalid pageID %d exceeds max pageID %d",
			pageID,
			hf.lastPage.pageID)
	}
	hp, err := loadHeapPage(hf.bf, pageID)
	if err != nil {
		return nil, err
	}
	return hp, nil
}

func (hf *heapFile) Delete(recordID zdb2.RecordID) error {
	hp, err := hf.loadPage(recordID.PageID)
	if err != nil {
		return err
	}
	err = hp.delete(recordID.SlotID)
	if err != nil {
		return err
	}
	// TODO: Can we switch to a lazier flush strategy?
	return hf.bf.WriteBlock(hp.data[:], hp.pageID)
}

func (hf *heapFile) Get(recordID zdb2.RecordID) (zdb2.Record, error) {
	hp, err := hf.loadPage(recordID.PageID)
	if err != nil {
		return nil, err
	}
	return hp.get(recordID.SlotID)
}

func (hf *heapFile) flush() error {
	hf.lastPage.flush()
	return hf.bf.WriteBlock(hf.lastPage.data[:], hf.lastPage.pageID)
}

func (hf *heapFile) Close() error {
	if hf.closed {
		return nil
	}
	err := hf.flush()
	if err != nil {
		return err
	}
	defer func() {
		hf.closed = true
	}()
	return hf.bf.Close()
}

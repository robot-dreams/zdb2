package heap_file

import (
	"io"

	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/block_file"
)

type result struct {
	record   zdb2.Record
	recordID zdb2.RecordID
	err      error
}

type scan struct {
	bf         *block_file.BlockFile
	t          *zdb2.TableHeader
	resultChan chan *result
	closed     bool
	done       chan struct{}
}

var _ zdb2.Iterator = (*scan)(nil)

func NewScan(path string) (*scan, error) {
	bf, err := block_file.OpenBlockFile(path, pageSize)
	if err != nil {
		return nil, err
	}
	if bf.NumBlocks == 0 {
		return nil, errors.Newf("%v is not a valid heap file", path)
	}
	hp, err := loadHeapPage(bf, 0)
	if err != nil {
		return nil, err
	}
	s := &scan{
		bf:         bf,
		t:          hp.t,
		resultChan: make(chan *result),
		closed:     false,
		done:       make(chan struct{}),
	}
	go s.startScan()
	return s, nil
}

func (s *scan) startScan() {
	defer close(s.resultChan)
	for pageID := int32(0); pageID < s.bf.NumBlocks; pageID++ {
		hp, err := loadHeapPage(s.bf, pageID)
		if err != nil {
			select {
			case <-s.done:
				return
			case s.resultChan <- &result{nil, zdb2.RecordID{}, err}:
			}
		}
		numSlots := hp.getNumSlots()
		for slotID := uint16(0); slotID < numSlots; slotID++ {
			record, err := hp.get(slotID)
			if err != nil {
				select {
				case <-s.done:
					return
				case s.resultChan <- &result{nil, zdb2.RecordID{}, err}:
				}
			}
			// Records marked as deleted shouldn't be returned.
			if record == nil {
				continue
			}
			recordID := zdb2.RecordID{
				PageID: pageID,
				SlotID: slotID,
			}
			select {
			case <-s.done:
				return
			case s.resultChan <- &result{record, recordID, nil}:
			}
		}
	}
}

func (s *scan) TableHeader() *zdb2.TableHeader {
	return s.t
}

func (s *scan) Next() (zdb2.Record, error) {
	record, _, err := s.NextWithID()
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *scan) NextWithID() (zdb2.Record, zdb2.RecordID, error) {
	select {
	case <-s.done:
		return nil, zdb2.RecordID{}, errors.New("scan was closed")
	case r, ok := <-s.resultChan:
		if ok {
			return r.record, r.recordID, r.err
		} else {
			return nil, zdb2.RecordID{}, io.EOF
		}
	}
}

func (s *scan) Close() error {
	if s.closed {
		return nil
	}
	defer func() {
		s.closed = true
	}()
	close(s.done)
	return s.bf.Close()
}

package heap_file

import (
	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/index"
)

type indexScan struct {
	bpt    *index.BPlusTree
	hf     *heapFile
	iter   index.Iterator
	closed bool
}

func newIndexScan(
	indexPath string,
	heapFilePath string,
	key int32,
	findFunc func(*index.BPlusTree, int32) (index.Iterator, error),
) (*indexScan, error) {
	bpt, err := index.OpenBPlusTree(indexPath)
	if err != nil {
		return nil, err
	}
	hf, err := OpenHeapFile(heapFilePath)
	if err != nil {
		return nil, err
	}
	iter, err := findFunc(bpt, key)
	if err != nil {
		return nil, err
	}
	return &indexScan{
		bpt:    bpt,
		hf:     hf,
		iter:   iter,
		closed: false,
	}, nil
}

func NewIndexScanGreaterEqual(
	indexPath string,
	heapFilePath string,
	key int32,
) (*indexScan, error) {
	return newIndexScan(
		indexPath,
		heapFilePath,
		key,
		(*index.BPlusTree).FindGreaterEqual)
}

func NewIndexScanEqual(
	indexPath string,
	heapFilePath string,
	key int32,
) (*indexScan, error) {
	return newIndexScan(
		indexPath,
		heapFilePath,
		key,
		(*index.BPlusTree).FindEqual)
}

func (s *indexScan) TableHeader() *zdb2.TableHeader {
	return s.hf.TableHeader()
}

func (s *indexScan) Next() (zdb2.Record, error) {
	entry, err := s.iter.Next()
	if err != nil {
		return nil, err
	}
	return s.hf.Get(entry.RID)
}

func (s *indexScan) Close() error {
	if s.closed {
		return nil
	}
	defer func() {
		s.closed = true
	}()
	err := s.bpt.Close()
	if err != nil {
		return err
	}
	return s.hf.Close()
}

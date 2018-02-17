package index

import (
	"os"

	"github.com/dropbox/godropbox/errors"
)

type blockFile struct {
	f         *os.File
	numBlocks int32
}

func newBlockFile(path string) (*blockFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	numBlocks := int32(stat.Size() / int64(blockSize))
	return &blockFile{
		f:         f,
		numBlocks: numBlocks,
	}, nil
}

// Returns blockID of the newly allocated block; it's guaranteed that the next
// blockID will be the current value of bf.numBlocks.
func (bf *blockFile) allocateBlock() (int32, error) {
	blockID := bf.numBlocks
	bf.numBlocks++
	err := bf.f.Truncate(int64(bf.numBlocks) * int64(blockSize))
	if err != nil {
		return invalidBlockID, err
	}
	return blockID, nil
}

func (bf *blockFile) readBlock(b []byte, blockID int32) error {
	if blockID < 0 || blockID >= bf.numBlocks {
		return errors.Newf("blockID must be in [0, %d); got %d", bf.numBlocks, blockID)
	}
	if len(b) != blockSize {
		return errors.Newf("len(b) must be %d; got %d", blockSize, len(b))
	}
	_, err := bf.f.ReadAt(b, int64(blockID)*int64(blockSize))
	if err != nil {
		return err
	}
	return nil
}

func (bf *blockFile) writeBlock(b []byte, blockID int32) error {
	if blockID < 0 || blockID >= bf.numBlocks {
		return errors.Newf("blockID must be in [0, %d); got %d", bf.numBlocks, blockID)
	}
	if len(b) != blockSize {
		return errors.Newf("len(b) must be %d; got %d", blockSize, len(b))
	}
	_, err := bf.f.WriteAt(b, int64(blockID)*int64(blockSize))
	return err
}

func (bf *blockFile) close() error {
	return bf.f.Close()
}

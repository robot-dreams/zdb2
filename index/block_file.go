package index

import (
	"os"

	"github.com/dropbox/godropbox/errors"
)

type BlockFile struct {
	File      *os.File
	NumBlocks int32
}

func NewBlockFile(path string) (*BlockFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	numBlocks := int32(stat.Size() / int64(blockSize))
	return &BlockFile{
		File:      f,
		NumBlocks: numBlocks,
	}, nil
}

// Returns blockID of the newly allocated block; it's guaranteed that the next
// blockID will be the current value of bf.numBlocks.
func (bf *BlockFile) AllocateBlock() (int32, error) {
	blockID := bf.NumBlocks
	bf.NumBlocks++
	err := bf.File.Truncate(int64(bf.NumBlocks) * int64(blockSize))
	if err != nil {
		return invalidBlockID, err
	}
	return blockID, nil
}

func (bf *BlockFile) ReadBlock(b []byte, blockID int32) error {
	if blockID < 0 || blockID >= bf.NumBlocks {
		return errors.Newf("blockID must be in [0, %d); got %d", bf.NumBlocks, blockID)
	}
	if len(b) != blockSize {
		return errors.Newf("len(b) must be %d; got %d", blockSize, len(b))
	}
	_, err := bf.File.ReadAt(b, int64(blockID)*int64(blockSize))
	if err != nil {
		return err
	}
	return nil
}

func (bf *BlockFile) WriteBlock(b []byte, blockID int32) error {
	if blockID < 0 || blockID >= bf.NumBlocks {
		return errors.Newf("blockID must be in [0, %d); got %d", bf.NumBlocks, blockID)
	}
	if len(b) != blockSize {
		return errors.Newf("len(b) must be %d; got %d", blockSize, len(b))
	}
	_, err := bf.File.WriteAt(b, int64(blockID)*int64(blockSize))
	return err
}

func (bf *BlockFile) Close() error {
	return bf.File.Close()
}

package block_file

import (
	"os"

	"github.com/dropbox/godropbox/errors"
)

const InvalidBlockID = -1

type BlockFile struct {
	File      *os.File
	BlockSize int
	NumBlocks int32
}

func OpenBlockFile(path string, blockSize int) (*BlockFile, error) {
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
		BlockSize: blockSize,
		NumBlocks: numBlocks,
	}, nil
}

// Returns blockID of the newly allocated block; it's guaranteed that the next
// blockID will be the current value of bf.numBlocks.
func (bf *BlockFile) AllocateBlock() (int32, error) {
	blockID := bf.NumBlocks
	bf.NumBlocks++
	err := bf.File.Truncate(int64(bf.NumBlocks) * int64(bf.BlockSize))
	if err != nil {
		return InvalidBlockID, err
	}
	return blockID, nil
}

func (bf *BlockFile) ReadBlock(b []byte, blockID int32) error {
	if blockID < 0 || blockID >= bf.NumBlocks {
		return errors.Newf("blockID must be in [0, %d); got %d", bf.NumBlocks, blockID)
	}
	if len(b) != bf.BlockSize {
		return errors.Newf("len(b) must be %d; got %d", bf.BlockSize, len(b))
	}
	_, err := bf.File.ReadAt(b, int64(blockID)*int64(bf.BlockSize))
	if err != nil {
		return err
	}
	return nil
}

func (bf *BlockFile) WriteBlock(b []byte, blockID int32) error {
	if blockID < 0 || blockID >= bf.NumBlocks {
		return errors.Newf("blockID must be in [0, %d); got %d", bf.NumBlocks, blockID)
	}
	if len(b) != bf.BlockSize {
		return errors.Newf("len(b) must be %d; got %d", bf.BlockSize, len(b))
	}
	_, err := bf.File.WriteAt(b, int64(blockID)*int64(bf.BlockSize))
	return err
}

func (bf *BlockFile) Close() error {
	return bf.File.Close()
}

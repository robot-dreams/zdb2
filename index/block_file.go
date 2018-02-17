package index

import (
	"os"

	"github.com/dropbox/godropbox/errors"
	"github.com/robot-dreams/zdb2"
)

type blockFile struct {
	f         *os.File
	numBlocks int32
}

// Returns BlockID of the newly allocated block.
func (bf *blockFile) allocateBlock() (int32, error) {
	blockID := bf.numBlocks
	bf.numBlocks++
	err := bf.f.Truncate(int64(bf.numBlocks) * zdb2.BlockSize)
	if err != nil {
		return invalidBlockID, err
	}
	return blockID, nil
}

func (bf *blockFile) readBlock(b []byte, blockID int32) error {
	if blockID < 0 || blockID >= bf.numBlocks {
		return errors.Newf("blockID must be in [0, %d); got %d", bf.numBlocks, blockID)
	}
	if len(b) != zdb2.BlockSize {
		return errors.Newf("len(b) must be %d; got %d", zdb2.BlockSize, len(b))
	}
	_, err := bf.f.ReadAt(b, int64(blockID)*zdb2.BlockSize)
	return err
}

func (bf *blockFile) writeBlock(b []byte, blockID int32) error {
	if blockID < 0 || blockID >= bf.numBlocks {
		return errors.Newf("blockID must be in [0, %d); got %d", bf.numBlocks, blockID)
	}
	if len(b) != zdb2.BlockSize {
		return errors.Newf("len(b) must be %d; got %d", zdb2.BlockSize, len(b))
	}
	_, err := bf.f.WriteAt(b, int64(blockID)*zdb2.BlockSize)
	return err
}

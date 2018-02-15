package executor

import (
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"

	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/executor/stream"
)

var inMemorySortBatchSize = 100000

type byField struct {
	sortFieldPosition int
	sortFieldType     zdb2.Type
	descending        bool
	records           []zdb2.Record
}

var _ sort.Interface = (*byField)(nil)

func (b *byField) Len() int {
	return len(b.records)
}

func (b *byField) Swap(i, j int) {
	b.records[i], b.records[j] = b.records[j], b.records[i]
}

func (b *byField) Less(i, j int) bool {
	v1 := b.records[i][b.sortFieldPosition]
	v2 := b.records[j][b.sortFieldPosition]
	if b.descending {
		return zdb2.Less(b.sortFieldType, v2, v1)
	} else {
		return zdb2.Less(b.sortFieldType, v1, v2)
	}
}

type diskSort struct {
	*merge
	iter         zdb2.Iterator
	sortedRunDir string
}

var _ zdb2.Iterator = (*diskSort)(nil)

func NewDiskSort(
	iter zdb2.Iterator,
	t *zdb2.TableHeader,
	sortField string,
	descending bool,
) (*diskSort, error) {
	sortFieldPosition, sortFieldType := zdb2.MustFieldPositionAndType(t, sortField)
	sortedRunDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}
	var sortedRunPaths []string
	for runID := 0; ; runID++ {
		// Read a batch of records.
		records, err := zdb2.ReadAll(NewLimit(iter, inMemorySortBatchSize))
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		// Sort them in memory.
		sort.Sort(&byField{
			sortFieldPosition: sortFieldPosition,
			sortFieldType:     sortFieldType,
			descending:        descending,
			records:           records,
		})

		// Write the sorted run to disk.
		sortedRunPath := sortedRunDir + "/sorted-run-" + strconv.Itoa(runID)
		err = stream.WriteAll(sortedRunPath, t, records)
		if err != nil {
			return nil, err
		}
		sortedRunPaths = append(sortedRunPaths, sortedRunPath)
	}
	iters := make([]zdb2.Iterator, len(sortedRunPaths))
	for i, sortedRunPath := range sortedRunPaths {
		iter, err := stream.NewScan(sortedRunPath)
		if err != nil {
			return nil, err
		}
		iters[i] = iter
	}
	// TODO: Is it necessary to merge all sorted runs into a single file before
	// returning, to limit memory (and file descriptor) usage?
	merge, err := NewMerge(iters, t, sortField, descending)
	if err != nil {
		return nil, err
	}
	return &diskSort{
		merge:        merge,
		iter:         iter,
		sortedRunDir: sortedRunDir,
	}, nil
}

func (d *diskSort) Close() error {
	err := d.merge.Close()
	if err != nil {
		return err
	}
	err = d.iter.Close()
	if err != nil {
		return err
	}
	return os.RemoveAll(d.sortedRunDir)
}

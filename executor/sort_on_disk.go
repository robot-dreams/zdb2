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

type sortOnDisk struct {
	*merge
	iter         zdb2.Iterator
	sortedRunDir string
}

var _ zdb2.Iterator = (*sortOnDisk)(nil)

func NewSortOnDisk(
	iter zdb2.Iterator,
	sortField string,
	descending bool,
) (*sortOnDisk, error) {
	t := iter.TableHeader()
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
	return &sortOnDisk{
		merge:        merge,
		iter:         iter,
		sortedRunDir: sortedRunDir,
	}, nil
}

func (s *sortOnDisk) Close() error {
	err := s.merge.Close()
	if err != nil {
		return err
	}
	err = s.iter.Close()
	if err != nil {
		return err
	}
	return os.RemoveAll(s.sortedRunDir)
}

package executor

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	"github.com/robot-dreams/zdb2"
)

type DiskSortSuite struct{}

var _ = Suite(&DiskSortSuite{})

func checkDiskSort(
	c *C,
	iter zdb2.Iterator,
	sortField string,
	descending bool,
) {
	d, err := NewDiskSort(iter, sortField, descending)
	c.Assert(err, IsNil)
	records, err := zdb2.ReadAll(d)
	c.Assert(err, IsNil)

	sortFieldPosition, sortFieldType := zdb2.MustFieldPositionAndType(
		iter.TableHeader(),
		sortField)
	for i := 1; i < len(records); i++ {
		v1 := records[i-1][sortFieldPosition]
		v2 := records[i][sortFieldPosition]
		if descending {
			c.Assert(zdb2.Less(sortFieldType, v1, v2), IsFalse)
		} else {
			c.Assert(zdb2.Less(sortFieldType, v2, v1), IsFalse)
		}
	}
	err = d.Close()
	c.Assert(err, IsNil)
}

func (s *DiskSortSuite) TestDiskSort(c *C) {
	// Setting a small inMemorySortLimit lets us actually test the case where
	// multiple passes are required.
	oldInMemorySortBatchSize := inMemorySortBatchSize
	inMemorySortBatchSize = 10
	defer func() {
		inMemorySortBatchSize = oldInMemorySortBatchSize
	}()

	t := &zdb2.TableHeader{
		Name: "movies",
		Fields: []*zdb2.Field{
			{"movie", zdb2.String},
			{"rating", zdb2.Float64},
			{"year", zdb2.Int32},
		},
	}
	records := []zdb2.Record{
		{"Leon: The Professional", 4.6, int32(1994)},
		{"Gattaca", 4.5, int32(1997)},
		{"Hackers", 3.7, int32(1995)},
		{"Inside Out", 4.7, int32(2015)},
	}
	for _, fieldName := range []string{"movie", "rating", "year"} {
		for _, descending := range []bool{false, true} {
			checkDiskSort(c, NewInMemoryScan(t, records), fieldName, descending)
		}
	}

	t = &zdb2.TableHeader{
		Name: "movies",
		Fields: []*zdb2.Field{
			{"movieId", zdb2.Int32},
			{"title", zdb2.String},
			{"genres", zdb2.String},
		},
	}
	for _, fieldName := range []string{"movieId", "title", "genres"} {
		for _, descending := range []bool{false, true} {
			iter, err := NewCSVScan("movies.csv", t)
			c.Assert(err, IsNil)
			checkDiskSort(c, iter, fieldName, descending)
		}
	}
}

package executor

import (
	"io"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	"github.com/robot-dreams/zdb2"
)

type AverageSuite struct{}

var _ = Suite(&AverageSuite{})

func (s *AverageSuite) TestAverage(c *C) {
	t := &zdb2.TableHeader{
		Name: "movies",
		Fields: []*zdb2.Field{
			{"movie", zdb2.String},
			{"rating", zdb2.Float64},
			{"views", zdb2.Int32},
		},
	}
	records := []zdb2.Record{
		{"Leon: The Professional", 4.6, int32(2)},
		{"Gattaca", 4.5, int32(2)},
		{"Hackers", 3.7, int32(3)},
		{"Inside Out", 4.7, int32(3)},
	}
	average, err := NewAverage(zdb2.NewInMemoryScan(t, records), "rating", "views")
	c.Assert(err, IsNil)
	c.Assert(
		average.TableHeader(),
		DeepEquals,
		&zdb2.TableHeader{
			Name: "average(movies.rating)",
			Fields: []*zdb2.Field{
				{"views", zdb2.Int32},
				{"average", zdb2.Float64},
			},
		})
	record, err := average.Next()
	c.Assert(err, IsNil)
	c.Assert(len(record), Equals, 2)
	c.Assert(record[0], Equals, int32(2))
	c.Assert(record[1], AlmostEqual, 4.55, 1e-9)
	record, err = average.Next()
	c.Assert(err, IsNil)
	c.Assert(len(record), Equals, 2)
	c.Assert(record[0], Equals, int32(3))
	c.Assert(record[1], AlmostEqual, 4.2, 1e-9)
	_, err = average.Next()
	c.Assert(err, Equals, io.EOF)
	err = average.Close()
	c.Assert(err, IsNil)
}

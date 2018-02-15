package encoding

import (
	"io"
	"os"

	. "gopkg.in/check.v1"

	"github.com/robot-dreams/zdb2"
)

type CSVScanSuite struct{}

var _ = Suite(&CSVScanSuite{})

func (s *CSVScanSuite) TestCSVScan(c *C) {
	t := &zdb2.TableHeader{
		Name: "movies",
		Fields: []*zdb2.Field{
			{"title", zdb2.String},
			{"rating", zdb2.Float64},
		},
	}
	csvData := `title,rating
The Shawshank Redemption,9.3
The Godfather,9.2
The Dark Knight,9.0
The Godfather: Part II,9.0
Pulp Fiction,8.9
Schindler's List,8.9
The Lord of the Rings: The Return of the King,8.9
12 Angry Men,8.9
The Good the Bad and the Ugly,8.9
The Lord of the Rings: The Fellowship of the Ring,8.8`
	path := c.MkDir() + "/movies.csv"
	f, err := os.Create(path)
	c.Assert(err, IsNil)
	_, err = io.WriteString(f, csvData)
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err, IsNil)
	csvScan, err := NewCSVScan(path, t)
	c.Assert(err, IsNil)
	expectedRecords := []zdb2.Record{
		{"The Shawshank Redemption", 9.3},
		{"The Godfather", 9.2},
		{"The Dark Knight", 9.0},
		{"The Godfather: Part II", 9.0},
		{"Pulp Fiction", 8.9},
		{"Schindler's List", 8.9},
		{"The Lord of the Rings: The Return of the King", 8.9},
		{"12 Angry Men", 8.9},
		{"The Good the Bad and the Ugly", 8.9},
		{"The Lord of the Rings: The Fellowship of the Ring", 8.8},
	}
	zdb2.CheckIterator(c, csvScan, expectedRecords)
}

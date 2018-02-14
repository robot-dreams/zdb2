package zdb2

import (
	"io"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

// CheckIterator should only be used in tests.
func CheckIterator(c *C, iter Iterator, expected []Record) {
	// Ensure that the Iterator contains exactly the expected Records.
	for _, record := range expected {
		actual, err := iter.Next()
		c.Assert(err, IsNil)
		c.Assert(actual.Equals(record), IsTrue)
	}
	_, err := iter.Next()
	c.Assert(err, Equals, io.EOF)
	// Repeated calls to Next should continue to return io.EOF after the
	// reaching the end of the Iterator.
	_, err = iter.Next()
	c.Assert(err, Equals, io.EOF)
	_, err = iter.Next()
	c.Assert(err, Equals, io.EOF)
	// Repeated calls to Close should be handled properly.
	err = iter.Close()
	c.Assert(err, IsNil)
	err = iter.Close()
	c.Assert(err, IsNil)
}

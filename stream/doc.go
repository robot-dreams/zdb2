// The stream package defines a binary representation that's useful as an
// intermediate representation when working with streams of Records (e.g. for
// out-of-core sorting or hashing algorithms).  Although we use buffered I/O
// everywhere, there's no explicit notion of "blocks".
package stream

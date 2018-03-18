# zdb2

zdb2 is a toy implementation of a relational database; this was my project for the [databases courses at Bradfield CS](https://bradfieldcs.com/courses/databases/) in the February 2018 session of the course.

This is mostly meant to explore some interesting ideas in the field of databases; it's NOT an integrated, fully featured system!  For example, constructing a query involves manually instantiating various executor implementations and combining them by hand (in some `.go` file).

## Interesting Features

- [Out-of-core mergesort](https://github.com/robot-dreams/zdb2/blob/master/executor/sort_on_disk.go)
- [Hybrid hash join](https://github.com/robot-dreams/zdb2/blob/master/executor/hash_join_hybrid.go)
    - Based on the paper [Join Processing in Database Systems with Large Main Memories](http://www.cs.ucr.edu/~tsotras/cs236/W15/join.pdf)
- [On-disk B+ tree index](https://github.com/robot-dreams/zdb2/tree/master/index)
- [Lock manager (for 2-phase locking) with deadlock detector](https://github.com/robot-dreams/zdb2/blob/master/lock_mgr/lock_manager.go)
    - This illustrates most of the main ideas, but actually it's totally broken right now
- [Binary format for heap files](https://github.com/robot-dreams/zdb2/tree/master/heap_file)

## Helpful Resources

- [The Databases Red Book](http://www.redbook.io/)
- [Use the Index, Luke](https://use-the-index-luke.com/)
- [PostgreSQL Source Code](https://github.com/postgres/postgres)

## Future Improvements

Honestly, I'm probably never going to do these.

- B+ tree index
    - Handle deletes
    - Support variable length keys
        - Prefix / suffix compression
    - Look into how to handle concurrent access
- Joins
    - Use "tournament sort" for generating initial sorted runs
    - Implement nested-loop join variants
        - Naive
        - Page-oriented
        - Chunk-oriented
        - Index
    - Implement sort-merge join
    - Implement out-of-core hashing for aggregations
- Lock manager
    - Fix deadlock detection for shared -> exclusive lock upgrade
    - Fix wait graph construction
- Misc
    - Add a system catalog
    - Add a query optimizer based on Selinger's algorithm
    - Design a convenient query plan representation

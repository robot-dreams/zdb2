package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/robot-dreams/zdb2/index"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	var flagNumKeys int
	var flagNumEntriesPerKey int
	flag.IntVar(
		&flagNumKeys,
		"num_keys",
		100000,
		"number of distinct keys to load")
	flag.IntVar(
		&flagNumEntriesPerKey,
		"num_entries_per_key",
		10,
		"number of entries to load for each key")
	flag.Parse()

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	generateEntry := func(key int32, offset int) index.Entry {
		return index.Entry{
			Key: key,
			RID: index.RecordID{
				PageID: int32(offset),
				SlotID: uint16(offset),
			},
		}
	}
	entries := make([]index.Entry, 0, flagNumKeys*flagNumEntriesPerKey)
	for i := 0; i < flagNumKeys; i++ {
		for j := 0; j < flagNumEntriesPerKey; j++ {
			entries = append(entries, generateEntry(int32(i)*5, j))
		}
	}

	path := dir + "/bulk_load_benchmark"
	tree, err := index.BulkLoadNewBPlusTree(path, entries, 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Done bulk loading %v entries!\n", len(entries))

	err = tree.Close()
	if err != nil {
		log.Fatal(err)
	}
}

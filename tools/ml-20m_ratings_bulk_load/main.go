package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"sort"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/executor"
	"github.com/robot-dreams/zdb2/heap_file"
	"github.com/robot-dreams/zdb2/index"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	var flagInput string
	var flagHeapFile string
	var flagIndexFile string
	flag.StringVar(&flagInput, "input", "", "path to input ratings table (csv)")
	flag.StringVar(&flagHeapFile, "heap_file", "", "path to output ratings table (heap file)")
	flag.StringVar(&flagIndexFile, "index_file", "", "path to output ratings index (B+ tree)")
	flag.Parse()
	if flagInput == "" || flagHeapFile == "" || flagIndexFile == "" {
		log.Fatal("input, heap_file, and index_file flags must all be provided")
	}
	t := &zdb2.TableHeader{
		Name: "ratings",
		Fields: []*zdb2.Field{
			{"userId", zdb2.Int32},
			{"movieId", zdb2.Int32},
			{"rating", zdb2.Float64},
			{"timestamp", zdb2.Int32},
		},
	}

	fmt.Println("Starting timer...")
	start := time.Now()
	csvScan, err := executor.NewCSVScan(flagInput, t)
	if err != nil {
		log.Fatal(err)
	}
	err = heap_file.BulkLoadNewHeapFile(flagHeapFile, csvScan)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"Done bulk loading heap file %v after %v\n",
		flagHeapFile,
		time.Since(start))

	fmt.Println("Resetting timer...")
	start = time.Now()
	heapFileScan, err := heap_file.NewFileScan(flagHeapFile)
	if err != nil {
		log.Fatal(err)
	}
	var entries []index.Entry
	for {
		record, recordID, err := heapFileScan.NextWithID()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		entries = append(
			entries,
			index.Entry{
				Key: record[1].(int32),
				RID: recordID,
			})
	}
	fmt.Printf(
		"Done reading index entries into memory after %v\n",
		time.Since(start))

	fmt.Println("Resetting timer...")
	start = time.Now()
	sort.Sort(index.ByKey(entries))
	fmt.Printf(
		"Done sorting index entries after %v\n",
		time.Since(start))

	fmt.Println("Resetting timer...")
	start = time.Now()
	bpt, err := index.BulkLoadNewBPlusTree(flagIndexFile, entries, 1)
	if err != nil {
		log.Fatal(err)
	}
	err = bpt.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"Done bulk loading index %v after %v\n",
		flagIndexFile,
		time.Since(start))
}

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/robot-dreams/zdb2"
	"github.com/robot-dreams/zdb2/executor"
	"github.com/robot-dreams/zdb2/heap_file"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	var flagInput string
	var flagOutput string
	flag.StringVar(&flagInput, "input", "", "path to input ratings table (csv)")
	flag.StringVar(&flagOutput, "output", "", "path to output ratings table (heap file)")
	flag.Parse()
	if flagInput == "" || flagOutput == "" {
		log.Fatal("input and output flags must be provided")
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
	start := time.Now()
	csvScan, err := executor.NewCSVScan(flagInput, t)
	if err != nil {
		log.Fatal(err)
	}
	err = heap_file.BulkLoadNewHeapFile(flagOutput, csvScan)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"Done bulk loading after %v\n",
		time.Since(start))
	fmt.Println("Resetting timer...")
	start = time.Now()
	heapFileScan, err := heap_file.NewScan(flagOutput)
	if err != nil {
		log.Fatal(err)
	}
	numRecords := 0
	for {
		_, err = heapFileScan.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		numRecords++
	}
	fmt.Printf(
		"Done scanning all %v records in heap file after %v\n",
		numRecords,
		time.Since(start))
}

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
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	var flagPath string
	flag.StringVar(&flagPath, "path", "", "path to ratings table (csv format)")
	flag.Parse()
	if flagPath == "" {
		log.Fatal("path flag must be provided")
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
	csvScan, err := executor.NewCSVScan(flagPath, t)
	if err != nil {
		log.Fatal(err)
	}
	diskSort, err := executor.NewDiskSort(csvScan, "rating", true)
	if err != nil {
		log.Fatal(err)
	}
	record, err := diskSort.Next()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"Done writing sorted runs after %v!  First record: %v\n",
		time.Since(start),
		record)
	numRecords := 1
	for {
		_, err = diskSort.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		numRecords++
	}
	fmt.Printf(
		"Done iterating through all %v sorted records after %v\n",
		numRecords,
		time.Since(start))
}

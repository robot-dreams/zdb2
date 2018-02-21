package main

import (
	"flag"
	"fmt"
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
	var flagHeapFile string
	var flagIndexFile string
	var flagMovieId int
	flag.StringVar(&flagHeapFile, "heap_file", "", "path to ratings table (heap file)")
	flag.StringVar(&flagIndexFile, "index_file", "", "path to ratings index on movieId (B+ tree)")
	flag.IntVar(&flagMovieId, "movieId", 5000, "movieId to look up")
	flag.Parse()
	if flagHeapFile == "" || flagIndexFile == "" {
		log.Fatal("heap_file and index_file flags must both be provided")
	}

	fmt.Println("Starting timer...")
	start := time.Now()
	fileScan, err := heap_file.NewFileScan(flagHeapFile)
	if err != nil {
		log.Fatal(err)
	}
	selection := executor.NewSelection(
		fileScan,
		zdb2.FieldEquals(fileScan.TableHeader(), "movieId", int32(flagMovieId)))
	if err != nil {
		log.Fatal(err)
	}
	err = printAverageRatingsByMovieID(selection)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"Done with file scan strategy after %v\n",
		time.Since(start))

	fmt.Println("Resetting timer...")
	start = time.Now()
	indexScan, err := heap_file.NewIndexScanEqual(
		flagIndexFile,
		flagHeapFile,
		int32(flagMovieId))
	if err != nil {
		log.Fatal(err)
	}
	err = printAverageRatingsByMovieID(indexScan)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"Done with index scan strategy after %v\n",
		time.Since(start))
}

func printAverageRatingsByMovieID(ratingsIter zdb2.Iterator) error {
	movieIDRating := executor.NewProjection(ratingsIter, []string{"movieId", "rating"})
	averageRating, err := executor.NewAverage(movieIDRating, "rating", "movieId")
	if err != nil {
		log.Fatal(err)
	}
	records, err := zdb2.ReadAll(averageRating)
	if err != nil {
		return err
	}
	fmt.Printf("%10v | %v\n", "movieId", "average rating")
	fmt.Printf("--------------------------------\n")
	for _, record := range records {
		fmt.Printf("%10d | %f\n", record[0], record[1])
	}
	return averageRating.Close()
}

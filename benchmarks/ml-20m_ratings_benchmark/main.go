package main

import (
	"flag"
	"fmt"
	"log"

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
	ratings, err := executor.NewCSVScan(
		flagPath,
		&zdb2.TableHeader{
			Name: "ratings",
			Fields: []*zdb2.Field{
				{"userId", zdb2.Int32},
				{"movieId", zdb2.Int32},
				{"rating", zdb2.Float64},
				{"timestamp", zdb2.Int32},
			},
		})
	if err != nil {
		log.Fatal(err)
	}
	movieIDRating := executor.NewProjection(ratings, []string{"movieId", "rating"})
	byMovieID, err := executor.NewSortOnDisk(movieIDRating, "movieId", false)
	if err != nil {
		log.Fatal(err)
	}
	averageRating, err := executor.NewAverage(byMovieID, "rating", "movieId")
	if err != nil {
		log.Fatal(err)
	}
	byRatingDescending, err := executor.NewSortInMemory(averageRating, "average", true)
	if err != nil {
		log.Fatal(err)
	}
	limit := executor.NewLimit(byRatingDescending, 10)
	records, err := zdb2.ReadAll(limit)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%10v | %v\n", "movieId", "average rating")
	fmt.Printf("--------------------------------\n")
	for _, record := range records {
		fmt.Printf("%10d | %f\n", record[0], record[1])
	}
	err = limit.Close()
	if err != nil {
		log.Fatal(err)
	}
}

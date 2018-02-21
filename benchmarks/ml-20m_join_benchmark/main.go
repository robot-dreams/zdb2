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
	var flagRatings string
	var flagMovies string
	flag.StringVar(&flagRatings, "ratings", "", "path to ratings table (csv format)")
	flag.StringVar(&flagMovies, "movies", "", "path to movies table (csv format)")
	flag.Parse()
	if flagRatings == "" || flagMovies == "" {
		log.Fatal("ratings and movies flags must both be provided")
	}
	ratings, err := executor.NewCSVScan(
		flagRatings,
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
	movies, err := executor.NewCSVScan(
		flagMovies,
		&zdb2.TableHeader{
			Name: "movies",
			Fields: []*zdb2.Field{
				{"movieId", zdb2.Int32},
				{"title", zdb2.String},
				{"genres", zdb2.String},
			},
		})
	if err != nil {
		log.Fatal(err)
	}
	joined, err := executor.NewHashJoinClassic(
		movies, ratings, "movieId", "movieId")
	if err != nil {
		log.Fatal(err)
	}
	predicate := zdb2.FieldEquals(
		joined.TableHeader(),
		"movies.title",
		"Medium Cool (1969)")
	selection := executor.NewSelection(joined, predicate)
	movieIDRating := executor.NewProjection(
		selection,
		[]string{"movies.title", "ratings.rating"})
	averageRating, err := executor.NewAverage(
		movieIDRating,
		"ratings.rating",
		"movies.title")
	if err != nil {
		log.Fatal(err)
	}
	records, err := zdb2.ReadAll(averageRating)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%20v | %v\n", "title", "average rating")
	fmt.Printf("------------------------------------------\n")
	for _, record := range records {
		fmt.Printf("%20v | %f\n", record[0], record[1])
	}
	err = averageRating.Close()
	if err != nil {
		log.Fatal(err)
	}
}

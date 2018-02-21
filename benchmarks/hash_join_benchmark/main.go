package main

import (
	"flag"
	"fmt"
	"io"
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
	t := &zdb2.TableHeader{
		Name: "ratings",
		Fields: []*zdb2.Field{
			{"userId", zdb2.Int32},
			{"movieId", zdb2.Int32},
			{"rating", zdb2.Float64},
			{"timestamp", zdb2.Int32},
		},
	}
	r, err := executor.NewCSVScan(flagPath, t)
	if err != nil {
		log.Fatal(err)
	}
	s, err := executor.NewCSVScan(flagPath, t)
	if err != nil {
		log.Fatal(err)
	}
	joined, err := executor.NewHashJoinHybrid(
		r, s,
		"timestamp", "timestamp",
		false,
		0.1,
		9)
	if err != nil {
		log.Fatal(err)
	}
	count := 0
	for {
		_, err := joined.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		count++
	}
	err = joined.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Finished iterating through %v joined records\n", count)
}

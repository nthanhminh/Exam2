package main

import (
	"context"
	"flag"
	"log"
)

func main() {
    port := flag.Int("p", 27017, "DATABASE_PORT")
    maxPoolSize := flag.Int("max", 200, "MAX_POOL_SIZE")
	minPoolSize := flag.Int("min", 100, "MIN_POOL_SIZE")
	filePath := flag.String("f", "", "The path of input data file")
	wokers:= flag.Int("w", 60, "The number of workers")

    flag.Parse()

	log.Printf("Port: %d, maxPoolSize: %d, minPoolSize: %d, filePath: %s", *port, *maxPoolSize, *minPoolSize, *filePath)

	client, err := ConnectToDB(*port, *maxPoolSize, *minPoolSize)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	importGeoJSON(ctx, client, *filePath, *wokers)

	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()
}

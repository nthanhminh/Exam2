package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func getDBConfig(port int, maxPoolSize int, minPoolSize int) string {
	uri:= fmt.Sprintf(`mongodb://localhost:%d/?maxPoolSize=%d&minPoolSize=%d`, port, maxPoolSize, minPoolSize)
	fmt.Println(uri);
	return uri
}

func ConnectToDB(port, maxPoolSize, minPoolSize int) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := getDBConfig(port, maxPoolSize, minPoolSize)

	clientOpts := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	fmt.Println("Connected to MongoDB with connection pool!")

	return client, nil
}

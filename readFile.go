package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type GeoFeature struct {
    Type     string `json:"type"`
    Geometry struct {
        Type        string        `json:"type"`
        Coordinates [][][]float64 `json:"coordinates"`
    } `json:"geometry"`
    Properties struct {
        Release           int    `json:"release"`
        CaptureDatesRange string `json:"capture_dates_range"`
    } `json:"properties"`
}

func importGeoJSON(ctx context.Context, client *mongo.Client, filePath string, workers int) {
    file, err := os.Open(filePath)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    decoder := json.NewDecoder(file)
    collection := client.Database("Exam").Collection("GeoData")

    featureChan := make(chan GeoFeature, 1000)

    var wg sync.WaitGroup
    var successCount, failCount uint64

    for i := 0; i < workers; i++ {
        wg.Add(1)
        go worker(ctx, collection, featureChan, &wg, &successCount, &failCount, i)
    }

    startTime := time.Now()

    log.Println("Start importing")
    readFeatures(decoder, featureChan)
    wg.Wait()

    log.Printf("Successfully inserted: %d\n", successCount)
    log.Printf("Failed inserts: %d\n", failCount)
    log.Printf("Done: %.2f giây\n", time.Since(startTime).Seconds())
}

func readFeatures(decoder *json.Decoder, featureChan chan<- GeoFeature) {
    defer close(featureChan)

    expectToken(decoder, json.Delim('{'))

    for decoder.More() {
        key := readKey(decoder)

        if key == "features" {
            expectToken(decoder, json.Delim('['))

            for decoder.More() {
                var feature GeoFeature
                if err := decoder.Decode(&feature); err != nil {
                    log.Fatal(err)
                }
                featureChan <- feature
            }

            expectToken(decoder, json.Delim(']'))
        } else {
            var skip interface{}
            decoder.Decode(&skip)
        }
    }

    expectToken(decoder, json.Delim('}'))
}

func worker(ctx context.Context, collection *mongo.Collection, featureChan <-chan GeoFeature, wg *sync.WaitGroup, successCount, failCount *uint64, workerID int) {
    defer wg.Done()

    batchSize:= 500;

    var batch []interface{}

    for feature := range featureChan {
        batch = append(batch, feature)

        if len(batch) >= batchSize {
            inserted, failed := insertBatch(ctx, collection, batch, workerID)
            atomic.AddUint64(successCount, uint64(inserted))
            atomic.AddUint64(failCount, uint64(failed))
            batch = batch[:0]
        }
    }

    if len(batch) > 0 {
        inserted, failed := insertBatch(ctx, collection, batch, workerID)
        atomic.AddUint64(successCount, uint64(inserted))
        atomic.AddUint64(failCount, uint64(failed))
    }
}

func insertBatch(ctx context.Context, collection *mongo.Collection, batch []interface{}, workerID int) (int, int) {
    opts := options.InsertMany().SetOrdered(false)
    result, err := collection.InsertMany(ctx, batch, opts)

    if err != nil {
        log.Printf("Worker %d batch insert error: %v\n", workerID, err)
    }

    success := len(result.InsertedIDs)
    fail := len(batch) - success

    return success, fail
}

func expectToken(decoder *json.Decoder, expected json.Delim) {
    token, err := decoder.Token()
    if err != nil {
        log.Fatal(err)
    }
    if token != expected {
        log.Fatalf("JSON error: expected %v but found %v", expected, token)
    }
}

func readKey(decoder *json.Decoder) string {
    token, err := decoder.Token()
    if err != nil {
        log.Fatal(err)
    }

    key, ok := token.(string)
    if !ok {
        log.Fatal("JSON error: expected a string key")
    }
    return key
}

package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/nicholasaperry/event-logging/constants"
	"github.com/nicholasaperry/event-logging/db"
	"github.com/nicholasaperry/event-logging/kafka"
	"github.com/nicholasaperry/event-logging/worker"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

var (
	numWorkers = 100
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := db.Connect()
	if err != nil {
		panic("failed to connect to database: " + err.Error())
	}

	workers := make(chan struct{}, numWorkers)

	consumer, err := kafka.NewConsumer(ctx, constants.DeviceEventsTopic)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	log.Println("Connected to Kafka consumer")
	var counter atomic.Int64
	counter.Store(0)
	var wg sync.WaitGroup
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			fetches := consumer.PollFetches(ctx)
			if err := fetches.Err(); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			fetches.EachRecord(func(record *kgo.Record) {
				workers <- struct{}{}
				wg.Add(1)
				go func() {
					println("firing goroutine")
					defer wg.Done()
					defer func() { <-workers }()
					worker.ConsumeKafkaMessage(ctx, db, record.Value)
					consumer.MarkCommitRecords(record)
					counter.Add(1)
				}()
			})
			wg.Wait()
			consumer.CommitMarkedOffsets(ctx)
			println("processed ", counter.Load(), " messages")
		}
	})

	<-ctx.Done()
	consumer.Close()
	if err := g.Wait(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

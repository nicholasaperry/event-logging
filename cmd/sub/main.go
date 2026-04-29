package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nicholasaperry/event-logging/constants"
	"github.com/nicholasaperry/event-logging/db"
	"github.com/nicholasaperry/event-logging/kafka"
	"github.com/nicholasaperry/event-logging/worker"
	"golang.org/x/sync/errgroup"
)

var (
	bufferSize = 100000
	workers    = 10000
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	msgs := make(chan mqtt.Message, bufferSize)

	db, err := db.Connect()
	if err != nil {
		panic("failed to connect to database: " + err.Error())
	}

	handler := func(client mqtt.Client, msg mqtt.Message) {
		select {
		case msgs <- msg:
		default:
			// log.Printf("WARN: dropping message, channel full")
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("subscriber-" + fmt.Sprintf("%d", os.Getpid())).
		SetCleanSession(true).
		SetConnectTimeout(10 * time.Second)
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		if tok := client.Subscribe("devices/+/events", 1, handler); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			log.Printf("subscribe failed: %v", tok.Error())
			os.Exit(1)
		}
	})

	client := mqtt.NewClient(opts)
	tok := client.Connect()
	if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		log.Fatalf("connect failed: %v", tok.Error())
	}

	producer, err := kafka.NewProducer(ctx, constants.DeviceEventsTopic)
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}
	log.Println("Connected to MQTT broker")

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for {
				select {
				case msg, ok := <-msgs:
					if !ok {
						return errors.New("channel closed")
					}
					worker.BridgeMqttToKafka(ctx, i, db, msg, producer)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	<-ctx.Done()
	client.Disconnect(500)
	close(msgs)
	if err := g.Wait(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

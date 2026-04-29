package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nicholasaperry/event-logging/constants"
	"github.com/nicholasaperry/event-logging/db"
	"github.com/nicholasaperry/event-logging/models"
	"gorm.io/datatypes"
)

func main() {
	start := time.Now()
	_, err := db.Connect()
	if err != nil {
		panic("failed to connect to database: " + err.Error())
	}

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("publisher-" + fmt.Sprintf("%d", os.Getpid())).
		SetCleanSession(true).
		SetConnectTimeout(10 * time.Second)

	client := mqtt.NewClient(opts)
	tok := client.Connect()
	if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		log.Fatalf("connect failed: %v", tok.Error())
	}
	defer client.Disconnect(500)
	log.Println("Connected to MQTT broker")

	wg := sync.WaitGroup{}
	ch := make(chan struct{}, 1000)
	for i := 0; i < 100000; i++ {
		ch <- struct{}{}
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer func() { <-ch }()

			ev := models.Event{
				DeviceID:  constants.DeviceIDs[rand.Intn(len(constants.DeviceIDs))],
				Timestamp: time.Now().UnixMilli(),
				EventType: constants.EventTypes[rand.Intn(len(constants.EventTypes))],
				EventData: datatypes.NewJSONType(models.DeviceMetric{
					Value: rand.Float64() * 100,
					Unit:  "celsius",
				}),
			}

			payload, err := json.Marshal(ev)
			if err != nil {
				log.Printf("marshal error: %v", err)
				return
			}

			topic := fmt.Sprintf("devices/%s/events", ev.DeviceID)
			tok := client.Publish(topic, 1, false, payload)
			if !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
				log.Printf("publish %d failed: %v", i, tok.Error())
				return
			}

			if i%100 == 0 {
				log.Printf("published %d/1000 → topic=%s", i, topic)
			}
		}()
	}

	wg.Wait()
	log.Printf("Done — 1000 events published in %s", time.Since(start))

}

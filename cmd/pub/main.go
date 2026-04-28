package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nicholasaperry/event-logging/models"
	"gorm.io/datatypes"
)

func main() {
	_, err := models.ConnectToDB()
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

	for i := 0; i < 1000; i++ {
		ev := models.Event{
			DeviceID:  models.DeviceIDs[rand.Intn(len(models.DeviceIDs))],
			Timestamp: time.Now().UnixMilli(),
			EventType: models.EventTypes[rand.Intn(len(models.EventTypes))],
			EventData: datatypes.NewJSONType(models.DeviceMetric{
				Value: rand.Float64() * 100,
				Unit:  "celsius",
			}),
		}

		payload, err := json.Marshal(ev)
		if err != nil {
			log.Printf("marshal error: %v", err)
			continue
		}

		topic := fmt.Sprintf("devices/%s/events", ev.DeviceID)
		tok := client.Publish(topic, 1, false, payload)
		if !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
			log.Printf("publish %d failed: %v", i, tok.Error())
			continue
		}

		if i%100 == 0 {
			log.Printf("published %d/1000 → topic=%s", i, topic)
		}
	}

	log.Println("Done — 1000 events published")

}

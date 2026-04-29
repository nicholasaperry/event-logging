package worker

import (
	"context"
	"encoding/json"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nicholasaperry/event-logging/kafka"
	"github.com/nicholasaperry/event-logging/models"
	"gorm.io/gorm"
)

func BridgeMqttToKafka(ctx context.Context, id int, db *gorm.DB, msg mqtt.Message, producer kafka.Producer) error {
	payload := msg.Payload()
	event := models.Event{}
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}
	if err := producer.Publish(ctx, event.DeviceID, payload); err != nil {
		return err
	}
	msg.Ack()
	return nil
}

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
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		log.Printf("worker %d panicked: %v\n%s", id, r, debug.Stack())
	// 		// maybe increment a panic counter metric
	// 	}
	// }()
	// event := models.Event{}
	// if err := json.Unmarshal(msg.Payload(), &event); err != nil {
	// 	log.Printf("worker %d unmarshal error: %v", id, err)
	// 	return
	// }
	// if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
	// 	if err := tx.Create(&event).Error; err != nil {
	// 		return err
	// 	}

	// 	if err := tx.Model(&models.Device{}).
	// 		Where("id = ?", event.DeviceID).
	// 		Update("last_seen", event.Timestamp).
	// 		Error; err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }); err != nil {
	// 	log.Printf("worker %d transaction error: %v", id, err)
	// 	return
	// }
	// msg.Ack()
}

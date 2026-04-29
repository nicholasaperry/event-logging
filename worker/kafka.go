package worker

import (
	"context"
	"encoding/json"
	"log"
	"runtime/debug"

	"github.com/nicholasaperry/event-logging/models"
	"gorm.io/gorm"
)

func ConsumeKafkaMessage(ctx context.Context, db *gorm.DB, payload []byte) error {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panicked: %v\n%s", r, debug.Stack())
			// maybe increment a panic counter metric
		}
	}()
	event := models.Event{}
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}
	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&event).Error; err != nil {
			return err
		}

		if err := tx.Model(&models.Device{}).
			Where("id = ?", event.DeviceID).
			Update("last_seen", event.Timestamp).
			Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

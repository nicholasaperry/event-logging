package db

import (
	"time"

	"github.com/nicholasaperry/event-logging/constants"
	"github.com/nicholasaperry/event-logging/models"
	"gorm.io/gorm"
)

func SeedDevices(db *gorm.DB) error {
	for _, id := range constants.DeviceIDs {
		device := &models.Device{ID: id, Status: "online", LastSeen: time.Now().UnixMilli()}
		if err := db.FirstOrCreate(device).Error; err != nil {
			return err
		}
	}
	return nil
}

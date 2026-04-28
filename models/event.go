package models

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

var (
	EventTypes = []string{"temperature", "humidity", "pressure", "battery"}
	DeviceIDs  = []string{"dev-001", "dev-002", "dev-003", "dev-004", "dev-005"}
)

type DeviceMetric struct {
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

type Device struct {
	ID       string    `gorm:"primaryKey;size:64"`
	LastSeen time.Time `gorm:"index"`
	Status   string    `gorm:"size:16"`
	Events   []Event   `gorm:"foreignKey:DeviceID;references:ID"`
}

type Event struct {
	gorm.Model
	DeviceID  string                           `json:"device_id" gorm:"index;size:64;not null"`
	Timestamp int64                            `json:"timestamp"`
	EventType string                           `json:"event_type" gorm:"index;size:32;not null"`
	EventData datatypes.JSONType[DeviceMetric] `json:"event_data"`
}

func SeedDevices(db *gorm.DB) {
	for _, id := range DeviceIDs {
		db.FirstOrCreate(&Device{ID: id}, Device{ID: id, Status: "online", LastSeen: time.Now()})
	}
}

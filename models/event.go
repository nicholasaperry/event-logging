package models

import (
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

type Event struct {
	gorm.Model
	DeviceID  string                           `json:"device_id"`
	Timestamp int64                            `json:"timestamp"`
	EventType string                           `json:"event_type"`
	EventData datatypes.JSONType[DeviceMetric] `json:"event_data"`
}

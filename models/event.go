package models

import (
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type DeviceMetric struct {
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

type Event struct {
	gorm.Model
	DeviceID  string                           `json:"device_id" gorm:"index;size:64;not null"`
	Timestamp int64                            `json:"timestamp"`
	EventType string                           `json:"event_type" gorm:"index;size:32;not null"`
	EventData datatypes.JSONType[DeviceMetric] `json:"event_data"`
}

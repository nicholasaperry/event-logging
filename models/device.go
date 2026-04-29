package models

import "time"

type Device struct {
	ID       string    `gorm:"primaryKey;size:64"`
	LastSeen time.Time `gorm:"index"`
	Status   string    `gorm:"size:16"`
	Events   []Event   `gorm:"foreignKey:DeviceID;references:ID"`
}

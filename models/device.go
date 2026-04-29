package models

type Device struct {
	ID       string  `gorm:"primaryKey;size:64"`
	LastSeen int64   `gorm:"index"`
	Status   string  `gorm:"size:16"`
	Events   []Event `gorm:"foreignKey:DeviceID;references:ID"`
}

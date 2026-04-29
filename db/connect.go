package db

import (
	"github.com/nicholasaperry/event-logging/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func Connect() (*gorm.DB, error) {
	dsn := "host=localhost user=postgres password=postgres dbname=events port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if err := db.AutoMigrate(&models.Device{}, &models.Event{}); err != nil {
		return nil, err
	}

	if err := SeedDevices(db); err != nil {
		return nil, err
	}

	if err := db.Exec("TRUNCATE TABLE events RESTART IDENTITY").Error; err != nil {
		return nil, err
	}

	println("Connected to Postgres successfully!")
	return db, nil
}

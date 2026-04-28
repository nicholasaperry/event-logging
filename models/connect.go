package models

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func ConnectToDB() (*gorm.DB, error) {
	dsn := "host=localhost user=postgres password=swag123@@ dbname=zebra port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if err := db.AutoMigrate(&Device{}, &Event{}); err != nil {
		return nil, err
	}

	SeedDevices(db)

	if err := db.Exec("TRUNCATE TABLE events RESTART IDENTITY").Error; err != nil {
		return nil, err
	}

	println("Connected to Postgres successfully!")
	return db, nil
}

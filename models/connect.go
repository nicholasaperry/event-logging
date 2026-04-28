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
	db.AutoMigrate(&Event{})
	println("Connected to Postgres successfully!")
	return db, nil
}

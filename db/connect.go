package db

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

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

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	maxOpenConns, err := getenvInt("DB_MAX_OPEN_CONNS", 50)
	if err != nil {
		return nil, err
	}
	maxIdleConns, err := getenvInt("DB_MAX_IDLE_CONNS", 25)
	if err != nil {
		return nil, err
	}
	connMaxLifetime, err := getenvDuration("DB_CONN_MAX_LIFETIME", 15*time.Minute)
	if err != nil {
		return nil, err
	}
	connMaxIdleTime, err := getenvDuration("DB_CONN_MAX_IDLE_TIME", 5*time.Minute)
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetConnMaxLifetime(connMaxLifetime)
	sqlDB.SetConnMaxIdleTime(connMaxIdleTime)

	if err := db.Exec("DROP TABLE IF EXISTS events, devices CASCADE").Error; err != nil {
		return nil, err
	}
	if err := db.AutoMigrate(&models.Device{}, &models.Event{}); err != nil {
		return nil, err
	}
	if err := SeedDevices(db); err != nil {
		return nil, err
	}
	log.Printf(
		"Connected to Postgres: max_open_conns=%d max_idle_conns=%d conn_max_lifetime=%s conn_max_idle_time=%s",
		maxOpenConns,
		maxIdleConns,
		connMaxLifetime,
		connMaxIdleTime,
	)
	return db, nil
}

func getenvInt(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %q: %w", key, raw, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("invalid %s value %d: must be >= 0", key, value)
	}
	return value, nil
}

func getenvDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %q: %w", key, raw, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("invalid %s value %s: must be >= 0", key, value)
	}
	return value, nil
}

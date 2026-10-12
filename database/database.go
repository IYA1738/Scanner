package database

import (
	"database/sql"
	"fmt"
	"time"

	"example.com/scanner_demo/config"
	"example.com/scanner_demo/database/model"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Database struct {
	db    *gorm.DB
	sqlDB *sql.DB
}

func NewDatabase(cfg config.DatabaseConfig) (*Database, error) {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=disable",
		cfg.Host, cfg.User, cfg.Password, cfg.DBName, cfg.Port,
	)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	if err := db.AutoMigrate(&model.ScannerCursor{}); err != nil {
		return nil, fmt.Errorf("migrate scanner cursor: %w", err)
	}

	if err := db.AutoMigrate(&model.AddressBlacklist{}); err != nil {
		return nil, fmt.Errorf("migrate address blacklist: %w", err)
	}

	return &Database{
		db:    db,
		sqlDB: sqlDB,
	}, nil
}

func (d *Database) Close() error {
	return d.sqlDB.Close()
}

package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"example.com/scanner_demo/scanner/internal/config"
	"example.com/scanner_demo/scanner/internal/database/model"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Database struct {
	db    *gorm.DB
	sqlDB *sql.DB
}

func NewDatabase(ctx context.Context, cfg config.DatabaseConfig) (_ *Database, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=disable",
		cfg.Host, cfg.User, cfg.Password, cfg.DBName, cfg.Port,
	)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			sqlDB.Close()
		}
	}()
	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	initDB := db.WithContext(ctx)
	if err := initDB.AutoMigrate(&model.ScannerCursor{}); err != nil {
		return nil, fmt.Errorf("migrate scanner cursor: %w", err)
	}

	if err := initDB.AutoMigrate(&model.AddressBlacklist{}); err != nil {
		return nil, fmt.Errorf("migrate address blacklist: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return &Database{
		db:    db,
		sqlDB: sqlDB,
	}, nil
}

func (d *Database) Close() error {
	return d.sqlDB.Close()
}

package database

import (
	"context"
	"fmt"
	"time"

	"example.com/scanner_demo/scanner/internal/database/model"
	"gorm.io/gorm/clause"
)

func (d *Database) SaveScannerEvent(
	ctx context.Context,
	event *model.ScannerEvent,
) error {
	writeCtx, cancel := context.WithTimeout(
		ctx,
		10*time.Second,
	)
	defer cancel()

	err := d.db.WithContext(writeCtx).Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "event_id"}},
			DoNothing: true,
		},
	).Create(event).Error

	if err != nil {
		return fmt.Errorf("save scanner event failed: %w", err)
	}
	return nil
}

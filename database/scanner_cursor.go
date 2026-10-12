package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"example.com/scanner_demo/database/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (d *Database) GetScannerCursor(ctx context.Context, chainID int64) (*model.ScannerCursor, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var cursor model.ScannerCursor

	err := d.db.WithContext(queryCtx).Where("chain_id = ?", chainID).First(&cursor).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("get scanner cursor chain_id=%d: %w", chainID, err)
	}

	return &cursor, nil
}

func (d *Database) SaveScannerCursor(ctx context.Context, chainID int64, blockNumber uint64) error {
	updateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cursor := &model.ScannerCursor{
		ChainID:     chainID,
		BlockNumber: blockNumber,
	}

	err := d.db.WithContext(updateCtx).Clauses(
		clause.OnConflict{
			Columns: []clause.Column{
				{Name: "chain_id"},
			},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"block_number": gorm.Expr(
					"GREATEST(scanner_cursors.block_number, EXCLUDED.block_number)",
				),
			}),
		},
	).Create(cursor).Error

	if err != nil {
		return fmt.Errorf(
			"save scanner cursor chain_id=%d block=%d: %w",
			chainID, blockNumber, err,
		)
	}

	return nil
}

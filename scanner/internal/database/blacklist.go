package database

import (
	"context"

	"example.com/scanner_demo/scanner/internal/database/model"
	"gorm.io/gorm/clause"
)

func (d *Database) AddBlacklist(ctx context.Context, addr string, reason string) error {
	record := model.AddressBlacklist{
		Address: addr,
		Reason:  reason,
	}
	return d.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&record).
		Error
}

func (d *Database) DeleteBlacklist(ctx context.Context, addr string) error {
	return d.db.WithContext(ctx).
		Where("address = ?", addr).
		Delete(&model.AddressBlacklist{}).
		Error
}

func (d *Database) LoadBlacklist(ctx context.Context) ([]model.AddressBlacklist, error) {
	var records []model.AddressBlacklist

	err := d.db.WithContext(ctx).Find(&records).Error
	if err != nil {
		return nil, err
	}

	return records, nil
}

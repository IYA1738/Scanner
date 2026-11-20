package model

import "time"

type TokenInfo struct {
	ChainID        int64  `gorm:"primaryKey;autoIncrement:false"`
	Addr           string `gorm:"type:varchar(42);primaryKey;not null"`
	Name           string
	Symbol         string
	Decimals       *uint8
	Creator        *string    `gorm:"type:varchar(42)"`
	CreatedAt      *time.Time `gorm:"type:timestamptz;autoCreateTime:false"`
	TotalSupply    *string    `gorm:"type:numeric(78,0)"`
	RefreshEnabled bool       `gorm:"not null;default:false"`
}

func (TokenInfo) TableName() string {
	return "tokens"
}

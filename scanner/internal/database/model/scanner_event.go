package model

import (
	"encoding/json"
	"time"
)

type ScannerEvent struct {
	EventID     string          `gorm:"primaryKey" json:"event_id"`
	EventType   string          `gorm:"not null" json:"event_type"`
	ChainID     int64           `gorm:"not null" json:"chain_id"`
	BlockNumber uint64          `gorm:"not null" json:"block_number"`
	BlockTime   time.Time       `gorm:"type:timestamptz;not null" json:"block_time"`
	ExpiredAt   time.Time       `gorm:"type:timestamptz;not null;index" json:"expired_at"`
	Data        json.RawMessage `gorm:"type:jsonb;serializer:json;not null" json:"data"`
}

package model

type ScannerCursor struct {
	ChainID     int64  `gorm:"primaryKey;autoIncrement:false"`
	BlockNumber uint64 `gorm:"not null"`
}

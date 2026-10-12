package domain

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type TokenInfo struct {
	Name      string
	Symbol    string
	Decimals  uint8
	Creator   common.Address
	CreatedAt time.Time
}

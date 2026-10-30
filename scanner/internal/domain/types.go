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

type UniV3PoolCreate struct {
	Token0      common.Address
	Token1      common.Address
	Fee         uint32
	TickSpacing int32
	Pool        common.Address
}

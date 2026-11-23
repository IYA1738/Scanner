package domain

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type TokenInfo struct {
	Name      *string
	Symbol    *string
	Decimals  *uint8
	Creator   common.Address
	CreatedAt time.Time

	TotalSupply    *big.Int
	RefreshEnabled bool
}

type PoolTokenInfo struct {
	Name     *string `json:",omitempty"`
	Symbol   *string `json:",omitempty"`
	Decimals *uint8  `json:",omitempty"`
}

type UniV3PoolCreate struct {
	Token0      common.Address
	Token1      common.Address
	Fee         uint32
	TickSpacing int32
	Pool        common.Address
	Token0Info  *PoolTokenInfo `json:",omitempty"`
	Token1Info  *PoolTokenInfo `json:",omitempty"`
}

package database

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"example.com/scanner_demo/scanner/internal/database/model"
	"github.com/ethereum/go-ethereum/common"
	"gorm.io/gorm"
)

func (d *Database) CreateTokenInfo(ctx context.Context, token *model.TokenInfo) error {
	record, err := normalizeTokenInfo(token)
	if err != nil {
		return fmt.Errorf("create token info: %w", err)
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := d.db.WithContext(writeCtx).Create(&record).Error; err != nil {
		return fmt.Errorf("create token info chain_id=%d addr=%s: %w", record.ChainID, record.Addr, err)
	}
	return nil
}

func (d *Database) GetTokenInfo(ctx context.Context, chainID int64, addr string) (*model.TokenInfo, error) {
	addr, err := normalizeTokenAddr(addr)
	if err != nil {
		return nil, fmt.Errorf("get token info: %w", err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var token model.TokenInfo
	err = d.db.WithContext(queryCtx).
		Where("chain_id = ? AND addr = ?", chainID, addr).
		First(&token).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get token info chain_id=%d addr=%s: %w", chainID, addr, err)
	}
	return &token, nil
}

func (d *Database) GetTokenInfos(ctx context.Context, chainID int64, addrs []string) ([]model.TokenInfo, error) {
	if len(addrs) == 0 {
		return []model.TokenInfo{}, nil
	}
	normalized := make([]string, len(addrs))
	for i, addr := range addrs {
		var err error
		normalized[i], err = normalizeTokenAddr(addr)
		if err != nil {
			return nil, fmt.Errorf("get token infos: %w", err)
		}
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var tokens []model.TokenInfo
	err := d.db.WithContext(queryCtx).
		Where("chain_id = ? AND addr IN ?", chainID, normalized).
		Find(&tokens).Error
	if err != nil {
		return nil, fmt.Errorf("get token infos chain_id=%d: %w", chainID, err)
	}
	return tokens, nil
}

func (d *Database) UpdateTokenInfo(ctx context.Context, token *model.TokenInfo) error {
	record, err := normalizeTokenInfo(token)
	if err != nil {
		return fmt.Errorf("update token info: %w", err)
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result := d.db.WithContext(writeCtx).
		Model(&model.TokenInfo{}).
		Where("chain_id = ? AND addr = ?", record.ChainID, record.Addr).
		Select("name", "symbol", "decimals", "creator", "created_at", "total_supply", "refresh_enabled").
		Updates(&record)
	err = result.Error
	if err == nil && result.RowsAffected == 0 {
		err = gorm.ErrRecordNotFound
	}
	if err != nil {
		return fmt.Errorf("update token info chain_id=%d addr=%s: %w", record.ChainID, record.Addr, err)
	}
	return nil
}

func (d *Database) DeleteTokenInfo(ctx context.Context, chainID int64, addr string) error {
	addr, err := normalizeTokenAddr(addr)
	if err != nil {
		return fmt.Errorf("delete token info: %w", err)
	}
	deleteCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = d.db.WithContext(deleteCtx).
		Where("chain_id = ? AND addr = ?", chainID, addr).
		Delete(&model.TokenInfo{}).Error
	if err != nil {
		return fmt.Errorf("delete token info chain_id=%d addr=%s: %w", chainID, addr, err)
	}
	return nil
}

func normalizeTokenAddr(addr string) (string, error) {
	if !common.IsHexAddress(addr) {
		return "", fmt.Errorf("invalid token address %q", addr)
	}
	return common.HexToAddress(addr).Hex(), nil
}

func normalizeTokenInfo(token *model.TokenInfo) (model.TokenInfo, error) {
	if token == nil {
		return model.TokenInfo{}, errors.New("nil token info")
	}
	record := *token
	var err error
	record.Addr, err = normalizeTokenAddr(record.Addr)
	if err != nil {
		return model.TokenInfo{}, err
	}
	if record.Creator != nil {
		creator, err := normalizeTokenAddr(*record.Creator)
		if err != nil {
			return model.TokenInfo{}, fmt.Errorf("invalid creator: %w", err)
		}
		record.Creator = &creator
	}
	if record.TotalSupply != nil {
		supply, ok := new(big.Int).SetString(*record.TotalSupply, 10)
		if !ok || supply.Sign() < 0 || supply.BitLen() > 256 {
			return model.TokenInfo{}, errors.New("total supply must be a uint256 decimal integer")
		}
		value := supply.String()
		record.TotalSupply = &value
	}
	return record, nil
}

func (d *Database) GetTokenInfoByChain(
	ctx context.Context,
	chainID int64,
) ([]model.TokenInfo, error) {
	quertCtx, cancel := context.WithTimeout(
		ctx,
		10*time.Second,
	)
	defer cancel()

	var tokens []model.TokenInfo
	err := d.db.WithContext(quertCtx).
		Where("chain_id = ?", chainID).
		Find(&tokens).Error

	if err != nil {
		return nil, fmt.Errorf(
			"get token infos by chain chain_id=%d: %w",
			chainID, err,
		)
	}
	return tokens, nil
}

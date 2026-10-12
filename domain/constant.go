package domain

import "github.com/ethereum/go-ethereum/common"

var (
	// ========== Uniswap V3 ==========

	V3PoolCreatedTopic = common.HexToHash(
		"0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118",
	)

	V3MintTopic = common.HexToHash(
		"0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
	)

	V3SwapTopic = common.HexToHash(
		"0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
	)

	// ========== Uniswap V4 ==========

	V4InitializeTopic = common.HexToHash(
		"0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438",
	)

	V4ModifyLiquidityTopic = common.HexToHash(
		"0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec",
	)

	V4SwapTopic = common.HexToHash(
		"0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
	)

	ERC20TransferTopic = common.HexToHash(
		"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
	)
)

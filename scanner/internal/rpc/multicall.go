package rpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"example.com/scanner_demo/scanner/internal/domain"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

type MulticallCall struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type MulticallResult struct {
	Success    bool
	ReturnData []byte
}

type TokenMetadata struct {
	Name        *string
	Symbol      *string
	Decimals    *uint8
	TotalSupply *big.Int
}

const multicall3ABIJSON = `[
  {
    "type": "function",
    "name": "aggregate3",
    "stateMutability": "payable",
    "inputs": [{
      "name": "calls",
      "type": "tuple[]",
      "components": [
        {"name": "target", "type": "address"},
        {"name": "allowFailure", "type": "bool"},
        {"name": "callData", "type": "bytes"}
      ]
    }],
    "outputs": [{
      "name": "returnData",
      "type": "tuple[]",
      "components": [
        {"name": "success", "type": "bool"},
        {"name": "returnData", "type": "bytes"}
      ]
    }]
  }
]`

const tokenMetadataABIJSON = `[
  {"type":"function","name":"name","stateMutability":"view","inputs":[],"outputs":[{"type":"string"}]},
  {"type":"function","name":"symbol","stateMutability":"view","inputs":[],"outputs":[{"type":"string"}]},
  {"type":"function","name":"decimals","stateMutability":"view","inputs":[],"outputs":[{"type":"uint8"}]},
  {"type":"function","name":"totalSupply","stateMutability":"view","inputs":[],"outputs":[{"type":"uint256"}]}
]`

var tokenMetadataABI, tokenMetadataABIErr = abi.JSON(
	strings.NewReader(tokenMetadataABIJSON),
)

var multicall3ABI, multicall3ABIErr = abi.JSON(
	strings.NewReader(multicall3ABIJSON),
)

func (r *RpcClient) Multicall(
	ctx context.Context,
	calls []MulticallCall,
	blockNumber *big.Int,
) ([]MulticallResult, error) {
	if len(calls) == 0 {
		return []MulticallResult{}, nil
	}

	if multicall3ABIErr != nil {
		return nil, fmt.Errorf("parse multicall ABI: %w", multicall3ABIErr)
	}

	input, err := multicall3ABI.Pack("aggregate3", calls)
	if err != nil {
		return nil, fmt.Errorf("pack multicall: %w", err)
	}

	var raw []byte
	var callErr error

	err = r.Execute(ctx, func(reqCtx context.Context, c *ethclient.Client) error {
		raw, callErr = c.CallContract(
			reqCtx,
			ethereum.CallMsg{
				To:   &domain.Multicall3Addr,
				Data: input,
			}, blockNumber)
		var rpcErr gethrpc.Error
		if errors.As(callErr, &rpcErr) && rpcErr.ErrorCode() == 3 {
			return nil
		}
		return callErr
	})

	if err != nil {
		return nil, fmt.Errorf("call multicall: %w", err)
	}

	if callErr != nil {
		return nil, fmt.Errorf("multicall reverted: %w", callErr)
	}

	var results []MulticallResult
	if err := multicall3ABI.UnpackIntoInterface(
		&results, "aggregate3", raw,
	); err != nil {
		return nil, fmt.Errorf("decode multicall: %w", err)
	}

	if len(results) != len(calls) {
		return nil, fmt.Errorf(
			"multicall result count %d != calls %d",
			len(results), len(calls),
		)
	}

	return results, nil
}

func (r *RpcClient) FetchTokenMetadata(
	ctx context.Context,
	tokens []common.Address,
	blockNumber *big.Int,
) (map[common.Address]TokenMetadata, error) {
	if len(tokens) == 0 {
		return map[common.Address]TokenMetadata{}, nil
	}
	if tokenMetadataABIErr != nil {
		return nil, fmt.Errorf("parse token metadata ABI: %w", tokenMetadataABIErr)
	}
	methods := []string{"name", "symbol", "decimals", "totalSupply"}
	calls := make([]MulticallCall, 0, len(tokens)*len(methods))

	for _, token := range tokens {
		for _, method := range methods {
			data, err := tokenMetadataABI.Pack(method)
			if err != nil {
				return nil, fmt.Errorf("pack token %s: %w", method, err)
			}

			calls = append(calls, MulticallCall{
				Target:       token,
				AllowFailure: true,
				CallData:     data,
			})
		}
	}

	batchSize := 25 * len(methods)
	batchCount := (len(calls) + batchSize - 1) / batchSize
	workerCount := min(len(r.Client), batchCount)

	if workerCount == 0 {
		return nil, errors.New("no HTTP RPC clients")
	}

	results := make([]MulticallResult, len(calls))
	batchErrors := make([]error, batchCount)
	jobs := make(chan int)

	var wg sync.WaitGroup
	wg.Add(workerCount)

	for worker := 0; worker < workerCount; worker++ {
		go func() {
			defer wg.Done()

			for i := range jobs {
				start := i * batchSize
				end := min(start+batchSize, len(calls))

				batch, err := r.Multicall(
					ctx,
					calls[start:end],
					blockNumber,
				)

				if err != nil {
					batchErrors[i] = fmt.Errorf("metadata batch %d: %w", i, err)
					continue
				}

				copy(results[start:end], batch)
			}
		}()
	}

	for i := 0; i < batchCount; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	metadata := make(map[common.Address]TokenMetadata, len(tokens))

	for i, token := range tokens {
		info := metadata[token]
		for j, method := range methods {
			result := results[i*len(methods)+j]
			if !result.Success {
				continue
			}
			values, err := tokenMetadataABI.Unpack(method, result.ReturnData)
			if err != nil {
				batchErrors = append(batchErrors,
					fmt.Errorf("decode token %s %s: %w", token.Hex(), method, err),
				)
				continue
			}
			if len(values) != 1 {
				batchErrors = append(batchErrors,
					fmt.Errorf("decode token %s %s: expected one value", token.Hex(), method),
				)
				continue
			}

			switch method {
			case "name":
				value := values[0].(string)
				info.Name = &value
			case "symbol":
				value := values[0].(string)
				info.Symbol = &value
			case "decimals":
				value := values[0].(uint8)
				info.Decimals = &value
			case "totalSupply":
				info.TotalSupply = values[0].(*big.Int)
			}
		}
		metadata[token] = info
	}
	return metadata, errors.Join(batchErrors...)
}

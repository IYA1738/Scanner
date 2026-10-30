package rpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http/httptrace"
	"sync"
	"time"

	"example.com/scanner_demo/scanner/internal/config"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

type RpcClient struct {
	Cfg      config.RPCConfig
	Client   []*ethclient.Client
	WSClient []*ethclient.Client

	httpStates map[int]RpcState
	wsStates   map[int]RpcState

	httpQueue chan int
	wsQueue   chan int

	mu   sync.Mutex
	stop chan struct{}
}

type RpcState struct {
	Failures     int
	BackoffUntil time.Time
}

type Protocol uint8

const (
	ProtocolHTTP Protocol = iota
	ProtocolWS
)

const (
	maxFailures = 2
	backoffTime = 10 * time.Minute
)

func NewRPCClient(ctx context.Context, cfg config.RPCConfig) (_ *RpcClient, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(cfg.URL) == 0 {
		return nil, errors.New("HTTP RPC URLs are empty")
	}

	if len(cfg.WS_URL) == 0 {
		return nil, errors.New("WS RPC URLs are empty")
	}

	var httpClients []*ethclient.Client
	var wsClients []*ethclient.Client
	var errs []error
	defer func() {
		if retErr == nil {
			return
		}
		for _, c := range httpClients {
			c.Close()
		}
		for _, c := range wsClients {
			c.Close()
		}
	}()

	for _, url := range cfg.URL {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		c, err := ethclient.DialContext(ctx, url)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}

			errs = append(errs, fmt.Errorf("HTTP RPC %s: %w", url, err))
			continue
		}

		httpClients = append(httpClients, c)
	}

	if len(httpClients) == 0 {
		return nil, fmt.Errorf("all HTTP RPC nodes failed: %w", errors.Join(errs...))
	}
	errs = nil

	for _, url := range cfg.WS_URL {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// 当前 WebSocket 依赖在握手读取期间不会主动监听取消。
		// 仅在建立连接期间注册关闭回调，成功后解除。
		var stopClose func() bool
		dialCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				if info.Conn != nil {
					stopClose = context.AfterFunc(ctx, func() {
						info.Conn.Close()
					})
				}
			},
		})
		w, err := ethclient.DialContext(dialCtx, url)
		if stopClose != nil {
			stopClose()
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			errs = append(errs, fmt.Errorf("WS RPC %s: %w", url, err))
			continue
		}

		wsClients = append(wsClients, w)
	}

	if len(wsClients) == 0 {
		return nil, fmt.Errorf("all WS RPC nodes failed: %w", errors.Join(errs...))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r := &RpcClient{
		Cfg:        cfg,
		Client:     httpClients,
		WSClient:   wsClients,
		httpStates: make(map[int]RpcState, len(httpClients)),
		wsStates:   make(map[int]RpcState, len(wsClients)),
		httpQueue:  make(chan int, len(httpClients)),
		wsQueue:    make(chan int, len(wsClients)),
		stop:       make(chan struct{}),
	}

	for i := range httpClients {
		r.httpStates[i] = RpcState{}
		r.httpQueue <- i
	}

	for i := range wsClients {
		r.wsStates[i] = RpcState{}
		r.wsQueue <- i
	}

	return r, nil
}

func (r *RpcClient) Close() error {
	close(r.stop)
	for _, c := range r.Client {
		c.Close()
	}

	for _, c := range r.WSClient {
		c.Close()
	}

	return nil
}

func (r *RpcClient) Acquire(
	ctx context.Context,
	protocol Protocol,
) (int, error) {

	var queue <-chan int

	switch protocol {
	case ProtocolHTTP:
		queue = r.httpQueue
	case ProtocolWS:
		queue = r.wsQueue
	default:
		return -1, fmt.Errorf("unknown RPC protocol: %d", protocol)
	}

	select {
	case <-r.stop:
		return -1, errors.New("RPC client closed")
	case <-ctx.Done():
		return -1, ctx.Err()
	case index := <-queue:
		return index, nil
	}
}

func (r *RpcClient) Release(protocol Protocol, index int, requestErr error) error {
	var states map[int]RpcState
	var queue chan int

	switch protocol {
	case ProtocolHTTP:
		states = r.httpStates
		queue = r.httpQueue
	case ProtocolWS:
		states = r.wsStates
		queue = r.wsQueue
	default:
		return fmt.Errorf("unknown RPC protocol: %d", protocol)
	}

	r.mu.Lock()

	state, ok := states[index]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("unknown RPC node index: %d", index)
	}

	if requestErr != nil && !errors.Is(requestErr, context.Canceled) {
		state.Failures++

		if state.Failures >= maxFailures {
			state.BackoffUntil = time.Now().Add(backoffTime)
			state.Failures = 0
		}
	} else if requestErr == nil {
		state.Failures = 0
	}

	states[index] = state

	shouldBackoff := time.Now().Before(state.BackoffUntil)

	r.mu.Unlock()

	if shouldBackoff {
		go r.recover(protocol, index, state.BackoffUntil)
		return nil
	}

	select {
	case queue <- index:
	case <-r.stop:
		return nil
	}
	return nil
}

func (r *RpcClient) recover(protocol Protocol, index int, until time.Time) {
	var queue chan int

	switch protocol {
	case ProtocolHTTP:
		queue = r.httpQueue
	case ProtocolWS:
		queue = r.wsQueue
	default:
		return
	}

	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-r.stop:
		return
	}

	select {
	case queue <- index:
	case <-r.stop:
		return
	}
}

func (r *RpcClient) Execute(
	ctx context.Context,
	fn func(context.Context, *ethclient.Client) error,
) error {
	attempts := r.Cfg.RetryCount + 1
	if attempts < 1 {
		attempts = 1
	}

	timeout := time.Duration(r.Cfg.RPCTimeout) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	interval := time.Duration(r.Cfg.RetryInterval) * time.Second

	var lastErr error

	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		index, err := r.Acquire(ctx, ProtocolHTTP)

		if err != nil {
			lastErr = err
		} else {
			reqCtx, cancel := context.WithTimeout(ctx, timeout)

			lastErr = fn(reqCtx, r.Client[index])

			cancel()

			if releaseErr := r.Release(ProtocolHTTP, index, lastErr); releaseErr != nil {
				return releaseErr
			}

			if lastErr == nil {
				return nil
			}
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if i == attempts-1 {
			break
		}

		timer := time.NewTimer(interval)

		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-r.stop:
			timer.Stop()
			return errors.New("RPC client closed")
		}
	}

	return fmt.Errorf(
		"HTTP RPC failed after %d attempts: %w",
		attempts, lastErr,
	)
}

func (r *RpcClient) BlockNumber(ctx context.Context) (uint64, error) {
	var blockNumber uint64

	err := r.Execute(ctx, func(reqCtx context.Context, client *ethclient.Client) error {
		number, err := client.BlockNumber(reqCtx)
		if err != nil {
			return err
		}
		blockNumber = number
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("get latest block number: %w", err)
	}
	return blockNumber, nil
}

func (r *RpcClient) BlockByNumber(
	ctx context.Context,
	blockNumber uint64,
) (*types.Block, error) {
	var block *types.Block

	err := r.Execute(ctx, func(reqCtx context.Context, client *ethclient.Client) error {
		result, err := client.BlockByNumber(
			reqCtx,
			new(big.Int).SetUint64(blockNumber),
		)
		if err != nil {
			return err
		}

		block = result
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("get block number=%d: %w", blockNumber, err)
	}

	return block, nil
}

func (r *RpcClient) BlockReceipts(ctx context.Context, hash common.Hash) (types.Receipts, error) {
	var receipts types.Receipts

	err := r.Execute(ctx, func(reqCtx context.Context, c *ethclient.Client) error {
		res, err := c.BlockReceipts(reqCtx, gethrpc.BlockNumberOrHashWithHash(hash, false))
		if err != nil {
			return err
		}
		receipts = res
		return nil
	})

	if err != nil {
		return nil, err
	}
	return receipts, nil
}

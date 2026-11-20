package scanner

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"sync"
	"time"

	"example.com/scanner_demo/scanner/internal/config"
	"example.com/scanner_demo/scanner/internal/database"
	"example.com/scanner_demo/scanner/internal/database/model"
	"example.com/scanner_demo/scanner/internal/domain"
	"example.com/scanner_demo/scanner/internal/events"
	"example.com/scanner_demo/scanner/internal/notify/tg"
	"example.com/scanner_demo/scanner/internal/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// scanner需要把block和receipts拿到作为fetch结果 而不是只拿block
type blockData struct {
	block    *rpc.ScanBlock
	receipts types.Receipts
}

type job struct {
	fromBlock uint64
	toBlock   uint64
}

type jobResult struct {
	task   job
	blocks []blockData
	err    error
}

type Scanner struct {
	ctx            context.Context
	ChainID        int64
	Client         *rpc.RpcClient
	DB             *database.Database
	publisher      *tg.TGPublisher
	tokenCache     map[common.Address]domain.TokenInfo
	cfg            config.ScannerConfig
	nextBlock      uint64
	redisPublisher *events.EventPublisher
}

func NewScanner(
	ctx context.Context,
	cfg config.ScannerConfig,
	chainID int64,
	client *rpc.RpcClient,
	db *database.Database,
	publisher *tg.TGPublisher,
	redisPublisher *events.EventPublisher,
) (*Scanner, error) {
	s := &Scanner{
		ctx:            ctx,
		cfg:            cfg,
		ChainID:        chainID,
		Client:         client,
		DB:             db,
		publisher:      publisher,
		tokenCache:     make(map[common.Address]domain.TokenInfo),
		redisPublisher: redisPublisher,
	}

	tokens, err := db.GetTokenInfoByChain(
		ctx,
		chainID,
	)
	if err != nil {
		return nil, fmt.Errorf("load token cache chain_id=%d: %w", chainID, err)
	}
	for i := range tokens {
		row := &tokens[i]

		info := domain.TokenInfo{
			Decimals:       row.Decimals,
			RefreshEnabled: row.RefreshEnabled,
		}

		if row.Name != "" {
			info.Name = &row.Name
		}

		if row.Symbol != "" {
			info.Symbol = &row.Symbol
		}

		if row.Creator != nil {
			info.Creator = common.HexToAddress(*row.Creator)
		}

		if row.CreatedAt != nil {
			info.CreatedAt = *row.CreatedAt
		}

		if row.TotalSupply != nil {
			supply, ok := new(big.Int).SetString(*row.TotalSupply, 10)
			if ok && supply.Sign() >= 0 && supply.BitLen() <= 256 {
				info.TotalSupply = supply
			} else {
				slog.Warn("token totalSupply 无效", "token", row.Addr)
			}
		}
		s.tokenCache[common.HexToAddress(row.Addr)] = info
	}
	return s, nil
}

// 线性启动 k为斜率 未来要换并发数策略直接把这个方法改成其他函数就行
func (s *Scanner) activeWorkers(blockNum uint64, k uint64) int {
	res := blockNum / k
	if res <= 0 {
		return 1
	}
	if res >= uint64(s.cfg.WorkersNum) {
		return s.cfg.WorkersNum
	}
	return int(res)
}

func (s *Scanner) Run(fromHead bool) error {
	s.nextBlock = 0
	if !fromHead {
		cursor, err := s.DB.GetScannerCursor(s.ctx, s.ChainID)
		if err != nil {
			return err
		}
		if cursor != nil {
			s.nextBlock = cursor.BlockNumber + 1
		}
	}

	if s.nextBlock == 0 {
		head, err := s.Client.BlockNumber(s.ctx)
		if err != nil {
			return err
		}
		s.nextBlock = head
	}
	slog.Info("扫链起点", "chain_id", s.ChainID, "block", s.nextBlock, "from_head", fromHead)

	// 扫链主循环
	for {
		if err := s.ctx.Err(); err != nil {
			return err
		}

		head, err := s.Client.BlockNumber(s.ctx)
		if err != nil {
			return err
		}

		if s.nextBlock > head {
			select {
			case <-time.After(time.Duration(s.cfg.BlockTimeMS) * time.Millisecond):
				continue
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		}

		pending := head - s.nextBlock + 1
		workers := s.activeWorkers(pending, 10) // 至少落后10块才并发

		batchCtx, cancel := context.WithCancelCause(s.ctx)

		jobs := make(chan job, workers*2)        // 任务投递队列
		window := make(chan struct{}, workers*2) // windows是滑动窗口 用来控制上游jobs投递速度来间接防止下游有序提交卡住时不断处理任务导致内存无限增长
		results := make(chan jobResult, workers*2)
		fullJob := job{
			fromBlock: s.nextBlock,
			toBlock:   head,
		}
		plannerDone := make(chan struct{})
		go func() {
			defer close(plannerDone)
			s.PlanJob(batchCtx, fullJob, jobs, window)
		}()

		var wg sync.WaitGroup
		wg.Add(workers)

		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				if err := s.Worker(batchCtx, jobs, results); err != nil {
					cancel(err)
				}
			}()
		}
		workersDone := make(chan struct{})
		go func() {
			wg.Wait()
			close(results)
			close(workersDone)
		}()

		expected := fullJob.fromBlock
		pendingResults := make(map[uint64]jobResult)
		var batchErr error

	collect:
		for {
			select {
			case <-batchCtx.Done():
				batchErr = context.Cause(batchCtx)
				break collect
			case result, ok := <-results:
				if !ok {
					if err := context.Cause(batchCtx); err != nil {
						batchErr = err
					} else if expected != fullJob.toBlock+1 || len(pendingResults) != 0 {
						batchErr = fmt.Errorf(
							"incomplete batch: expected block %d, head %d",
							expected, fullJob.toBlock,
						)
					}
					break collect
				}
				if result.err != nil {
					batchErr = result.err
					break collect
				}
				pendingResults[result.task.fromBlock] = result
				for {
					ready, ok := pendingResults[expected]
					if !ok {
						break // 前面的还没完成 继续接收结果
					}

					if ready.task.fromBlock > ready.task.toBlock ||
						uint64(len(ready.blocks)) != ready.task.toBlock-ready.task.fromBlock+1 {
						batchErr = fmt.Errorf(
							"incomplete result for job %d-%d",
							ready.task.fromBlock, ready.task.toBlock,
						)
						break collect
					}

					// 组内升序处理区块
					for i, data := range ready.blocks {
						if err := context.Cause(batchCtx); err != nil {
							batchErr = err
							break collect
						}
						height := ready.task.fromBlock + uint64(i)
						if data.block == nil || data.block.Number != height {
							batchErr = fmt.Errorf(
								"unordered or missing block at height %d", height,
							)
							break collect
						}
						// 处理单个区块和收据 下游业务在这里触发
						if err := s.ProcessBlock(batchCtx, data.block, data.receipts); err != nil {
							batchErr = fmt.Errorf("process block %d: %w", height, err)
							break collect
						}

						if err := context.Cause(batchCtx); err != nil {
							batchErr = err
							break collect
						}
						if err := s.DB.SaveScannerCursor(batchCtx, s.ChainID, height); err != nil {
							batchErr = err
							break collect
						}
						s.nextBlock = height + 1
					}
					delete(pendingResults, expected)
					expected = ready.task.toBlock + 1
					<-window
				}
			}
		}
		// 接收结束后，取消并回收本轮所有调度 Goroutine。
		cancel(batchErr)
		<-plannerDone
		<-workersDone

		if batchErr != nil {
			return context.Cause(batchCtx)
		}

	}
}

func (s *Scanner) ProcessBlock(ctx context.Context, block *rpc.ScanBlock, receipts types.Receipts) error {
	var pendingEvents []pendingEvent
	tokenSet := make(map[common.Address]struct{})
	if err := context.Cause(ctx); err != nil {
		return err
	}
	var logs []*types.Log

	for _, receipt := range receipts {
		logs = append(logs, receipt.Logs...)
	}

	for _, log := range logs {
		if len(log.Topics) == 0 {
			continue
		}

		switch log.Topics[0] {
		case domain.V3PoolCreatedTopic:
			if log.Address != s.cfg.Constants.UniV3Factory ||
				len(log.Data) != 64 ||
				len(log.Topics) != 4 {
				continue
			}

			uniV3PoolCreateEvent, err := s.ParseUniV3PoolCreate(log)
			if err != nil {
				continue
			}

			pendingEvents = append(pendingEvents,
				pendingEvent{
					logIndex: log.Index,
					data:     uniV3PoolCreateEvent,
				})

			tokenSet[uniV3PoolCreateEvent.Token0] = struct{}{}
			tokenSet[uniV3PoolCreateEvent.Token1] = struct{}{}

		case domain.V4InitializeTopic:
			if log.Address != s.cfg.Constants.UniV4PoolManager ||
				len(log.Data) != 160 ||
				len(log.Topics) != 4 {
				continue
			}

			// TODO: 解析 V4 Initialize
		case domain.V3MintTopic:
			if len(log.Data) != 128 ||
				len(log.Topics) != 4 {
				continue
			}

			// TODO: 解析 V3 Mint

		case domain.V4ModifyLiquidityTopic:
			if log.Address != s.cfg.Constants.UniV4PoolManager ||
				len(log.Data) != 128 ||
				len(log.Topics) != 3 {
				continue
			}

			// TODO: 解析 V4 ModifyLiquidity

		case domain.ERC20TransferTopic:
			if len(log.Data) != 32 ||
				len(log.Topics) != 3 {
				continue
			}

			// TODO: 解析 ERC20 Transfer

		case domain.V3SwapTopic:
			if len(log.Data) != 160 ||
				len(log.Topics) != 3 {
				continue
			}

			// TODO: 解析 V3 Swap

		case domain.V4SwapTopic:
			if log.Address != s.cfg.Constants.UniV4PoolManager ||
				len(log.Data) != 192 ||
				len(log.Topics) != 3 {
				continue
			}

			// TODO: 解析 V4 Swap
		}
	}

	for addr := range tokenSet {
		info := s.tokenCache[addr]
		if info.Name != nil &&
			info.Symbol != nil &&
			info.Decimals != nil &&
			info.TotalSupply != nil {
			delete(tokenSet, addr)
		}
	}

	tokens := make([]common.Address, 0, len(tokenSet))
	for addr := range tokenSet {
		tokens = append(tokens, addr)
	}
	metadata, err := s.Client.FetchTokenMetadata(
		ctx,
		tokens,
		new(big.Int).SetUint64(block.Number),
	)
	if err != nil {
		slog.Warn("token metadata 查询有失败",
			"chain_id", s.ChainID,
			"block", block.Number,
			"error", err,
		)
	}
	for addr, meta := range metadata {
		info := s.tokenCache[addr]
		if info.Name == nil && meta.Name != nil {
			info.Name = meta.Name
		}
		if info.Symbol == nil && meta.Symbol != nil {
			info.Symbol = meta.Symbol
		}
		if info.Decimals == nil && meta.Decimals != nil {
			info.Decimals = meta.Decimals
		}
		if info.TotalSupply == nil && meta.TotalSupply != nil {
			info.TotalSupply = meta.TotalSupply
		}

		s.tokenCache[addr] = info
	}

	if err := context.Cause(ctx); err != nil {
		return err
	}

	for _, item := range pendingEvents {
		s.dispatchEvent(ctx, block, item)
	}
	return nil
}

func (s *Scanner) Worker(
	ctx context.Context,
	jobs <-chan job,
	results chan<- jobResult,
) error {
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)

		case task, ok := <-jobs:
			if !ok {
				return nil
			}
			blocks, err := s.FetchBlocks(ctx, task)
			if err != nil {
				err = fmt.Errorf(
					"fetch job %d-%d: %w",
					task.fromBlock, task.toBlock, err,
				)
			}
			select {
			case results <- jobResult{
				task:   task,
				blocks: blocks,
				err:    err,
			}:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
			if err != nil {
				return err
			}
		}
	}
}

func (s *Scanner) PlanJob(ctx context.Context, fullJob job, jobs chan<- job, window chan struct{}) {
	defer close(jobs)

	groupSize := uint64(s.cfg.GroupSize)

	for from := fullJob.fromBlock; from <= fullJob.toBlock; {
		to := fullJob.toBlock

		if groupSize-1 < to-from {
			to = from + groupSize - 1
		}

		// 申请窗口名额
		select {
		case window <- struct{}{}:
		case <-ctx.Done():
			return
		}

		// 投递 Job
		select {
		case jobs <- job{fromBlock: from, toBlock: to}:
		case <-ctx.Done():
			<-window // 未投递成功，归还名额
			return
		}

		if to == fullJob.toBlock {
			return
		}

		from = to + 1
	}
}

func (s *Scanner) FetchBlocks(ctx context.Context, task job) ([]blockData, error) {
	if task.fromBlock > task.toBlock {
		return nil, fmt.Errorf("invalid block range")
	}

	blocks := make([]blockData, 0)

	for i := task.fromBlock; ; i++ {
		block, err := s.Client.BlockByNumber(ctx, i)
		if err != nil {
			return nil, fmt.Errorf("fetch block %d: %w", i, err)
		}

		if block == nil || block.Number != i {
			return nil, fmt.Errorf("invalid block at height %d", i)
		}

		receipts, err := s.Client.BlockReceipts(ctx, block.Hash)
		if err != nil {
			return nil, fmt.Errorf("fetch receipts for block %d: %w", i, err)
		}

		txs := block.Transactions

		if len(txs) != len(receipts) {
			return nil, fmt.Errorf(
				"block %d: transactions %d != receipts %d",
				i, len(txs), len(receipts),
			)
		}

		blocks = append(blocks, blockData{
			block:    block,
			receipts: receipts,
		})

		if i == task.toBlock {
			break
		}
	}
	return blocks, nil
}

type pendingEvent struct {
	logIndex uint
	data     any
}

func (s *Scanner) dispatchEvent(
	ctx context.Context,
	block *rpc.ScanBlock,
	item pendingEvent,
) {
	var eventType string
	var tgEvent tg.TGEvent
	var sendTG bool

	switch item.data.(type) {
	case *domain.UniV3PoolCreate:
		eventType = "univ3.pool_created"
		tgEvent = tg.TGEventNewV3Pool
		sendTG = true
	default:
		slog.Warn("未支持的事件类型",
			"type", fmt.Sprintf("%T", item.data))
		return
	}
	if sendTG {
		if err := s.publisher.PublishToChat(
			ctx, "DM", tgEvent, item.data,
		); err != nil {
			slog.Error("投递事件通知失败",
				"chain_id", s.ChainID, "error", err)
		}
	}
	event, err := s.newEvent(
		block,
		item.logIndex,
		eventType,
		item.data,
	)
	if err != nil {
		slog.Error("创建事件失败失败",
			"chain_id", s.ChainID, "error", err)
		return
	}
	if err := s.redisPublisher.Publish(
		ctx,
		event,
	); err != nil {
		slog.Error("redis推送失败",
			"chain_id", s.ChainID, "error", err)
	}
	if err := s.DB.SaveScannerEvent(
		ctx,
		event,
	); err != nil {
		slog.Error("事件落库失败",
			"chain_id", s.ChainID, "error", err)
	}
}

func (s *Scanner) newEvent(
	block *rpc.ScanBlock,
	logIndex uint,
	eventType string,
	data any,
) (*model.ScannerEvent, error) {
	if block == nil || block.Timestamp == nil {
		return nil, fmt.Errorf("missing block timestamp for %s", eventType)
	}
	seconds := s.cfg.EventTTLSeconds[eventType]
	if seconds <= 0 || seconds > math.MaxInt64/int64(time.Second) {
		return nil, fmt.Errorf("invalid TTL seconds for %s: %d", eventType, seconds)
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal event data failed: %w", err)
	}

	blockTime := time.Unix(int64(*block.Timestamp), 0).UTC()

	return &model.ScannerEvent{
		EventID:     fmt.Sprintf("%d:%s:%d", s.ChainID, block.Hash.Hex(), logIndex),
		EventType:   eventType,
		ChainID:     s.ChainID,
		BlockNumber: block.Number,
		BlockTime:   blockTime,
		ExpiredAt:   blockTime.Add(time.Duration(seconds) * time.Second),
		Data:        payload,
	}, nil
}

func (s *Scanner) ParseUniV3PoolCreate(log *types.Log) (*domain.UniV3PoolCreate, error) {
	if log == nil {
		return nil, fmt.Errorf("log is nil")
	}
	fee := uint32(log.Topics[3][29])<<16 | uint32(log.Topics[3][30])<<8 | uint32(log.Topics[3][31])
	tickSpacing := int32(binary.BigEndian.Uint32(log.Data[28:32]))
	return &domain.UniV3PoolCreate{
		Token0:      common.Address(log.Topics[1][12:]),
		Token1:      common.Address(log.Topics[2][12:]),
		Fee:         fee,
		TickSpacing: tickSpacing,
		Pool:        common.Address(log.Data[44:64]),
	}, nil
}

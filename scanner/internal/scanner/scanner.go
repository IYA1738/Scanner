package scanner

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"example.com/scanner_demo/scanner/internal/config"
	"example.com/scanner_demo/scanner/internal/database"
	"example.com/scanner_demo/scanner/internal/domain"
	"example.com/scanner_demo/scanner/internal/notify/tg"
	"example.com/scanner_demo/scanner/internal/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// scanner需要把block和receipts拿到作为fetch结果 而不是只拿block
type blockData struct {
	block    *types.Block
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
	ctx        context.Context
	ChainID    int64
	Client     *rpc.RpcClient
	DB         *database.Database
	publisher  *tg.TGPublisher
	tokenCache map[common.Address]domain.TokenInfo
	cfg        config.ScannerConfig
	nextBlock  uint64
}

func NewScanner(
	ctx context.Context,
	cfg config.ScannerConfig,
	chainID int64,
	client *rpc.RpcClient,
	db *database.Database,
	publisher *tg.TGPublisher,
) (*Scanner, error) {
	s := &Scanner{
		ctx:        ctx,
		cfg:        cfg,
		ChainID:    chainID,
		Client:     client,
		DB:         db,
		publisher:  publisher,
		tokenCache: make(map[common.Address]domain.TokenInfo),
	}

	// TODO: 从数据库加载本链的 Token Metadata 到缓存

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

func (s *Scanner) Run() error {
	cursor, err := s.DB.GetScannerCursor(s.ctx, s.ChainID)
	if err != nil {
		return err
	}

	if cursor == nil {
		head, err := s.Client.BlockNumber(s.ctx)
		if err != nil {
			return err
		}
		s.nextBlock = head
	} else {
		s.nextBlock = cursor.BlockNumber + 1
	}

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
						if data.block == nil || data.block.NumberU64() != height {
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

func (s *Scanner) ProcessBlock(ctx context.Context, block *types.Block, receipts types.Receipts) error {
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
			if err := s.publisher.PublishToChat(
				ctx, "DM", tg.TGEventNewV3Pool, uniV3PoolCreateEvent,
			); err != nil {
				slog.Error("投递新池通知失败", "chain_id", s.ChainID, "error", err)
			}

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

		if block == nil || block.NumberU64() != i {
			return nil, fmt.Errorf("invalid block at height %d", i)
		}

		receipts, err := s.Client.BlockReceipts(ctx, block.Hash())
		if err != nil {
			return nil, fmt.Errorf("fetch receipts for block %d: %w", i, err)
		}

		txs := block.Transactions()

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

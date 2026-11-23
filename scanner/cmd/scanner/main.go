package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/scanner_demo/scanner/internal/config"
	"example.com/scanner_demo/scanner/internal/database"
	"example.com/scanner_demo/scanner/internal/events"
	"example.com/scanner_demo/scanner/internal/logger"
	"example.com/scanner_demo/scanner/internal/notify/tg"
	"example.com/scanner_demo/scanner/internal/rpc"
	"example.com/scanner_demo/scanner/internal/scanner"
	"github.com/redis/go-redis/v9"
)

func main() {
	fromHead := flag.Bool("from-head", false, "启动时从当前链头开始，忽略已保存的游标")
	flag.Parse()

	if err := run(*fromHead); err != nil {
		slog.Error("application failed", "error", err)
		os.Exit(1)
	}
}

func run(fromHead bool) (runErr error) {
	cfg, err := config.LoadConfig("./config.json")
	if err != nil {
		return err
	}
	log, err := logger.NewLogger(cfg.Logger)
	if err != nil {
		return err
	}
	slog.SetDefault(log)
	signalCtx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer stop()
	manager := NewManager(signalCtx)
	defer func() {
		runErr = errors.Join(runErr, manager.Shutdown())
	}()

	dbInitCtx, cancelDBInit := context.WithTimeout(manager.ctx, 10*time.Second)
	db, err := database.NewDatabase(dbInitCtx, cfg.Database)
	cancelDBInit()
	if err != nil {
		if signalCtx.Err() != nil {
			return nil
		}
		return fmt.Errorf("初始化数据库: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:                  "127.0.0.1:6379",
		DB:                    0,
		ContextTimeoutEnabled: true,
	})

	manager.AddCleanup(redisClient.Close)
	manager.AddCleanup(db.Close)

	eventPublisher := events.NewEventPublisher(redisClient)

	kafkaPublisher, err := events.NewKafkaPublisher(
		[]string{"localhost:9092"},
		"chain.events.v1",
	)

	if err != nil {
		return fmt.Errorf("初始化 Kafka publisher: %w", err)
	}

	manager.AddCleanup(func() error {
		kafkaPublisher.Close()
		return nil
	})

	manager.Go(kafkaPublisher.Run)

	const chainRetryDelay = 5 * time.Second
	for chainName, chainCfg := range cfg.Chains {
		if signalCtx.Err() != nil {
			return nil
		}

		chainLog := log.With("chain", chainName, "chain_id", chainCfg.ChainID)
		publisher, err := tg.NewTGPublisher(cfg.TG, chainLog, chainCfg.ExplorerURL)
		if err != nil {
			return fmt.Errorf("%s 初始化 TG: %w", chainName, err)
		}
		manager.Go(func(ctx context.Context) {
			if err := publisher.ConsumeMessage(ctx); err != nil && ctx.Err() == nil {
				chainLog.Warn("TG 消费停止", "error", err)
			}
		})
		manager.Go(func(ctx context.Context) {
			var chainScanner *scanner.Scanner
			superviseChain(ctx, chainLog, chainRetryDelay, func(ctx context.Context) (attemptErr error) {
				initCtx, cancelInit := context.WithTimeout(ctx, 10*time.Second)
				client, err := rpc.NewRPCClient(initCtx, chainCfg.RPCCfg)
				cancelInit()
				if err != nil {
					return fmt.Errorf("初始化 RPC: %w", err)
				}
				defer func() {
					attemptErr = errors.Join(attemptErr, client.Close())
				}()

				if chainScanner == nil {
					chainScanner, err = scanner.NewScanner(
						ctx, chainCfg.ScannerCfg, int64(chainCfg.ChainID),
						client, db, publisher, eventPublisher, kafkaPublisher,
					)
					if err != nil {
						return fmt.Errorf("初始化 Scanner: %w", err)
					}
				} else {
					// 上一次 Run 已经返回并回收了自己的 worker。
					chainScanner.Client = client
				}

				chainLog.Info("链服务已启动")
				return chainScanner.Run(fromHead)
			})
		})
	}

	<-signalCtx.Done()
	log.Info("收到退出信号")
	return nil
}

func superviseChain(ctx context.Context, log *slog.Logger, retryDelay time.Duration, runOnce func(context.Context) error) {
	for ctx.Err() == nil {
		err := runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Warn("本链停止，等待重试", "error", err, "retry_in", retryDelay.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryDelay):
		}
	}
}

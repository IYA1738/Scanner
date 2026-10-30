package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/scanner_demo/scanner/internal/config"
	"example.com/scanner_demo/scanner/internal/database"
	"example.com/scanner_demo/scanner/internal/logger"
	"example.com/scanner_demo/scanner/internal/notify/tg"
	"example.com/scanner_demo/scanner/internal/rpc"
	"example.com/scanner_demo/scanner/internal/scanner"
)

func main() {
	if err := run(); err != nil {
		slog.Error("application failed", "error", err)
		os.Exit(1)
	}
}

func run() (runErr error) {
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
	manager.AddCleanup(db.Close)

	// 每个链有两个后台任务 每个任务最多报错一次
	errCh := make(chan error, len(cfg.Chains)*2)

	reportError := func(chainName, task string, err error) {
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("%s %s: %w", chainName, task, err)
		}
	}

	for chainName, chainCfg := range cfg.Chains {
		if signalCtx.Err() != nil {
			return nil
		}
		initCtx, cancelInit := context.WithTimeout(manager.ctx, 10*time.Second)
		client, err := rpc.NewRPCClient(initCtx, chainCfg.RPCCfg)
		cancelInit()
		if err != nil {
			if signalCtx.Err() != nil {
				return nil
			}
			return fmt.Errorf("%s 初始化 RPC: %w", chainName, err)
		}
		manager.AddCleanup(client.Close)

		publisher, err := tg.NewTGPublisher(cfg.TG, log, chainCfg.ExplorerURL)
		if err != nil {
			return fmt.Errorf("%s 初始化 TG: %w", chainName, err)
		}

		s, err := scanner.NewScanner(
			manager.ctx,
			chainCfg.ScannerCfg,
			int64(chainCfg.ChainID),
			client,
			db,
			publisher,
		)
		if err != nil {
			return fmt.Errorf("%s 初始化 Scanner: %w", chainName, err)
		}
		manager.Go(func(ctx context.Context) {
			reportError(chainName, "TG消费", publisher.ConsumeMessage(ctx))
		})
		manager.Go(func(ctx context.Context) {
			reportError(chainName, "扫链", s.Run())
		})
		log.Info("链服务已启动", "chain", chainName)
	}
	select {
	case <-signalCtx.Done():
		log.Info("收到退出信号")
		return nil
	case err := <-errCh:
		return err
	}
}

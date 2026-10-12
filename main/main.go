package main

import (
	"log/slog"
	"os"

	"example.com/scanner_demo/config"
	"example.com/scanner_demo/logger"
)

func main() {
	if err := run(); err != nil {
		slog.Error("application failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadConfig("./config.json")
	if err != nil {
		return err
	}
	log, err := logger.NewLogger(cfg.Logger)
	if err != nil {
		return err
	}

	log.Info("application initialized",
		"chains", len(cfg.Chains),
	)

	return nil
}

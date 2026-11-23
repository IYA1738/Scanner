package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestChainSupervisionIsolatesFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	manager := NewManager(ctx)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()

	initializing := make(chan struct{})
	failStartup := make(chan struct{})
	running := make(chan struct{})
	failRunning := make(chan struct{})
	recovered := make(chan struct{})
	faultyStopped := make(chan struct{})
	manager.Go(func(ctx context.Context) {
		defer close(faultyStopped)
		attempt := 0
		superviseChain(ctx, log, time.Millisecond, func(ctx context.Context) error {
			attempt++
			switch attempt {
			case 1:
				close(initializing)
				select {
				case <-failStartup:
					return errors.New("RPC initialization failed")
				case <-ctx.Done():
					return ctx.Err()
				}
			case 2:
				close(running)
				select {
				case <-failRunning:
					return errors.New("scan failed")
				case <-ctx.Done():
					return ctx.Err()
				}
			default:
				close(recovered)
				<-ctx.Done()
				return ctx.Err()
			}
		})
	})

	healthyProgress := make(chan struct{})
	healthyStopped := make(chan struct{})
	var healthyAttempts atomic.Int32
	manager.Go(func(ctx context.Context) {
		defer close(healthyStopped)
		superviseChain(ctx, log, time.Millisecond, func(ctx context.Context) error {
			healthyAttempts.Add(1)
			for {
				select {
				case <-healthyProgress:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	})

	waitFor := func(signal <-chan struct{}, stage string) {
		t.Helper()
		select {
		case <-signal:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", stage)
		}
	}
	checkHealthy := func(stage string) {
		t.Helper()
		select {
		case healthyProgress <- struct{}{}:
		case <-deadline.C:
			t.Fatalf("healthy chain stopped making progress %s", stage)
		}
	}

	waitFor(initializing, "the first initialization attempt")
	checkHealthy("during the other chain's initialization")
	close(failStartup)
	waitFor(running, "retry after initialization failure")
	checkHealthy("after the initialization failure")
	close(failRunning)
	waitFor(recovered, "retry after scan failure")
	checkHealthy("after the scan failure")

	cancel()
	waitFor(faultyStopped, "the retried chain to stop")
	waitFor(healthyStopped, "the healthy chain to stop")
	if got := healthyAttempts.Load(); got != 1 {
		t.Fatalf("healthy chain started %d times, want 1", got)
	}
}

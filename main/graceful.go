package main

import (
	"context"
	"errors"
	"sync"
)

type Manager struct {
	ctx          context.Context
	cancel       context.CancelFunc
	cleanupFuncs []func() error
	wg           sync.WaitGroup
}

func NewManager() *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (m *Manager) Go(fn func(context.Context)) {
	m.wg.Add(1)

	go func() {
		defer m.wg.Done()
		fn(m.ctx)
	}()
}

func (m *Manager) AddCleanup(fn func() error) {
	m.cleanupFuncs = append(m.cleanupFuncs, fn)
}

func (m *Manager) Shutdown() error {
	m.cancel()

	var errs []error
	for i := len(m.cleanupFuncs) - 1; i >= 0; i-- {
		if err := m.cleanupFuncs[i](); err != nil {
			errs = append(errs, err)
		}
	}

	m.wg.Wait()
	return errors.Join(errs...)
}

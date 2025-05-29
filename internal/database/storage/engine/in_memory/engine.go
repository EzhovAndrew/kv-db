package in_memory

import (
	"context"
	"sync"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
)

type Engine struct {
	db   map[string]string
	lock sync.RWMutex
}

func NewEngine() (*Engine, error) {
	return &Engine{
		db:   make(map[string]string),
		lock: sync.RWMutex{},
	}, nil
}

func (e *Engine) Get(ctx context.Context, key string) (string, error) {
	var result string
	concurrency.WithLock(&e.lock, func() {
		result = e.db[key]
	})
	return result, nil
}

func (e *Engine) Set(ctx context.Context, key, value string) error {
	concurrency.WithLock(&e.lock, func() {
		e.db[key] = value
	})
	return nil
}

func (e *Engine) Delete(ctx context.Context, key string) error {
	concurrency.WithLock(&e.lock, func() {
		delete(e.db, key)
	})
	return nil
}

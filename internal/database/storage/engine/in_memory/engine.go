package in_memory

import (
	"context"
	"errors"
	"sync"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("key cannot be empty")
	ErrEmptyValue  = errors.New("value cannot be empty")
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
	concurrency.WithLock(e.lock.RLocker(), func() {
		result = e.db[key]
	})
	if result == "" {
		return "", ErrKeyNotFound
	}
	return result, nil
}

func (e *Engine) Set(ctx context.Context, key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}
	if value == "" {
		return ErrEmptyValue
	}
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

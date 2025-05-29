package in_memory

import (
	"context"
	"sync"
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
	return "", nil
}

func (e *Engine) Set(ctx context.Context, key, value string) error {
	return nil
}

func (e *Engine) Delete(ctx context.Context, key string) error {
	return nil
}

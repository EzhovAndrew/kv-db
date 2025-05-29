package storage

import (
	"context"
	"errors"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/engine/in_memory"
)

var ErrUnknownEngine = errors.New("unknown engine type")

type Engine interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
}

type Storage struct {
	engine Engine
}

func NewStorage(cfg *configuration.EngineConfig) (*Storage, error) {
	var engine Engine
	switch cfg.Type {
	case configuration.EngineInMemoryKey:
		inMemEngine, err := in_memory.NewEngine()
		if err != nil {
			return nil, err
		}
		engine = inMemEngine
	default:
		return nil, ErrUnknownEngine
	}
	return &Storage{engine: engine}, nil
}

func (s *Storage) Get(ctx context.Context, key string) (string, error) {
	return s.engine.Get(ctx, key)
}

func (s *Storage) Set(ctx context.Context, key, value string) error {
	return s.engine.Set(ctx, key, value)
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	return s.engine.Delete(ctx, key)
}

package storage

import (
	"context"
	"errors"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/engine/in_memory"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/wal"
)

var ErrUnknownEngine = errors.New("unknown engine type")

type Engine interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
}

type WAL interface {
	Recover() (*[]wal.Log, error)
	Set(key string, value string) uint64
	Delete(key string) uint64
}

type Storage struct {
	engine Engine
	wal    WAL
}

func NewStorage(cfg *configuration.Config) (*Storage, error) {
	var engine Engine
	switch cfg.Engine.Type {
	case configuration.EngineInMemoryKey:
		inMemEngine, err := in_memory.NewEngine()
		if err != nil {
			return nil, err
		}
		engine = inMemEngine
	default:
		return nil, ErrUnknownEngine
	}
	if cfg.WAL == nil {
		return &Storage{engine: engine, wal: nil}, nil
	}
	walEngine := wal.NewWAL(cfg.WAL)
	return &Storage{engine: engine, wal: walEngine}, nil
}

func (s *Storage) Get(ctx context.Context, key string) (string, error) {
	return s.engine.Get(ctx, key)
}

func (s *Storage) Set(ctx context.Context, key, value string) error {
	if s.wal != nil {
		lsn := s.wal.Set(key, value)
	}
	return s.engine.Set(ctx, key, value)
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	return s.engine.Delete(ctx, key)
}

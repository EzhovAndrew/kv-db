package storage

import (
	"context"
	"errors"
	"iter"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/engine/in_memory"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/wal"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/utils"
	"go.uber.org/zap"
)

var ErrUnknownEngine = errors.New("unknown engine type")

type Engine interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	// if enabled, all operations applying will be ordered by LSN
	EnableLSNOrdering()
	DisableLSNOrdering()
	SetCurrentAppliedLSN(lsn uint64)
}

type WAL interface {
	Recover() iter.Seq2[wal.Log, error]
	Set(key string, value string) uint64
	Delete(key string) uint64
	Shutdown()
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
	storage := &Storage{engine: engine, wal: walEngine}
	err := storage.recover()
	if err != nil {
		logging.Fatal(err.Error(), zap.Stack("recover"))
	}
	return storage, nil
}

// Because key-value dbs are often used as cache
// we will optimize get function using only in-memory operations
func (s *Storage) Get(ctx context.Context, key string) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	return s.engine.Get(ctx, key)
}

func (s *Storage) Set(ctx context.Context, key, value string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.wal != nil {
		lsn := s.wal.Set(key, value)
		ctx = utils.ContextWithLSN(ctx, lsn)
	}

	return s.engine.Set(ctx, key, value)
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s.wal != nil {
		lsn := s.wal.Delete(key)
		ctx = utils.ContextWithLSN(ctx, lsn)
	}

	return s.engine.Delete(ctx, key)
}

func (s *Storage) Shutdown() {
	s.wal.Shutdown()
}

func (s *Storage) recover() error {
	if s.wal == nil {
		return nil
	}
	s.engine.DisableLSNOrdering()
	var lastAppliedLSN uint64 = 0
	for log, err := range s.wal.Recover() {
		if err != nil {
			return err
		}
		switch log.Command {
		case compute.SetCommandID:
			err := s.engine.Set(context.Background(), log.Arguments[0], log.Arguments[1])
			if err != nil {
				return err
			}
		case compute.DelCommandID:
			err := s.engine.Delete(context.Background(), log.Arguments[0])
			if err != nil {
				return err
			}
		}
		lastAppliedLSN = log.LSN
	}
	s.engine.SetCurrentAppliedLSN(lastAppliedLSN)
	s.engine.EnableLSNOrdering()
	return nil
}

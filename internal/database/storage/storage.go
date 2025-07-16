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

var (
	// Engine errors
	ErrUnknownEngine = errors.New("unknown engine type")

	// WAL errors
	ErrWALNotEnabled  = errors.New("WAL is not enabled")
	ErrWALWriteFailed = errors.New("failed to write logs to WAL")

	// Recovery errors
	ErrRecoveryFailed = errors.New("recovery failed")
	ErrLogReadFailed  = errors.New("failed to read log during recovery")
	ErrLogApplyFailed = errors.New("failed to apply log during recovery")

	// Log validation errors
	ErrEmptyLogs           = errors.New("logs cannot be empty")
	ErrLogNoArguments      = errors.New("log entry has no arguments")
	ErrLogNilEntry         = errors.New("log entry is nil")
	ErrSetInvalidArguments = errors.New("SET command requires exactly 2 arguments")
	ErrDelInvalidArguments = errors.New("DELETE command requires exactly 1 argument")
	ErrUnknownCommand      = errors.New("unknown command")

	// Replication errors
	ErrLogApplyReplicationFailed = errors.New("failed to apply log during replication")
)

// Engine defines the interface for storage engines
type Engine interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	// if enabled, all operations applying will be ordered by LSN
	EnableLSNOrdering()
	DisableLSNOrdering()
	SetCurrentAppliedLSN(lsn uint64)
}

// WAL defines the interface for Write-Ahead Log operations
type WAL interface {
	Recover() iter.Seq2[*wal.LogEntry, error]
	Set(key string, value string) *wal.Future
	Delete(key string) *wal.Future
	Shutdown()
	SetLastLSN(lsn uint64)

	// for replication purposes
	GetLastLSN() uint64
	WriteLogs(logs []*wal.LogEntry) error
	ReadLogsFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*wal.LogEntry, error]
}

// Storage provides a persistent key-value storage with optional WAL support
type Storage struct {
	engine Engine
	wal    WAL
}

// NewStorage creates a new Storage instance with the given configuration
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
		// Clean up on failure
		walEngine.Shutdown()
		return nil, errors.Join(ErrRecoveryFailed, err)
	}
	return storage, nil
}

// Get retrieves a value by key (optimized for cache usage with in-memory operations only)
func (s *Storage) Get(ctx context.Context, key string) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	return s.engine.Get(ctx, key)
}

// Set stores a key-value pair with optional WAL persistence
func (s *Storage) Set(ctx context.Context, key, value string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ctx, err := s.handleWALOperation(ctx, func() *wal.Future {
		return s.wal.Set(key, value)
	})
	if err != nil {
		return err
	}

	return s.engine.Set(ctx, key, value)
}

// Delete removes a key-value pair with optional WAL persistence
func (s *Storage) Delete(ctx context.Context, key string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ctx, err := s.handleWALOperation(ctx, func() *wal.Future {
		return s.wal.Delete(key)
	})
	if err != nil {
		return err
	}

	return s.engine.Delete(ctx, key)
}

// Shutdown gracefully shuts down the storage system
func (s *Storage) Shutdown() {
	if s.wal == nil {
		return
	}
	s.wal.Shutdown()
}

// ApplyLogs applies a batch of log entries for replication purposes
func (s *Storage) ApplyLogs(logs []*wal.LogEntry) error {
	if len(logs) == 0 {
		return ErrEmptyLogs
	}
	if s.wal == nil {
		return ErrWALNotEnabled
	}

	s.engine.DisableLSNOrdering()
	defer s.engine.EnableLSNOrdering()

	// Write logs to WAL first for durability
	if err := s.wal.WriteLogs(logs); err != nil {
		return errors.Join(ErrWALWriteFailed, err)
	}

	// Apply logs to engine
	var lastAppliedLSN uint64
	for i, log := range logs {
		if log == nil {
			logging.Error("Nil log entry in batch", zap.Int("index", i))
			return ErrLogNilEntry
		}

		err := s.applyLogToEngine(log)
		if err != nil {
			logging.Error("Failed to apply log during replication",
				zap.Uint64("lsn", log.LSN),
				zap.Int("index", i),
				zap.Error(err))
			return errors.Join(ErrLogApplyReplicationFailed, err)
		}

		if log.LSN > lastAppliedLSN {
			lastAppliedLSN = log.LSN
		}
	}

	// Update engine's current applied LSN
	s.engine.SetCurrentAppliedLSN(lastAppliedLSN)

	logging.Info("Applied logs for replication",
		zap.Int("logCount", len(logs)),
		zap.Uint64("lastAppliedLSN", lastAppliedLSN),
		zap.String("component", "Storage"))

	return nil
}

// GetLastLSN returns the last LSN processed by the storage system
func (s *Storage) GetLastLSN() uint64 {
	if s.wal == nil {
		return 0
	}
	return s.wal.GetLastLSN()
}

// ReadLogsFromLSN reads log entries starting from the specified LSN
func (s *Storage) ReadLogsFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*wal.LogEntry, error] {
	if s.wal == nil {
		return func(yield func(*wal.LogEntry, error) bool) {
			yield(nil, ErrWALNotEnabled)
		}
	}
	return s.wal.ReadLogsFromLSN(ctx, lsn)
}

// handleWALOperation handles the common WAL operation pattern for Set/Delete operations
func (s *Storage) handleWALOperation(ctx context.Context, operation func() *wal.Future) (context.Context, error) {
	if s.wal == nil {
		return ctx, nil
	}

	future := operation()
	lsn, err := future.Wait()
	if err != nil {
		return ctx, err
	}

	return utils.ContextWithLSN(ctx, lsn), nil
}

// applyLogToEngine applies a single log entry to the storage engine
func (s *Storage) applyLogToEngine(log *wal.LogEntry) error {
	// Validate log entry
	if log == nil {
		return ErrLogNilEntry
	}
	if len(log.Arguments) == 0 {
		return ErrLogNoArguments
	}

	// Create context with LSN for proper ordering
	ctx := utils.ContextWithLSN(context.Background(), log.LSN)

	switch log.Command {
	case compute.SetCommandID:
		if len(log.Arguments) != 2 {
			return ErrSetInvalidArguments
		}
		return s.engine.Set(ctx, log.Arguments[0], log.Arguments[1])
	case compute.DelCommandID:
		if len(log.Arguments) != 1 {
			return ErrDelInvalidArguments
		}
		return s.engine.Delete(ctx, log.Arguments[0])
	default:
		return ErrUnknownCommand
	}
}

// recover restores the storage state from WAL during startup
func (s *Storage) recover() error {
	if s.wal == nil {
		return nil
	}

	s.engine.DisableLSNOrdering()
	defer s.engine.EnableLSNOrdering()

	var lastAppliedLSN uint64
	for log, err := range s.wal.Recover() {
		if err != nil {
			return errors.Join(ErrLogReadFailed, err)
		}
		err := s.applyLogToEngine(log)
		if err != nil {
			logging.Error("Failed to apply log during recovery",
				zap.Uint64("lsn", log.LSN),
				zap.Error(err))
			return errors.Join(ErrLogApplyFailed, err)
		}
		lastAppliedLSN = log.LSN
	}

	s.engine.SetCurrentAppliedLSN(lastAppliedLSN)
	s.wal.SetLastLSN(lastAppliedLSN)

	logging.Info("Recovery completed",
		zap.Uint64("lastAppliedLSN", lastAppliedLSN),
		zap.String("component", "Storage"))

	return nil
}

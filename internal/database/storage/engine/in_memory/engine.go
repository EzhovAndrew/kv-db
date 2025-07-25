package in_memory

import (
	"context"
	"errors"
	"hash/fnv"
	"runtime"
	"sync"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/utils"
)

var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrEmptyKey           = errors.New("key cannot be empty")
	ErrEmptyValue         = errors.New("value cannot be empty")
	ErrEngineShuttingDown = errors.New("engine is shutting down")
)

var NumShards = uint32(runtime.NumCPU())

type shard struct {
	data map[string]string
	lock sync.RWMutex
}

type Engine struct {
	shards  []*shard
	workers []*Worker
}

// operationContext holds the LSN and count extracted from context
type operationContext struct {
	lsn        uint64
	count      int
	hasContext bool
}

func NewEngine() (*Engine, error) {
	e := &Engine{
		shards:  make([]*shard, NumShards),
		workers: make([]*Worker, NumShards),
	}

	for i := range NumShards {
		e.shards[i] = &shard{
			data: make(map[string]string),
			lock: sync.RWMutex{},
		}
		e.workers[i] = newWorker(e.shards[i])
	}

	return e, nil
}

func (e *Engine) hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % NumShards
}

func (e *Engine) getShard(key string) *shard {
	return e.shards[e.hashKey(key)]
}

func (e *Engine) Get(ctx context.Context, key string) (string, error) {
	shard := e.getShard(key)
	var result string
	concurrency.WithLock(shard.lock.RLocker(), func() error { // nolint:errcheck
		result = shard.data[key]
		return nil
	})
	if result == "" {
		return "", ErrKeyNotFound
	}
	return result, nil
}

// extractOperationContext extracts LSN and count from context with fallback logic
func (e *Engine) extractOperationContext(ctx context.Context) operationContext {
	// Try to get LSN and count from context
	lsn, count, hasLSNAndCount := utils.LSNAndCountFromContext(ctx)
	if hasLSNAndCount {
		return operationContext{lsn: lsn, count: count, hasContext: true}
	}

	// If count is not available, try to get just LSN (for recovery/replication)
	if lsnValue, hasLSN := utils.LSNFromContext(ctx); hasLSN {
		return operationContext{lsn: lsnValue, count: 1, hasContext: true}
	}

	// No context available - direct operation
	return operationContext{hasContext: false}
}

// executeSetImmediate performs an immediate set operation on the shard
func (e *Engine) executeSetImmediate(key, value string) error {
	shard := e.getShard(key)
	concurrency.WithLock(&shard.lock, func() error { // nolint:errcheck
		shard.data[key] = value
		return nil
	})
	return nil
}

// executeDeleteImmediate performs an immediate delete operation on the shard
func (e *Engine) executeDeleteImmediate(key string) error {
	shard := e.getShard(key)
	concurrency.WithLock(&shard.lock, func() error { // nolint:errcheck
		delete(shard.data, key)
		return nil
	})
	return nil
}

// executeOperation handles the common operation execution logic
func (e *Engine) executeOperation(ctx context.Context, opType OperationType, key, value string) error {
	opCtx := e.extractOperationContext(ctx)

	// If no context available or count is 1, execute immediately
	if !opCtx.hasContext || opCtx.count == 1 {
		switch opType {
		case OpSet:
			return e.executeSetImmediate(key, value)
		case OpDelete:
			return e.executeDeleteImmediate(key)
		}
	}

	// If count > 1, use worker to handle ordering
	future := e.submitToWorker(opType, key, value, opCtx.lsn, opCtx.count, ctx)
	result := future.Wait()
	return result.Error()
}

func (e *Engine) Set(ctx context.Context, key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}
	if value == "" {
		return ErrEmptyValue
	}

	return e.executeOperation(ctx, OpSet, key, value)
}

func (e *Engine) Delete(ctx context.Context, key string) error {
	return e.executeOperation(ctx, OpDelete, key, "")
}

func (e *Engine) submitToWorker(
	opType OperationType,
	key,
	value string,
	lsn uint64,
	count int,
	ctx context.Context,
) *EngineFuture {
	shardIndex := e.hashKey(key)
	worker := e.workers[shardIndex]

	future := newEngineFuture()

	operation := &EngineOperation{
		opType: opType,
		key:    key,
		value:  value,
		lsn:    lsn,
		count:  count,
		future: future,
		ctx:    ctx,
	}

	worker.submitOperation(operation)
	return future
}

func (e *Engine) Shutdown() {
	for _, worker := range e.workers {
		worker.shutdown()
	}
}

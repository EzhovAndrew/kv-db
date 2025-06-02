package in_memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/utils"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("key cannot be empty")
	ErrEmptyValue  = errors.New("value cannot be empty")
)

type Engine struct {
	db                 map[string]string
	lock               sync.RWMutex
	currentAppliedLSN  atomic.Uint64
	lsnOrderingEnabled atomic.Bool
	lsnCond            *sync.Cond
	lsnMutex           sync.Mutex
}

func NewEngine() (*Engine, error) {
	e := &Engine{
		db:   make(map[string]string),
		lock: sync.RWMutex{},
	}
	e.lsnCond = sync.NewCond(&e.lsnMutex)
	return e, nil
}

func (e *Engine) Get(ctx context.Context, key string) (string, error) {
	var result string
	concurrency.WithLock(e.lock.RLocker(), func() error { //nolint:errcheck
		result = e.db[key]
		return nil
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
	if e.lsnOrderingEnabled.Load() {
		err := e.waitForLSNFromContext(ctx)
		if err != nil {
			return err
		}
	}
	concurrency.WithLock(&e.lock, func() error { //nolint:errcheck
		e.db[key] = value
		return nil
	})
	e.incrementLSNAndNotify()
	return nil
}

func (e *Engine) Delete(ctx context.Context, key string) error {
	if e.lsnOrderingEnabled.Load() {
		err := e.waitForLSNFromContext(ctx)
		if err != nil {
			return err
		}
	}
	concurrency.WithLock(&e.lock, func() error { //nolint:errcheck
		delete(e.db, key)
		return nil
	})
	e.incrementLSNAndNotify()
	return nil
}

func (e *Engine) EnableLSNOrdering() {
	e.lsnOrderingEnabled.Store(true)
}

func (e *Engine) DisableLSNOrdering() {
	e.lsnOrderingEnabled.Store(false)
}

func (e *Engine) SetCurrentAppliedLSN(lsn uint64) {
	concurrency.WithLock(&e.lsnMutex, func() error { //nolint:errcheck
		e.currentAppliedLSN.Store(lsn)
		e.lsnCond.Broadcast()
		return nil
	})
}

func (e *Engine) incrementLSNAndNotify() {
	concurrency.WithLock(&e.lsnMutex, func() error { //nolint:errcheck
		e.currentAppliedLSN.Add(1)
		e.lsnCond.Broadcast()
		return nil
	})
}

func (e *Engine) waitForLSNFromContext(ctx context.Context) error {
	lsn, err := utils.MustLSNFromContext(ctx)
	if err != nil {
		return err
	}
	targetLSN := lsn - 1

	err = concurrency.WithLock(&e.lsnMutex, func() error {
		for e.currentAppliedLSN.Load() != targetLSN {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				e.lsnCond.Wait()
			}()

			select {
			case <-done:
			case <-ctx.Done():
				e.lsnCond.Broadcast()
				<-done
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

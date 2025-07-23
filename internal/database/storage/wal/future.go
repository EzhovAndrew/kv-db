package wal

import (
	"sync"
	"time"
)

// FutureResult represents the result of a WAL operation
type FutureResult struct {
	lsn   uint64
	count int
	err   error
}

func (r *FutureResult) LSN() uint64 {
	return r.lsn
}

func (r *FutureResult) Count() int {
	return r.count
}

func (r *FutureResult) Error() error {
	return r.err
}

// Future represents a pending WAL operation result
type Future struct {
	done   chan struct{}
	result *FutureResult
	once   sync.Once
}

func (f *Future) Done() <-chan struct{} {
	return f.done
}

func (f *Future) Wait() *FutureResult {
	<-f.done
	return f.result
}

func (f *Future) WaitWithTimeout(timeout time.Duration) *FutureResult {
	select {
	case <-f.done:
		return f.result
	case <-time.After(timeout):
		return &FutureResult{
			lsn:   0,
			count: 0,
			err:   ErrOperationTimeout,
		}
	}
}

func (f *Future) complete(result *FutureResult) {
	f.once.Do(func() {
		f.result = result
		close(f.done)
	})
}

func newFuture() *Future {
	return &Future{done: make(chan struct{})}
}

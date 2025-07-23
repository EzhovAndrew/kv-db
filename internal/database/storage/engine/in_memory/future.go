package in_memory

import (
	"sync"
)

// EngineFutureResult represents the result of an engine operation
type EngineFutureResult struct {
	err error
}

func (r *EngineFutureResult) Error() error {
	return r.err
}

// EngineFuture represents a pending engine operation result
type EngineFuture struct {
	done   chan struct{}
	result *EngineFutureResult
	once   sync.Once
}

func (f *EngineFuture) Done() <-chan struct{} {
	return f.done
}

func (f *EngineFuture) Wait() *EngineFutureResult {
	<-f.done
	return f.result
}

func (f *EngineFuture) complete(result *EngineFutureResult) {
	f.once.Do(func() {
		f.result = result
		close(f.done)
	})
}

func newEngineFuture() *EngineFuture {
	return &EngineFuture{done: make(chan struct{})}
}

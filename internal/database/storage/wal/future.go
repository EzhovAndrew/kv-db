package wal

import (
	"sync"
	"time"
)

// Future represents a pending WAL operation result
type Future struct {
	done chan struct{}
	lsn  uint64
	err  error
	once sync.Once
}

// Done returns a channel that will be closed when the operation completes
func (f *Future) Done() <-chan struct{} {
	return f.done
}

// Wait blocks until the operation completes and returns the result
func (f *Future) Wait() (uint64, error) {
	<-f.done
	return f.lsn, f.err
}

// WaitWithTimeout blocks until the operation completes or timeout occurs
func (f *Future) WaitWithTimeout(timeout time.Duration) (uint64, error) {
	select {
	case <-f.done:
		return f.lsn, f.err
	case <-time.After(timeout):
		return 0, ErrOperationTimeout
	}
}

// LSN returns the LSN assigned to this operation (only valid after Done() is closed)
func (f *Future) LSN() uint64 {
	return f.lsn
}

// Error returns any error that occurred (only valid after Done() is closed)
func (f *Future) Error() error {
	return f.err
}

// complete sets the result and closes the done channel (can only be called once)
func (f *Future) complete(lsn uint64, err error) {
	f.once.Do(func() {
		f.lsn = lsn
		f.err = err
		close(f.done)
	})
}

// newFuture creates a new Future instance
func newFuture() *Future {
	return &Future{done: make(chan struct{})}
}

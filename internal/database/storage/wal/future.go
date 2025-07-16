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

func (f *Future) Done() <-chan struct{} {
	return f.done
}

func (f *Future) Wait() (uint64, error) {
	<-f.done
	return f.lsn, f.err
}

func (f *Future) WaitWithTimeout(timeout time.Duration) (uint64, error) {
	select {
	case <-f.done:
		return f.lsn, f.err
	case <-time.After(timeout):
		return 0, ErrOperationTimeout
	}
}

func (f *Future) LSN() uint64 {
	return f.lsn
}

func (f *Future) Error() error {
	return f.err
}

func (f *Future) complete(lsn uint64, err error) {
	f.once.Do(func() {
		f.lsn = lsn
		f.err = err
		close(f.done)
	})
}

func newFuture() *Future {
	return &Future{done: make(chan struct{})}
}

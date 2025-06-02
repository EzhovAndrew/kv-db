package concurrency

import (
	"errors"
	"sync"
)

func WithLock(lock sync.Locker, fn func() error) error {
	if fn == nil {
		return errors.New("nil function")
	}
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

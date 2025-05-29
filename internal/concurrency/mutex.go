package concurrency

import "sync"

func WithLock(lock sync.Locker, fn func()) {
	if fn == nil {
		return
	}
	lock.Lock()
	defer lock.Unlock()
	fn()
}

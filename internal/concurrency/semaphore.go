package concurrency

type Semaphore struct {
	tickets chan struct{}
}

func NewSemaphore(maxConcurrency int) *Semaphore {
	return &Semaphore{
		tickets: make(chan struct{}, maxConcurrency),
	}
}

func (s *Semaphore) Acquire() {
	if s == nil || s.tickets == nil {
		return
	}
	s.tickets <- struct{}{}
}

func (s *Semaphore) TryAcquire() bool {
	if s == nil || s.tickets == nil {
		return false
	}
	select {
	case s.tickets <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Semaphore) Release() {
	if s == nil || s.tickets == nil {
		return
	}
	<-s.tickets
}

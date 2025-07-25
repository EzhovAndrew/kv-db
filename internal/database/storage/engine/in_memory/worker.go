package in_memory

import (
	"context"
	"sort"
	"sync"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
)

type OperationType int

const (
	OpSet OperationType = iota
	OpDelete
)

type EngineOperation struct {
	opType OperationType
	key    string
	value  string // only used for Set operations
	lsn    uint64
	count  int
	future *EngineFuture
	ctx    context.Context
}

type KeyOperations struct {
	operations []*EngineOperation
	received   int
	expected   int
}

type Worker struct {
	shard      *shard
	operations chan *EngineOperation
	keyOps     map[string]*KeyOperations
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

func newWorker(shard *shard) *Worker {
	w := &Worker{
		shard:      shard,
		operations: make(chan *EngineOperation, 1000),
		keyOps:     make(map[string]*KeyOperations),
		shutdownCh: make(chan struct{}),
	}

	w.wg.Add(1)
	go w.run()

	return w
}

func (w *Worker) run() {
	defer w.wg.Done()

	for {
		select {
		case <-w.shutdownCh:
			w.processRemainingOperations()
			return
		case op := <-w.operations:
			w.handleOperation(op)
		}
	}
}

func (w *Worker) handleOperation(op *EngineOperation) {
	keyOps, exists := w.keyOps[op.key]
	if !exists {
		keyOps = &KeyOperations{
			operations: make([]*EngineOperation, 0, op.count),
			received:   0,
			expected:   op.count,
		}
		w.keyOps[op.key] = keyOps
	}

	keyOps.operations = append(keyOps.operations, op)
	keyOps.received++

	if keyOps.received == keyOps.expected {
		w.processKeyOperations(keyOps)
		delete(w.keyOps, op.key)
	}
}

func (w *Worker) processKeyOperations(keyOps *KeyOperations) {
	sort.Slice(keyOps.operations, func(i, j int) bool {
		return keyOps.operations[i].lsn < keyOps.operations[j].lsn
	})

	err := concurrency.WithLock(&w.shard.lock, func() error {
		for _, op := range keyOps.operations {
			switch op.opType {
			case OpSet:
				w.shard.data[op.key] = op.value
			case OpDelete:
				delete(w.shard.data, op.key)
			}
		}
		return nil
	})

	for _, op := range keyOps.operations {
		op.future.complete(&EngineFutureResult{err: err})
	}
}

func (w *Worker) processRemainingOperations() {
drainLoop:
	for {
		select {
		case op := <-w.operations:
			op.future.complete(&EngineFutureResult{err: ErrEngineShuttingDown})
		default:
			break drainLoop
		}
	}

	for _, keyOps := range w.keyOps {
		for _, op := range keyOps.operations {
			op.future.complete(&EngineFutureResult{err: ErrEngineShuttingDown})
		}
	}
}

func (w *Worker) submitOperation(op *EngineOperation) {
	select {
	case w.operations <- op:
	case <-w.shutdownCh:
		op.future.complete(&EngineFutureResult{err: ErrEngineShuttingDown})
	}
}

func (w *Worker) shutdown() {
	close(w.shutdownCh)
	w.wg.Wait()
}

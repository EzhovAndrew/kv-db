package wal

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/encoders"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/filesystem"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type LogEntry = encoders.Log

type LogMessage struct {
	entry  *LogEntry
	future *Future
}

type LogsWriter interface {
	Write(logs []*LogEntry) error
}

type LogsReader interface {
	Read() iter.Seq2[*LogEntry, error]
	ReadFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*LogEntry, error]
}

type WAL struct {
	batch      []*LogEntry
	logsWriter LogsWriter
	logsReader LogsReader

	newLogsChan    chan LogMessage
	pendingFutures []*Future
	batchKeyCounts map[string]int

	lsnGenerator *LSNGenerator

	shutdownChan chan struct{}
	closed       bool
	shutdownOnce sync.Once
}

func NewWAL(config *configuration.WALConfig) *WAL {
	fileSystem := filesystem.NewSegmentedFileSystem(config.DataDirectory, config.MaxSegmentSize)
	wal := &WAL{
		batch:          make([]*LogEntry, 0, config.FlushBatchSize),
		pendingFutures: make([]*Future, 0, config.FlushBatchSize),
		batchKeyCounts: make(map[string]int, config.FlushBatchSize),
		lsnGenerator:   NewLSNGenerator(0),
		newLogsChan:    make(chan LogMessage),
		shutdownChan:   make(chan struct{}),
		logsWriter:     NewFileLogsWriter(fileSystem),
		logsReader:     NewFileLogsReader(fileSystem),
	}
	go wal.handleNewLogs(config)
	return wal
}

func (w *WAL) Recover() iter.Seq2[*LogEntry, error] {
	return func(yield func(*LogEntry, error) bool) {
		for log, err := range w.logsReader.Read() {
			if !yield(log, err) {
				return
			}
		}
	}
}

func (w *WAL) SetLastLSN(lsn uint64) {
	w.lsnGenerator.ResetToLSN(lsn)
}

func (w *WAL) handleNewLogs(config *configuration.WALConfig) {
	flushTimeout := time.Duration(config.FlushBatchTimeout)
	timer := w.createFlushTimer(flushTimeout)
	defer timer.Stop()

	for {
		select {
		case <-w.shutdownChan:
			w.handleShutdown()
			return

		case logMessage := <-w.newLogsChan:
			w.handleNewLogMessage(logMessage, config, timer, flushTimeout)

		case <-timer.C:
			w.handleFlushTimeout()
		}
	}
}

func (w *WAL) createFlushTimer(flushTimeout time.Duration) *time.Timer {
	timer := time.NewTimer(flushTimeout)
	if !timer.Stop() {
		<-timer.C
	}
	return timer
}

func (w *WAL) handleShutdown() {
	if err := w.flushToDisk(); err != nil {
		logging.Error("Failed to flush during shutdown", zap.Error(err))
		w.completeBatchWithError(ErrFlushFailed)
	} else {
		w.completeBatchWithSuccess()
	}
}

// handleNewLogMessage processes a new log message
func (w *WAL) handleNewLogMessage(
	logMessage LogMessage,
	config *configuration.WALConfig,
	timer *time.Timer,
	flushTimeout time.Duration,
) {
	// Reset timer if this is the first log in the batch
	if len(w.batch) == 0 {
		timer.Reset(flushTimeout)
	}

	// Count key in batch
	key := w.extractKey(logMessage.entry)
	w.batchKeyCounts[key]++

	w.batch = append(w.batch, logMessage.entry)
	w.pendingFutures = append(w.pendingFutures, logMessage.future)

	if len(w.batch) >= config.FlushBatchSize {
		w.stopTimer(timer)
		w.flushAndRespond()
	}
}

// handleFlushTimeout processes a flush timeout - flush the batch and respond to clients
func (w *WAL) handleFlushTimeout() {
	w.flushAndRespond()
}

// stopTimer safely stops a timer and drains its channel
func (w *WAL) stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// flushAndRespond flushes the batch and responds to clients
func (w *WAL) flushAndRespond() {
	if err := w.flushToDisk(); err == nil {
		w.completeBatchWithSuccess()
	} else {
		w.completeBatchWithError(ErrFlushFailed)
	}
}

// flushToDisk writes the current batch to persistent storage
func (w *WAL) flushToDisk() error {
	if len(w.batch) == 0 {
		return nil
	}
	err := w.logsWriter.Write(w.batch)
	if err != nil {
		logging.Error(
			"Failed to write logs to disk",
			zap.Error(err),
			zap.String("component", "WAL"),
			zap.String("method", "flushToDisk"),
		)
	}
	return err
}

func (w *WAL) Shutdown() {
	w.shutdownOnce.Do(func() {
		w.closed = true
		close(w.shutdownChan)
	})
}

// completeBatchWithSuccess completes all pending futures with success
func (w *WAL) completeBatchWithSuccess() {
	for i, future := range w.pendingFutures {
		key := w.extractKey(w.batch[i])
		count := w.batchKeyCounts[key]
		future.complete(&FutureResult{
			lsn:   w.batch[i].LSN,
			count: count,
			err:   nil,
		})
	}
	w.clearBatch()
}

// completeBatchWithError completes all pending futures with the given error
func (w *WAL) completeBatchWithError(err error) {
	for i, future := range w.pendingFutures {
		key := w.extractKey(w.batch[i])
		count := w.batchKeyCounts[key]
		future.complete(&FutureResult{
			lsn:   0,
			count: count,
			err:   err,
		})
	}
	w.clearBatch()
}

// clearBatch resets the batch and pending futures
func (w *WAL) clearBatch() {
	w.pendingFutures = w.pendingFutures[:0]
	w.batch = w.batch[:0]
	w.batchKeyCounts = make(map[string]int)
}

func (w *WAL) extractKey(entry *LogEntry) string {
	return entry.Arguments[0]
}

// executeOperation executes a WAL operation with the given log entry
func (w *WAL) executeOperation(entry *LogEntry) *Future {
	future := newFuture()

	select {
	case w.newLogsChan <- LogMessage{entry: entry, future: future}:
		// Successfully sent to channel
	case <-w.shutdownChan:
		// WAL is shutting down, operation failed
		future.complete(&FutureResult{
			lsn:   0,
			count: 0,
			err:   ErrWALShuttingDown,
		})
	}

	return future
}

func (w *WAL) Set(key, value string) *Future {
	lsn := w.lsnGenerator.Next()
	entry := &LogEntry{
		LSN:       lsn,
		Command:   compute.SetCommandID,
		Arguments: []string{key, value},
	}
	return w.executeOperation(entry)
}

func (w *WAL) Delete(key string) *Future {
	lsn := w.lsnGenerator.Next()
	entry := &LogEntry{
		LSN:       lsn,
		Command:   compute.DelCommandID,
		Arguments: []string{key},
	}
	return w.executeOperation(entry)
}

func (w *WAL) GetLastLSN() uint64 {
	return w.lsnGenerator.Current()
}

// WriteLogs writes multiple log entries for replication purposes
func (w *WAL) WriteLogs(logs []*LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	// Create futures for all logs
	futures := make([]*Future, len(logs))
	for i := range futures {
		futures[i] = newFuture()
	}

	// Send logs through the normal batching mechanism to maintain single-writer guarantee
	for i, log := range logs {
		select {
		case w.newLogsChan <- LogMessage{entry: log, future: futures[i]}:
			// Successfully sent to channel
		case <-w.shutdownChan:
			return ErrWALShuttingDown
		}
	}

	// Wait for all futures to complete
	for i, future := range futures {
		select {
		case <-future.Done():
			if future.Wait().Error() != nil {
				return fmt.Errorf("log %d failed: %w", i, future.Wait().Error())
			}
		case <-w.shutdownChan:
			return fmt.Errorf("WAL shut down while waiting for log %d", i)
		}
	}

	return nil
}

func (w *WAL) ReadLogsFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*LogEntry, error] {
	return func(yield func(*LogEntry, error) bool) {
		for log, err := range w.logsReader.ReadFromLSN(ctx, lsn) {
			if !yield(log, err) {
				return
			}
		}
	}
}

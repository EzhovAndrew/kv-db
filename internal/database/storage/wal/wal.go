package wal

import (
	"context"
	"iter"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/encoders"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type Log = encoders.Log

type NewLog struct {
	log          *Log
	responseChan chan struct{}
}

type LogsWriter interface {
	Write(logs []*Log) error
}

type LogsReader interface {
	Read() iter.Seq2[*Log, error]
	ReadFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*Log, error]
}

type WAL struct {
	batch      []*Log
	logsWriter LogsWriter
	logsReader LogsReader

	newLogsChan   chan NewLog
	responseChans []chan struct{}

	lsnGenerator *LSNGenerator

	shutdownChan chan struct{}
	closed       bool
}

func NewWAL(cfg *configuration.WALConfig) *WAL {
	wal := &WAL{
		batch:         make([]*Log, 0, cfg.FlushBatchSize),
		responseChans: make([]chan struct{}, 0, cfg.FlushBatchSize),
		lsnGenerator:  NewLSNGenerator(0),
		newLogsChan:   make(chan NewLog),
		shutdownChan:  make(chan struct{}),
		logsWriter:    NewFileLogsWriter(cfg.DataDirectory, cfg.MaxSegmentSize),
		logsReader:    NewFileLogsReader(cfg.DataDirectory, cfg.MaxSegmentSize),
	}
	go wal.handleNewLogs(cfg)
	return wal
}

func (w *WAL) Recover() iter.Seq2[*Log, error] {
	return func(yield func(*Log, error) bool) {
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

func (w *WAL) handleNewLogs(cfg *configuration.WALConfig) {
	flushTimeout := time.Duration(cfg.FlushBatchTimeout)
	timer := time.NewTimer(flushTimeout)
	defer timer.Stop()

	// Initially stop the timer since we have no logs to flush
	if !timer.Stop() {
		<-timer.C
	}

	for {
		select {
		case <-w.shutdownChan:
			w.flushToDisk()
			return

		case newLog := <-w.newLogsChan:
			if len(w.batch) == 0 {
				timer.Reset(flushTimeout)
			}

			w.batch = append(w.batch, newLog.log)
			w.responseChans = append(w.responseChans, newLog.responseChan)

			if len(w.batch) >= cfg.FlushBatchSize {
				// Stop timer and drain if necessary
				// This is important for correct timeout behavior
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				w.flushToDisk()
				w.sendResponses()
			}

		case <-timer.C:
			w.flushToDisk()
			w.sendResponses()
		}
	}
}

func (w *WAL) flushToDisk() {
	if len(w.batch) == 0 {
		return
	}
	err := w.logsWriter.Write(w.batch)
	if err != nil {
		logging.Error(
			err.Error(),
			zap.String("summary", "Failed to write logs to disk"),
			zap.String("component", "WAL"),
			zap.String("method", "flushToDisk"),
		)
	}
}

func (w *WAL) Shutdown() {
	if w.closed {
		return
	}
	w.closed = true
	close(w.shutdownChan)
}

func (w *WAL) sendResponses() {
	for _, responseChan := range w.responseChans {
		responseChan <- struct{}{}
	}
	w.responseChans = w.responseChans[:0]
	w.batch = w.batch[:0]
}

func (w *WAL) Set(key, value string) uint64 {
	lsn := w.lsnGenerator.Next()
	log := &Log{LSN: lsn, Command: compute.SetCommandID, Arguments: []string{key, value}}
	responseChan := make(chan struct{})
	w.newLogsChan <- NewLog{log: log, responseChan: responseChan}
	<-responseChan
	return lsn
}

func (w *WAL) Delete(key string) uint64 {
	lsn := w.lsnGenerator.Next()
	log := &Log{LSN: lsn, Command: compute.DelCommandID, Arguments: []string{key}}
	responseChan := make(chan struct{})
	w.newLogsChan <- NewLog{log: log, responseChan: responseChan}
	<-responseChan
	return lsn
}

// for REPLICATION purposes

func (w *WAL) GetLastLSN() uint64 {
	return w.lsnGenerator.Current()
}

func (w *WAL) WriteLogs(logs []*Log) error {
	if len(logs) == 0 {
		return nil
	}
	if err := w.logsWriter.Write(logs); err != nil {
		return err
	}
	return nil
}

func (w *WAL) ReadLogsFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*Log, error] {
	return func(yield func(*Log, error) bool) {
		for log, err := range w.logsReader.ReadFromLSN(ctx, lsn) {
			if !yield(log, err) {
				return
			}
		}
	}
}

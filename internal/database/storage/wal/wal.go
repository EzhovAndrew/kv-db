package wal

import (
	"context"
	"iter"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type NewLog struct {
	log          *Log
	responseChan chan struct{}
}

type LogsWriter interface {
	Write(logs []*Log) error
}

type LogsReader interface {
	Read() iter.Seq2[Log, error]
}

type WAL struct {
	batch      []*Log
	logsWriter LogsWriter
	logsReader LogsReader

	newLogsChan   chan NewLog
	responseChans []chan struct{}

	lsnGenerator *LSNGenerator

	shutdownChan chan struct{}
}

func NewWAL(cfg *configuration.WALConfig) *WAL {
	wal := &WAL{
		batch:         make([]*Log, 0, cfg.FlushBatchSize),
		responseChans: make([]chan struct{}, 0, cfg.FlushBatchSize),
		lsnGenerator:  NewLSNGenerator(0),
		newLogsChan:   make(chan NewLog),
		shutdownChan:  make(chan struct{}),
		logsWriter:    NewFileLogsWriter(cfg.DataDirectory),
		logsReader:    NewFileLogsReader(cfg.DataDirectory),
	}
	go wal.handleNewLogs(cfg)
	return wal
}

func (w *WAL) Recover() iter.Seq2[Log, error] {
	return func(yield func(Log, error) bool) {
		for log, err := range w.logsReader.Read() {
			if !yield(log, err) {
				return
			}
		}
	}
}

func (w *WAL) handleNewLogs(cfg *configuration.WALConfig) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.FlushBatchTimeout))
		defer cancel()
	INNER:
		for {
			select {
			case <-w.shutdownChan:
				w.flushToDisk()
				return
			default:
			}

			// Prioritization of timeout
			select {
			case <-ctx.Done():
				w.flushToDisk()
				w.sendResponses()
				break INNER
			default:
			}

			select {
			case newLog := <-w.newLogsChan:
				w.batch = append(w.batch, newLog.log)
				w.responseChans = append(w.responseChans, newLog.responseChan)
				if len(w.batch) == cfg.FlushBatchSize {
					w.flushToDisk()
					w.sendResponses()
				}
				cancel()
				break INNER
			case <-ctx.Done():
				w.flushToDisk()
				w.sendResponses()
				break INNER
			}
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

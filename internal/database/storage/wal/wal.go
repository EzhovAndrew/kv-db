package wal

import "github.com/EzhovAndrew/kv-db/internal/configuration"

// just a mock
type WAL struct {
	logs []Log
}

func NewWAL(cfg *configuration.WALConfig) *WAL {
	return &WAL{logs: make([]Log, 0)}
}

func (w *WAL) Recover() (*[]Log, error) {
	return &w.logs, nil
}

func (w *WAL) Set(key, value string) uint64 {
	return 0
}

func (w *WAL) Delete(key string) uint64 {
	return 0
}

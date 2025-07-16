package wal

import "errors"

// Common error messages for consistency
var (
	ErrWALShuttingDown  = errors.New("WAL is shutting down")
	ErrFlushFailed      = errors.New("failed to write to disk")
	ErrOperationTimeout = errors.New("operation timed out")
)

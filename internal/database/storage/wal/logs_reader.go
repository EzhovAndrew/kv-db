package wal

import (
	"bytes"
	"context"
	"io"
	"iter"

	"github.com/EzhovAndrew/kv-db/internal/database/storage/encoders"
	"github.com/EzhovAndrew/kv-db/internal/logging"
)

type FileSystemReader interface {
	ReadAll() iter.Seq2[[]byte, error]
	GetSegmentForLSN(lsn uint64) (string, error)
	ReadContinuouslyFromSegment(segment string) iter.Seq2[[]byte, error]
}

type FileLogsReader struct {
	filesystem FileSystemReader
}

func NewFileLogsReader(fileSystem FileSystemReader) *FileLogsReader {
	return &FileLogsReader{filesystem: fileSystem}
}

func (l *FileLogsReader) Read() iter.Seq2[*LogEntry, error] {
	return func(yield func(*LogEntry, error) bool) {
		for data, err := range l.filesystem.ReadAll() {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if len(data) == 0 {
				continue
			}

			if err := l.processData(data, yield); err != nil {
				if !yield(nil, err) {
					return
				}
			}
		}
	}
}

func (l *FileLogsReader) ReadFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*LogEntry, error] {
	return func(yield func(*LogEntry, error) bool) {
		if ctx.Err() != nil {
			if !yield(nil, ctx.Err()) {
				return
			}
			return
		}
		segment, err := l.filesystem.GetSegmentForLSN(lsn)
		if err != nil {
			if !yield(nil, err) {
				return
			}
			return
		}
		for data, err := range l.filesystem.ReadContinuouslyFromSegment(segment) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if err := l.processDataFromLSN(data, lsn, ctx, yield); err != nil {
				if !yield(nil, err) {
					return
				}
			}
		}
	}
}

func (l *FileLogsReader) processData(data []byte, yield func(*LogEntry, error) bool) error {
	return l.processDataWithOptions(data, yield, nil, 0)
}

func (l *FileLogsReader) processDataFromLSN(data []byte, lsn uint64, ctx context.Context, yield func(*LogEntry, error) bool) error {
	return l.processDataWithOptions(data, yield, ctx, lsn)
}

func (l *FileLogsReader) processDataWithOptions(data []byte, yield func(*LogEntry, error) bool, ctx context.Context, minLSN uint64) error {
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		// Check context cancellation per log for better responsiveness (if context provided)
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		log, bytesRead, err := encoders.DecodeLog(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if bytesRead == 0 {
			// Prevent infinite loop if no bytes were consumed
			logging.Error("No bytes consumed during log decoding, stopping to prevent infinite loop")
			break
		}

		// Filter by LSN if minLSN is specified (> 0)
		if minLSN > 0 && log.LSN < minLSN {
			continue // Skip logs with LSN below minimum
		}

		if !yield(log, nil) {
			return nil // Consumer wants to stop
		}
	}

	return nil
}

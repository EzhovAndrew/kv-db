package wal

import (
	"bytes"
	"errors"

	"github.com/EzhovAndrew/kv-db/internal/database/storage/encoders"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"go.uber.org/zap"
)

type FileSystemWriteSyncer interface {
	WriteSync(data []byte, lsnStart uint64) error
}

type FileLogsWriter struct {
	filesystem FileSystemWriteSyncer
	buf        *bytes.Buffer
}

func NewFileLogsWriter(fileSystem FileSystemWriteSyncer) *FileLogsWriter {
	buf := &bytes.Buffer{}
	buf.Grow(9192)
	return &FileLogsWriter{filesystem: fileSystem, buf: buf}
}

func (l *FileLogsWriter) Write(logs []*LogEntry) (err error) {
	if len(logs) == 0 {
		return nil
	}

	defer func() {
		if v := recover(); v != nil {
			err = errors.New("Write logs to disk failed due to panic")
			logging.Error(
				"Failed to write logs to disk",
				zap.String("component", "WAL Logs Writer"),
				zap.String("method", "Write"),
				zap.Any("panic", v),
			)
		}
	}()

	for _, log := range logs {
		if log == nil {
			return errors.New("log entry cannot be nil")
		}
		encoders.EncodeLog(log, l.buf)
	}

	err = l.filesystem.WriteSync(l.buf.Bytes(), logs[0].LSN)
	l.buf.Reset()
	return err
}

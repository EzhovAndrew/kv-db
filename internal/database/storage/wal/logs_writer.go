package wal

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/EzhovAndrew/kv-db/internal/database/storage/filesystem"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/utils"
	"go.uber.org/zap"
)

type FileSystemWriteSyncer interface {
	WriteSync(data []byte) error
}

type FileLogsWriter struct {
	filesystem FileSystemWriteSyncer
	buf        *bytes.Buffer
}

func NewFileLogsWriter(dataDir string) *FileLogsWriter {
	fileSystem := filesystem.NewSegmentedFileSystem(dataDir)
	buf := &bytes.Buffer{}
	buf.Grow(9192)
	return &FileLogsWriter{filesystem: fileSystem, buf: buf}
}

func (l *FileLogsWriter) Write(logs []*Log) (err error) {
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

	l.buf.Reset()
	for _, log := range logs {
		l.encodeLog(log, l.buf)
	}
	return l.filesystem.WriteSync(l.buf.Bytes())
}

func (l *FileLogsWriter) encodeLog(log *Log, buf *bytes.Buffer) {
	var lsnBuf [10]byte
	n := binary.PutUvarint(lsnBuf[:], log.LSN)
	buf.Write(lsnBuf[:n])

	var cmdBuf [5]byte
	n = binary.PutUvarint(cmdBuf[:], uint64(log.Command))
	buf.Write(cmdBuf[:n])

	n = binary.PutUvarint(lsnBuf[:], uint64(len(log.Arguments)))
	buf.Write(lsnBuf[:n])

	for _, arg := range log.Arguments {
		n = binary.PutUvarint(lsnBuf[:], uint64(len(arg)))
		buf.Write(lsnBuf[:n])
		buf.Write(utils.StringToBytes(arg))
	}
}

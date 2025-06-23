package wal

import (
	"bytes"
	"context"
	"io"
	"iter"

	"github.com/EzhovAndrew/kv-db/internal/database/storage/encoders"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/filesystem"
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

func NewFileLogsReader(dataDir string, maxSegmentSize int) *FileLogsReader {
	fileSystem := filesystem.NewSegmentedFileSystem(dataDir, maxSegmentSize)
	return &FileLogsReader{filesystem: fileSystem}
}

func (l *FileLogsReader) Read() iter.Seq2[*Log, error] {
	return func(yield func(*Log, error) bool) {
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

func (l *FileLogsReader) ReadFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*Log, error] {
	return func(yield func(*Log, error) bool) {
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
			if err := l.processData(data, yield); err != nil {
				if !yield(nil, err) {
					return
				}
			}
		}
	}
}

func (l *FileLogsReader) processData(data []byte, yield func(*Log, error) bool) error {
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
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

		if !yield(log, nil) {
			return nil // Consumer wants to stop
		}
	}

	return nil
}

package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"iter"

	"github.com/EzhovAndrew/kv-db/internal/database/storage/filesystem"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/utils"
	"go.uber.org/zap"
)

var (
	ErrDecodeLSN          = errors.New("failed to decode LSN")
	ErrDecodeCmdID        = errors.New("failed to decode command ID")
	ErrDecodeArguments    = errors.New("failed to decode arguments")
	ErrDecodeArgumentsNum = errors.New("failed to decode arguments number")
	ErrDecodeArgumentLen  = errors.New("failed to decode argument length")
	ErrDecodeArgument     = errors.New("failed to decode argument")
)

type FileSystemReader interface {
	ReadAll() iter.Seq2[[]byte, error]
	GetSegmentForLSN(lsn uint64) (int, error)
	ReadContinuouslyFromSegment(segment int) iter.Seq2[[]byte, error]
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
		log, bytesRead, err := l.decodeLog(buf)
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

// decodeLog decodes a single log entry from the reader and returns the log, bytes read, and any error
func (l *FileLogsReader) decodeLog(buf *bytes.Reader) (*Log, int, error) {
	initialPos := buf.Len()
	entry := &Log{}

	lsn, err := binary.ReadUvarint(buf)
	if err != nil {
		if err == io.EOF {
			return nil, 0, err
		}
		logging.Error("Failed to decode LSN", zap.Error(err))
		return nil, 0, ErrDecodeLSN
	}
	entry.LSN = lsn

	cmdID, err := binary.ReadUvarint(buf)
	if err != nil {
		logging.Error("Failed to decode CmdID", zap.Error(err))
		return nil, 0, ErrDecodeCmdID
	}
	entry.Command = int(cmdID)

	numArgs, err := binary.ReadUvarint(buf)
	if err != nil {
		logging.Error("Failed to decode number of arguments", zap.Error(err))
		return nil, 0, ErrDecodeArgumentsNum
	}

	entry.Arguments = make([]string, numArgs)
	for i := uint64(0); i < numArgs; i++ {
		argLen, err := binary.ReadUvarint(buf)
		if err != nil {
			logging.Error("Failed to decode argument length", zap.Error(err))
			return nil, 0, ErrDecodeArgumentLen
		}

		arg := make([]byte, argLen)
		n, err := io.ReadFull(buf, arg)
		if err != nil {
			logging.Error("Failed to read argument data", zap.Error(err))
			return nil, 0, ErrDecodeArgument
		}
		if uint64(n) != argLen {
			logging.Error("Unexpected EOF reading argument data")
			return nil, 0, ErrDecodeArgument
		}
		entry.Arguments[i] = utils.BytesToString(arg)
	}

	bytesRead := initialPos - buf.Len()
	return entry, bytesRead, nil
}

package wal

import (
	"bytes"
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
				return
			}
			for {
				log, err := l.decodeLog(data)
				if err == io.EOF {
					break
				}
				if !yield(log, err) {
					return
				}
			}
		}
	}
}

func (l *FileLogsReader) decodeLog(data []byte) (*Log, error) {
	buf := bytes.NewReader(data)
	entry := &Log{}

	lsn, err := binary.ReadUvarint(buf)
	if err != nil {
		logging.Error("Failed to decode LSN", zap.Error(err))
		return nil, ErrDecodeLSN
	}
	entry.LSN = lsn

	cmdID, err := binary.ReadUvarint(buf)
	if err != nil {
		logging.Error("Failed to decode CmdID", zap.Error(err))
		return nil, ErrDecodeCmdID
	}
	entry.Command = int(cmdID)

	numArgs, err := binary.ReadUvarint(buf)
	if err != nil {
		logging.Error("Failed to decode number of arguments", zap.Error(err))
		return nil, ErrDecodeArgumentsNum
	}

	entry.Arguments = make([]string, numArgs)
	for i := uint64(0); i < numArgs; i++ {
		argLen, err := binary.ReadUvarint(buf)
		if err != nil {
			logging.Error("Failed to decode argument length", zap.Error(err))
			return nil, ErrDecodeArgumentLen
		}
		arg := make([]byte, argLen)
		n, err := io.ReadFull(buf, arg)
		if err != nil {
			logging.Error("Failed to read argument data", zap.Error(err))
			return nil, ErrDecodeArgument
		}
		if uint64(n) != argLen {
			logging.Error("Unexpected EOF reading argument data", zap.Error(err))
			return nil, ErrDecodeArgument
		}
		entry.Arguments[i] = utils.BytesToString(arg)
	}

	return entry, nil
}

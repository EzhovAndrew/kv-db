package encoders

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"go.uber.org/zap"

	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/utils"
)

var (
	ErrDecodeLSN          = errors.New("failed to decode LSN")
	ErrDecodeCmdID        = errors.New("failed to decode command ID")
	ErrDecodeArguments    = errors.New("failed to decode arguments")
	ErrDecodeArgumentsNum = errors.New("failed to decode arguments number")
	ErrDecodeArgumentLen  = errors.New("failed to decode argument length")
	ErrDecodeArgument     = errors.New("failed to decode argument")
)

func GetLastLSNInData(data []byte) (uint64, error) {
	var lastLSN uint64
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		log, bytesRead, err := DecodeLog(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return lastLSN, err
		}
		lastLSN = log.LSN
		if bytesRead == 0 {
			// Prevent infinite loop if no bytes were consumed
			logging.Error("No bytes consumed during log decoding, stopping to prevent infinite loop")
			break
		}
	}

	return lastLSN, nil
}

func DecodeLog(buf *bytes.Reader) (*Log, int, error) {
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

func EncodeLog(log *Log, buf *bytes.Buffer) {
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

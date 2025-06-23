package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"iter"
	"os"
	"testing"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/encoders"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Initialize logging once before all tests
	logging.Init(&configuration.LoggingConfig{})

	// Run all tests
	code := m.Run()

	// Exit with the test result code
	os.Exit(code)
}

func TestNewFileLogsReader(t *testing.T) {
	tests := []struct {
		name           string
		dataDir        string
		maxSegmentSize int
	}{
		{
			name:           "valid configuration",
			dataDir:        "/tmp/wal",
			maxSegmentSize: 1024,
		},
		{
			name:           "small segment size",
			dataDir:        "./data",
			maxSegmentSize: 4096,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewFileLogsReader(tt.dataDir, tt.maxSegmentSize)
			defer os.RemoveAll(tt.dataDir)
			assert.NotNil(t, reader)
			assert.NotNil(t, reader.filesystem)
		})
	}
}

func TestFileLogsReader_Read_ValidLogs(t *testing.T) {
	// Create valid log data
	validLogData := createValidLogData([]*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
		{LSN: 2, Command: compute.DelCommandID, Arguments: []string{"key2"}},
		{LSN: 3, Command: compute.SetCommandID, Arguments: []string{"key3", "value3"}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 3, len(logs))

	// Verify first log
	assert.Equal(t, uint64(1), logs[0].LSN)
	assert.Equal(t, compute.SetCommandID, logs[0].Command)
	assert.Equal(t, []string{"key1", "value1"}, logs[0].Arguments)

	// Verify second log
	assert.Equal(t, uint64(2), logs[1].LSN)
	assert.Equal(t, compute.DelCommandID, logs[1].Command)
	assert.Equal(t, []string{"key2"}, logs[1].Arguments)

	// Verify third log
	assert.Equal(t, uint64(3), logs[2].LSN)
	assert.Equal(t, compute.SetCommandID, logs[2].Command)
	assert.Equal(t, []string{"key3", "value3"}, logs[2].Arguments)
}

func TestFileLogsReader_Read_EmptyData(t *testing.T) {
	mockFS := &mockFileSystemReader{
		data: [][]byte{{}}, // Empty data
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 0, len(logs))
}

func TestFileLogsReader_Read_FileSystemError(t *testing.T) {
	expectedError := errors.New("filesystem read error")
	mockFS := &mockFileSystemReader{
		data:   [][]byte{nil},
		errors: []error{expectedError},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var errorCount int
	for _, err := range reader.Read() {
		if err != nil {
			errorCount++
			assert.Equal(t, expectedError, err)
		}
	}

	assert.Equal(t, 1, errorCount)
}

func TestFileLogsReader_Read_CorruptedLSN(t *testing.T) {
	// Create data with corrupted LSN (incomplete varint)
	corruptedData := []byte{0xFF} // Invalid varint

	mockFS := &mockFileSystemReader{
		data: [][]byte{corruptedData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var errorCount int
	for _, err := range reader.Read() {
		if err != nil {
			errorCount++
			assert.Equal(t, encoders.ErrDecodeLSN, err)
		}
	}

	assert.Equal(t, 1, errorCount)
}

func TestFileLogsReader_Read_CorruptedCommandID(t *testing.T) {
	buf := &bytes.Buffer{}

	// Write valid LSN using helper
	encodeLsn(1, buf)
	// Write incomplete command ID
	buf.WriteByte(0xFF)

	mockFS := &mockFileSystemReader{
		data: [][]byte{buf.Bytes()},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var errorCount int
	for _, err := range reader.Read() {
		if err != nil {
			errorCount++
			assert.Equal(t, encoders.ErrDecodeCmdID, err)
		}
	}

	assert.Equal(t, 1, errorCount)
}

func TestFileLogsReader_Read_CorruptedArgumentsNum(t *testing.T) {
	buf := &bytes.Buffer{}

	// Write valid LSN and command ID using helpers
	encodeLsn(1, buf)
	encodeCMD(compute.SetCommandID, buf)
	// Write incomplete arguments number
	buf.WriteByte(0xFF)

	mockFS := &mockFileSystemReader{
		data: [][]byte{buf.Bytes()},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var errorCount int
	for _, err := range reader.Read() {
		if err != nil {
			errorCount++
			assert.Equal(t, encoders.ErrDecodeArgumentsNum, err)
		}
	}

	assert.Equal(t, 1, errorCount)
}

func TestFileLogsReader_Read_CorruptedArgumentLength(t *testing.T) {
	buf := &bytes.Buffer{}

	// Write valid LSN, command ID, and arguments number using helpers
	encodeLsn(1, buf)
	encodeCMD(compute.SetCommandID, buf)
	encodeArgumentsNum(1, buf) // One argument
	// Write incomplete argument length
	buf.WriteByte(0xFF)

	mockFS := &mockFileSystemReader{
		data: [][]byte{buf.Bytes()},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var errorCount int
	for _, err := range reader.Read() {
		if err != nil {
			errorCount++
			assert.Equal(t, encoders.ErrDecodeArgumentLen, err)
		}
	}

	assert.Equal(t, 1, errorCount)
}

func TestFileLogsReader_Read_CorruptedArgumentData(t *testing.T) {
	buf := &bytes.Buffer{}

	// Write valid LSN, command ID, arguments number using helpers
	encodeLsn(1, buf)
	encodeCMD(compute.SetCommandID, buf)
	encodeArgumentsNum(1, buf) // One argument
	// Write argument length but incomplete data
	var lsnBuf [10]byte
	n := binary.PutUvarint(lsnBuf[:], 10) // Argument length 10
	buf.Write(lsnBuf[:n])
	// Write incomplete argument data (only 5 bytes instead of 10)
	buf.Write([]byte("short"))

	mockFS := &mockFileSystemReader{
		data: [][]byte{buf.Bytes()},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var errorCount int
	for _, err := range reader.Read() {
		if err != nil {
			errorCount++
			assert.Equal(t, encoders.ErrDecodeArgument, err)
		}
	}

	assert.Equal(t, 1, errorCount)
}

func TestFileLogsReader_Read_EOFHandling(t *testing.T) {
	buf := &bytes.Buffer{}

	// Write valid LSN using helper
	encodeLsn(1, buf)
	// Simulate EOF by not writing complete log

	mockFS := &mockFileSystemReader{
		data: [][]byte{buf.Bytes()},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		if err != nil {
			assert.Equal(t, encoders.ErrDecodeCmdID, err)
		} else {
			logs = append(logs, log)
		}
	}

	// Should have no complete logs due to EOF
	assert.Equal(t, 0, len(logs))
}

func TestFileLogsReader_Read_ZeroArguments(t *testing.T) {
	validLogData := createValidLogData([]*Log{
		{LSN: 1, Command: compute.DelCommandID, Arguments: []string{}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 1, len(logs))
	assert.Equal(t, []string{}, logs[0].Arguments)
}

func TestFileLogsReader_Read_SingleArgument(t *testing.T) {
	validLogData := createValidLogData([]*Log{
		{LSN: 1, Command: compute.DelCommandID, Arguments: []string{"key1"}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 1, len(logs))
	assert.Equal(t, []string{"key1"}, logs[0].Arguments)
}

func TestFileLogsReader_Read_MaxUint64LSN(t *testing.T) {
	validLogData := createValidLogData([]*Log{
		{LSN: ^uint64(0), Command: compute.SetCommandID, Arguments: []string{"key", "value"}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 1, len(logs))
	assert.Equal(t, ^uint64(0), logs[0].LSN)
}

func TestFileLogsReader_Read_MixedChunksWithErrors(t *testing.T) {
	// Create valid data chunk
	validChunk := createValidLogData([]*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
	})

	// Create another valid chunk
	validChunk2 := createValidLogData([]*Log{
		{LSN: 2, Command: compute.DelCommandID, Arguments: []string{"key2"}},
	})

	expectedError := errors.New("filesystem error")
	mockFS := &mockFileSystemReader{
		data:   [][]byte{validChunk, nil, validChunk2},
		errors: []error{nil, expectedError, nil},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	var errorCount int
	for log, err := range reader.Read() {
		if err != nil {
			errorCount++
			assert.Equal(t, expectedError, err)
		} else {
			logs = append(logs, log)
		}
	}

	assert.Equal(t, 2, len(logs)) // Should get 2 valid logs
	assert.Equal(t, uint64(1), logs[0].LSN)
	assert.Equal(t, compute.SetCommandID, logs[0].Command)
	assert.Equal(t, []string{"key1", "value1"}, logs[0].Arguments)
	assert.Equal(t, uint64(2), logs[1].LSN)
	assert.Equal(t, compute.DelCommandID, logs[1].Command)
	assert.Equal(t, []string{"key2"}, logs[1].Arguments)
	assert.Equal(t, 1, errorCount) // Should get 1 error
}

func TestFileLogsReader_Read_PartialLogAtEndOfChunk(t *testing.T) {
	buf := &bytes.Buffer{}

	// Write one complete log
	encodeLogHelper(&Log{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}}, buf)

	// Write partial log (only LSN and command)
	encodeLsn(2, buf)
	encodeCMD(compute.SetCommandID, buf)
	// Missing arguments number and arguments

	mockFS := &mockFileSystemReader{
		data: [][]byte{buf.Bytes()},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	var errorCount int
	for log, err := range reader.Read() {
		if err != nil {
			errorCount++
			assert.Equal(t, encoders.ErrDecodeArgumentsNum, err)
		} else {
			logs = append(logs, log)
		}
	}

	assert.Equal(t, 1, len(logs)) // Should get 1 complete log
	assert.Equal(t, uint64(1), logs[0].LSN)
	assert.Equal(t, compute.SetCommandID, logs[0].Command)
	assert.Equal(t, []string{"key1", "value1"}, logs[0].Arguments)
	assert.Equal(t, 1, errorCount) // Should get 1 error for partial log
}

func TestFileLogsReader_Read_MultipleLogsInSingleChunk(t *testing.T) {
	validLogData := createValidLogData([]*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
		{LSN: 2, Command: compute.DelCommandID, Arguments: []string{"key2"}},
		{LSN: 3, Command: compute.SetCommandID, Arguments: []string{"key3", "value3"}},
		{LSN: 4, Command: compute.DelCommandID, Arguments: []string{"key4"}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 4, len(logs))

	// Verify all logs
	expectedLogs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
		{LSN: 2, Command: compute.DelCommandID, Arguments: []string{"key2"}},
		{LSN: 3, Command: compute.SetCommandID, Arguments: []string{"key3", "value3"}},
		{LSN: 4, Command: compute.DelCommandID, Arguments: []string{"key4"}},
	}

	for i, expected := range expectedLogs {
		assert.Equal(t, expected.LSN, logs[i].LSN)
		assert.Equal(t, expected.Command, logs[i].Command)
		assert.Equal(t, expected.Arguments, logs[i].Arguments)
	}
}

func TestFileLogsReader_Read_UnicodeArguments(t *testing.T) {
	unicodeKey := "键值"
	unicodeValue := "数据库🔑💾"

	validLogData := createValidLogData([]*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{unicodeKey, unicodeValue}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 1, len(logs))
	assert.Equal(t, unicodeKey, logs[0].Arguments[0])
	assert.Equal(t, unicodeValue, logs[0].Arguments[1])
}

func TestFileLogsReader_Read_SpecialCharacters(t *testing.T) {
	specialKey := "key\nwith\ttabs\rand\x00null"
	specialValue := "value!@#$%^&*()_+{}|:<>?[]\\;'\",./"

	validLogData := createValidLogData([]*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{specialKey, specialValue}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 1, len(logs))
	assert.Equal(t, specialKey, logs[0].Arguments[0])
	assert.Equal(t, specialValue, logs[0].Arguments[1])
}

func TestFileLogsReader_Read_EmptyArguments(t *testing.T) {
	validLogData := createValidLogData([]*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"", ""}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 1, len(logs))
	assert.Equal(t, []string{"", ""}, logs[0].Arguments)
}

func TestFileLogsReader_Read_LargeArguments(t *testing.T) {
	largeKey := string(make([]byte, 1000))
	largeValue := string(make([]byte, 5000))

	validLogData := createValidLogData([]*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{largeKey, largeValue}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
	}

	assert.Equal(t, 1, len(logs))
	assert.Equal(t, largeKey, logs[0].Arguments[0])
	assert.Equal(t, largeValue, logs[0].Arguments[1])
}

func TestFileLogsReader_Read_EarlyTermination(t *testing.T) {
	validLogData := createValidLogData([]*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
		{LSN: 2, Command: compute.SetCommandID, Arguments: []string{"key2", "value2"}},
		{LSN: 3, Command: compute.SetCommandID, Arguments: []string{"key3", "value3"}},
	})

	mockFS := &mockFileSystemReader{
		data: [][]byte{validLogData},
	}

	reader := &FileLogsReader{filesystem: mockFS}

	var logs []*Log
	count := 0
	for log, err := range reader.Read() {
		require.NoError(t, err)
		logs = append(logs, log)
		count++
		if count == 2 {
			break // Early termination
		}
	}

	// Should only have processed 2 logs due to early termination
	assert.Equal(t, 2, len(logs))
}

// Mock FileSystemReader for testing
type mockFileSystemReader struct {
	data   [][]byte
	errors []error
}

func (m *mockFileSystemReader) ReadAll() iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for i, data := range m.data {
			var err error
			if i < len(m.errors) {
				err = m.errors[i]
			}
			if !yield(data, err) {
				return
			}
		}
	}
}

// Helper function to create valid log data
func encodeLsn(lsn uint64, buf *bytes.Buffer) {
	var lsnBuf [10]byte
	n := binary.PutUvarint(lsnBuf[:], lsn)
	buf.Write(lsnBuf[:n])
}

func encodeCMD(cmd int, buf *bytes.Buffer) {
	var cmdBuf [5]byte
	n := binary.PutUvarint(cmdBuf[:], uint64(cmd))
	buf.Write(cmdBuf[:n])
}

func encodeArgumentsNum(num int, buf *bytes.Buffer) {
	var numBuf [5]byte
	n := binary.PutUvarint(numBuf[:], uint64(num))
	buf.Write(numBuf[:n])
}

func encodeArgument(arg string, buf *bytes.Buffer) {
	var lsnBuf [10]byte
	n := binary.PutUvarint(lsnBuf[:], uint64(len(arg)))
	buf.Write(lsnBuf[:n])
	buf.Write(utils.StringToBytes(arg))
}

func encodeLogHelper(log *Log, buf *bytes.Buffer) {
	encodeLsn(log.LSN, buf)
	encodeCMD(log.Command, buf)
	encodeArgumentsNum(len(log.Arguments), buf)

	for _, arg := range log.Arguments {
		encodeArgument(arg, buf)
	}
}

func createValidLogData(logs []*Log) []byte {
	buf := &bytes.Buffer{}
	for _, log := range logs {
		encodeLogHelper(log, buf)
	}
	return buf.Bytes()
}

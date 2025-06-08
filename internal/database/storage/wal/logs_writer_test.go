package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"testing"

	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFileLogsWriter(t *testing.T) {
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
			writer := NewFileLogsWriter(tt.dataDir, tt.maxSegmentSize)
			defer os.RemoveAll(tt.dataDir)

			assert.NotNil(t, writer)
			assert.NotNil(t, writer.filesystem)
			assert.NotNil(t, writer.buf)
		})
	}
}

func TestFileLogsWriter_Write_EmptyLogs(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	err := writer.Write([]*Log{})

	assert.NoError(t, err)
	assert.Equal(t, 0, len(mockFS.getWrittenData()))
}

func TestFileLogsWriter_Write_SingleLog(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	logs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
	}

	err := writer.Write(logs)

	require.NoError(t, err)
	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 1, len(writtenData))

	// Verify the encoded data
	verifyEncodedLog(t, writtenData[0], logs[0])
}

func TestFileLogsWriter_Write_MultipleLogs(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	logs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
		{LSN: 2, Command: compute.DelCommandID, Arguments: []string{"key2"}},
		{LSN: 3, Command: compute.SetCommandID, Arguments: []string{"key3", "value3"}},
	}

	err := writer.Write(logs)

	require.NoError(t, err)
	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 1, len(writtenData))

	// Verify all logs are encoded in the single write
	verifyEncodedLogs(t, writtenData[0], logs)
}

func TestFileLogsWriter_Write_FileSystemError(t *testing.T) {
	expectedError := errors.New("filesystem write error")
	mockFS := &mockFileSystemWriteSyncer{writeError: expectedError}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	logs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
	}

	err := writer.Write(logs)

	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
}

func TestFileLogsWriter_Write_PanicRecovery(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{panicOnWrite: true}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	logs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
	}

	err := writer.Write(logs)

	assert.Error(t, err)
	assert.Equal(t, "Write logs to disk failed due to panic", err.Error())
}

func TestFileLogsWriter_Write_SpecialCharacters(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	specialKey := "key\nwith\ttabs\rand\x00null"
	specialValue := "value!@#$%^&*()_+{}|:<>?[]\\;'\",./"

	logs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{specialKey, specialValue}},
	}

	err := writer.Write(logs)

	require.NoError(t, err)
	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 1, len(writtenData))

	verifyEncodedLog(t, writtenData[0], logs[0])
}

func TestFileLogsWriter_Write_UnicodeCharacters(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	unicodeKey := "键值"
	unicodeValue := "数据库🔑💾"

	logs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{unicodeKey, unicodeValue}},
	}

	err := writer.Write(logs)

	require.NoError(t, err)
	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 1, len(writtenData))

	verifyEncodedLog(t, writtenData[0], logs[0])
}

func TestFileLogsWriter_Write_EmptyArguments(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	logs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"", ""}},
		{LSN: 2, Command: compute.DelCommandID, Arguments: []string{}},
	}

	err := writer.Write(logs)

	require.NoError(t, err)
	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 1, len(writtenData))

	verifyEncodedLogs(t, writtenData[0], logs)
}

func TestFileLogsWriter_Write_LargeArguments(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	largeKey := string(make([]byte, 1000))
	largeValue := string(make([]byte, 5000))

	logs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{largeKey, largeValue}},
	}

	err := writer.Write(logs)

	require.NoError(t, err)
	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 1, len(writtenData))

	verifyEncodedLog(t, writtenData[0], logs[0])
}

func TestFileLogsWriter_Write_MaxLSN(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	logs := []*Log{
		{LSN: ^uint64(0), Command: compute.SetCommandID, Arguments: []string{"key", "value"}},
	}

	err := writer.Write(logs)

	require.NoError(t, err)
	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 1, len(writtenData))

	verifyEncodedLog(t, writtenData[0], logs[0])
}

func TestFileLogsWriter_Write_BufferReuse(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	// First write
	logs1 := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
	}

	err := writer.Write(logs1)
	require.NoError(t, err)

	// Second write - buffer should be reset
	logs2 := []*Log{
		{LSN: 2, Command: compute.DelCommandID, Arguments: []string{"key2"}},
	}

	err = writer.Write(logs2)
	require.NoError(t, err)

	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 2, len(writtenData))

	// Verify each write contains only its respective logs
	verifyEncodedLog(t, writtenData[0], logs1[0])
	verifyEncodedLog(t, writtenData[1], logs2[0])
}

func TestFileLogsWriter_Write_MixedCommands(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	logs := []*Log{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
		{LSN: 2, Command: compute.DelCommandID, Arguments: []string{"key2"}},
		{LSN: 3, Command: compute.SetCommandID, Arguments: []string{"key3", "value3"}},
		{LSN: 4, Command: compute.DelCommandID, Arguments: []string{"key4"}},
	}

	err := writer.Write(logs)

	require.NoError(t, err)
	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 1, len(writtenData))

	verifyEncodedLogs(t, writtenData[0], logs)
}

// Helper functions for verification

func verifyEncodedLog(t *testing.T, data []byte, expectedLog *Log) {
	buf := bytes.NewReader(data)

	// Decode LSN
	lsn, err := binary.ReadUvarint(buf)
	require.NoError(t, err)
	assert.Equal(t, expectedLog.LSN, lsn)

	// Decode command
	cmd, err := binary.ReadUvarint(buf)
	require.NoError(t, err)
	assert.Equal(t, uint64(expectedLog.Command), cmd)

	// Decode arguments count
	argCount, err := binary.ReadUvarint(buf)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(expectedLog.Arguments)), argCount)

	// Decode arguments
	for i, expectedArg := range expectedLog.Arguments {
		argLen, err := binary.ReadUvarint(buf)
		require.NoError(t, err)
		assert.Equal(t, uint64(len(expectedArg)), argLen)

		argData := make([]byte, argLen)
		n, err := buf.Read(argData)
		require.NoError(t, err)
		assert.Equal(t, int(argLen), n)
		assert.Equal(t, expectedArg, utils.BytesToString(argData), "Argument %d mismatch", i)
	}
}

func verifyEncodedLogs(t *testing.T, data []byte, expectedLogs []*Log) {
	buf := bytes.NewReader(data)

	for i, expectedLog := range expectedLogs {
		// Decode LSN
		lsn, err := binary.ReadUvarint(buf)
		require.NoError(t, err, "Failed to decode LSN for log %d", i)
		assert.Equal(t, expectedLog.LSN, lsn, "LSN mismatch for log %d", i)

		// Decode command
		cmd, err := binary.ReadUvarint(buf)
		require.NoError(t, err, "Failed to decode command for log %d", i)
		assert.Equal(t, uint64(expectedLog.Command), cmd, "Command mismatch for log %d", i)

		// Decode arguments count
		argCount, err := binary.ReadUvarint(buf)
		require.NoError(t, err, "Failed to decode arguments count for log %d", i)
		assert.Equal(t, uint64(len(expectedLog.Arguments)), argCount, "Arguments count mismatch for log %d", i)

		// Decode arguments
		for j, expectedArg := range expectedLog.Arguments {
			argLen, err := binary.ReadUvarint(buf)
			require.NoError(t, err, "Failed to decode argument length for log %d, arg %d", i, j)
			assert.Equal(t, uint64(len(expectedArg)), argLen, "Argument length mismatch for log %d, arg %d", i, j)

			argData := make([]byte, argLen)
			n, err := buf.Read(argData)
			require.NoError(t, err, "Failed to read argument data for log %d, arg %d", i, j)
			assert.Equal(t, int(argLen), n, "Incomplete argument data read for log %d, arg %d", i, j)
			assert.Equal(t, expectedArg, utils.BytesToString(argData), "Argument value mismatch for log %d, arg %d", i, j)
		}
	}

	// Verify no extra data remains
	remaining := buf.Len()
	assert.Equal(t, 0, remaining, "Extra data found after decoding all logs: %d bytes", remaining)
}

// Mock FileSystemWriteSyncer for testing
type mockFileSystemWriteSyncer struct {
	writtenData  [][]byte
	writeError   error
	panicOnWrite bool
}

func (m *mockFileSystemWriteSyncer) WriteSync(data []byte) error {
	if m.panicOnWrite {
		panic("mock filesystem panic")
	}
	if m.writeError != nil {
		return m.writeError
	}
	// Store a copy of the data
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	m.writtenData = append(m.writtenData, dataCopy)
	return nil
}

func (m *mockFileSystemWriteSyncer) getWrittenData() [][]byte {
	return m.writtenData
}

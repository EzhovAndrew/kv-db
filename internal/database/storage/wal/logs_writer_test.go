package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFileLogsWriter(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := NewFileLogsWriter(mockFS)

	assert.NotNil(t, writer)
	assert.NotNil(t, writer.filesystem)
	assert.NotNil(t, writer.buf)

	// Verify buffer is properly initialized and pre-grown
	assert.Equal(t, 0, writer.buf.Len())             // Should be empty initially
	assert.GreaterOrEqual(t, writer.buf.Cap(), 9192) // Should have capacity of at least 9192
}

func TestFileLogsWriter_Write_EmptyLogs(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	err := writer.Write([]*LogEntry{})

	assert.NoError(t, err)
	assert.Equal(t, 0, len(mockFS.getWrittenData()))

	// Verify no filesystem calls were made
	writeCalls := mockFS.getWriteCalls()
	assert.Equal(t, 0, len(writeCalls))

	// Verify buffer remains clean
	assert.Equal(t, 0, writer.buf.Len())
}

func TestFileLogsWriter_Write_NilLogs(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	err := writer.Write(nil)

	assert.NoError(t, err)
	assert.Equal(t, 0, len(mockFS.getWrittenData()))

	// Verify no filesystem calls were made
	writeCalls := mockFS.getWriteCalls()
	assert.Equal(t, 0, len(writeCalls))
}

func TestFileLogsWriter_Write_SingleLog(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	logs := []*LogEntry{
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

	logs := []*LogEntry{
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

	logs := []*LogEntry{
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

	logs := []*LogEntry{
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

	logs := []*LogEntry{
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

	logs := []*LogEntry{
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

	logs := []*LogEntry{
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

	logs := []*LogEntry{
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

	logs := []*LogEntry{
		{LSN: ^uint64(0), Command: compute.SetCommandID, Arguments: []string{"key", "value"}},
	}

	err := writer.Write(logs)

	require.NoError(t, err)
	writtenData := mockFS.getWrittenData()
	assert.Equal(t, 1, len(writtenData))

	verifyEncodedLog(t, writtenData[0], logs[0])
}

func TestFileLogsWriter_Write_MixedCommands(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	logs := []*LogEntry{
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

func TestFileLogsWriter_Write_LSNPassedToFilesystem(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	tests := []struct {
		name        string
		logs        []*LogEntry
		expectedLSN uint64
	}{
		{
			name: "single log with LSN 100",
			logs: []*LogEntry{
				{LSN: 100, Command: compute.SetCommandID, Arguments: []string{"key", "value"}},
			},
			expectedLSN: 100,
		},
		{
			name: "multiple logs - first LSN should be passed",
			logs: []*LogEntry{
				{LSN: 50, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
				{LSN: 51, Command: compute.DelCommandID, Arguments: []string{"key2"}},
				{LSN: 52, Command: compute.SetCommandID, Arguments: []string{"key3", "value3"}},
			},
			expectedLSN: 50,
		},
		{
			name: "max uint64 LSN",
			logs: []*LogEntry{
				{LSN: ^uint64(0), Command: compute.SetCommandID, Arguments: []string{"key", "value"}},
			},
			expectedLSN: ^uint64(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset mock for each test
			mockFS.writtenData = nil
			mockFS.writeError = nil

			err := writer.Write(tt.logs)
			require.NoError(t, err)

			writeCalls := mockFS.getWriteCalls()
			require.Equal(t, 1, len(writeCalls))

			// Verify the LSN was passed correctly
			assert.Equal(t, tt.expectedLSN, writeCalls[0].lsnStart)

			// Verify the data was encoded correctly
			verifyEncodedLogs(t, writeCalls[0].data, tt.logs)
		})
	}
}

func TestFileLogsWriter_Write_NilLogValidation(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	tests := []struct {
		name          string
		logs          []*LogEntry
		expectedError string
	}{
		{
			name: "nil middle log",
			logs: []*LogEntry{
				{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
				nil,
				{LSN: 3, Command: compute.SetCommandID, Arguments: []string{"key3", "value3"}},
			},
			expectedError: "log entry cannot be nil",
		},
		{
			name: "nil last log",
			logs: []*LogEntry{
				{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
				{LSN: 2, Command: compute.DelCommandID, Arguments: []string{"key2"}},
				nil,
			},
			expectedError: "log entry cannot be nil",
		},
		{
			name:          "all nil logs",
			logs:          []*LogEntry{nil, nil, nil},
			expectedError: "log entry cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := writer.Write(tt.logs)

			assert.Error(t, err)
			assert.Equal(t, tt.expectedError, err.Error())

			// Verify no data was written on error
			writeCalls := mockFS.getWriteCalls()
			assert.Equal(t, 0, len(writeCalls))

			// Reset mock for next test
			mockFS.writtenData = nil
		})
	}
}

func TestFileLogsWriter_Write_BufferResetBetweenWrites(t *testing.T) {
	mockFS := &mockFileSystemWriteSyncer{}
	writer := &FileLogsWriter{
		filesystem: mockFS,
		buf:        &bytes.Buffer{},
	}

	// First write
	logs1 := []*LogEntry{
		{LSN: 10, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
	}
	err := writer.Write(logs1)
	require.NoError(t, err)

	// Verify buffer is empty after first write
	assert.Equal(t, 0, writer.buf.Len())

	// Second write with different data
	logs2 := []*LogEntry{
		{LSN: 20, Command: compute.DelCommandID, Arguments: []string{"key2"}},
	}
	err = writer.Write(logs2)
	require.NoError(t, err)

	// Verify buffer is empty after second write
	assert.Equal(t, 0, writer.buf.Len())

	// Verify both writes were made separately
	writeCalls := mockFS.getWriteCalls()
	require.Equal(t, 2, len(writeCalls))

	assert.Equal(t, uint64(10), writeCalls[0].lsnStart)
	assert.Equal(t, uint64(20), writeCalls[1].lsnStart)

	// Verify each write contains only its respective logs
	verifyEncodedLog(t, writeCalls[0].data, logs1[0])
	verifyEncodedLog(t, writeCalls[1].data, logs2[0])
}

// Helper functions for verification

func verifyEncodedLog(t *testing.T, data []byte, expectedLog *LogEntry) {
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

func verifyEncodedLogs(t *testing.T, data []byte, expectedLogs []*LogEntry) {
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
	writtenData  []mockWriteCall
	writeError   error
	panicOnWrite bool
}

type mockWriteCall struct {
	data     []byte
	lsnStart uint64
}

func (m *mockFileSystemWriteSyncer) WriteSync(data []byte, lsnStart uint64) error {
	if m.panicOnWrite {
		panic("mock filesystem panic")
	}
	if m.writeError != nil {
		return m.writeError
	}
	// Store a copy of the data with LSN
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	m.writtenData = append(m.writtenData, mockWriteCall{
		data:     dataCopy,
		lsnStart: lsnStart,
	})
	return nil
}

func (m *mockFileSystemWriteSyncer) getWrittenData() [][]byte {
	result := make([][]byte, len(m.writtenData))
	for i, call := range m.writtenData {
		result[i] = call.data
	}
	return result
}

func (m *mockFileSystemWriteSyncer) getWriteCalls() []mockWriteCall {
	return m.writtenData
}

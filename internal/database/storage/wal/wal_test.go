package wal

import (
	"context"
	"errors"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWAL(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,
		FlushBatchTimeout: 10,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)

	assert.NotNil(t, wal)
	assert.NotNil(t, wal.batch)
	assert.NotNil(t, wal.pendingFutures)
	assert.NotNil(t, wal.lsnGenerator)
	assert.NotNil(t, wal.newLogsChan)
	assert.NotNil(t, wal.shutdownChan)
	assert.NotNil(t, wal.logsWriter)
	assert.NotNil(t, wal.logsReader)
	assert.Equal(t, 0, len(wal.batch))
	assert.Equal(t, 0, len(wal.pendingFutures))

	wal.Shutdown()
}

func TestWAL_Set(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    2,
		FlushBatchTimeout: 10,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	// Replace with mock writer
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	tests := []struct {
		name     string
		key      string
		value    string
		expected uint64
	}{
		{
			name:     "first set operation",
			key:      "key1",
			value:    "value1",
			expected: 1,
		},
		{
			name:     "second set operation",
			key:      "key2",
			value:    "value2",
			expected: 2,
		},
		{
			name:     "set with special characters",
			key:      "key!@#$%",
			value:    "value\nwith\ttabs",
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			future := wal.Set(tt.key, tt.value)
			lsn, err := future.Wait()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, lsn)
		})
	}

	// Allow some time for async processing
	time.Sleep(20 * time.Millisecond)

	// Verify logs were written
	writtenLogs := mockWriter.getWrittenLogs()
	assert.True(t, len(writtenLogs) > 0)
	assert.Equal(t, 3, len(writtenLogs))

	// Check first log
	log := writtenLogs[0]
	assert.Equal(t, uint64(1), log.LSN)
	assert.Equal(t, compute.SetCommandID, log.Command)
	assert.Equal(t, []string{"key1", "value1"}, log.Arguments)
}

func TestWAL_Delete(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    2,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	// Replace with mock writer
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	tests := []struct {
		name     string
		key      string
		expected uint64
	}{
		{
			name:     "first delete operation",
			key:      "key1",
			expected: 1,
		},
		{
			name:     "delete with special characters",
			key:      "key!@#$%",
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			future := wal.Delete(tt.key)
			lsn, err := future.Wait()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, lsn)
		})
	}

	// Verify logs were written
	writtenLogs := mockWriter.getWrittenLogs()
	assert.True(t, len(writtenLogs) > 0)
	assert.Equal(t, 2, len(writtenLogs))

	log := writtenLogs[0]
	assert.Equal(t, uint64(1), log.LSN)
	assert.Equal(t, compute.DelCommandID, log.Command)
	assert.Equal(t, []string{"key1"}, log.Arguments)
}

func TestWAL_BatchFlushing(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    3,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Add operations that should trigger batch flush
	_ = wal.Set("key1", "value1")
	_ = wal.Set("key2", "value2")
	future3 := wal.Set("key3", "value3") // This should trigger flush

	// Wait for all operations to complete
	select {
	case <-future3.Done():
	case <-time.After(20 * time.Millisecond):
		t.Error("Does not flushed for timeout")
	}

	writtenLogs := mockWriter.getWrittenLogs()
	assert.Equal(t, 3, len(writtenLogs))

}

func TestWAL_TimeoutFlushing(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10, // High batch size
		FlushBatchTimeout: 5,  // Low timeout
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Add single operation (won't trigger batch flush)
	future := wal.Set("key1", "value1")
	future.Wait()

	writtenLogs := mockWriter.getWrittenLogs()
	assert.Equal(t, 1, len(writtenLogs))
}

func TestWAL_Recover(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	expectedLogs := []*LogEntry{
		{LSN: 1, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
		{LSN: 2, Command: compute.DelCommandID, Arguments: []string{"key2"}},
		{LSN: 3, Command: compute.SetCommandID, Arguments: []string{"key3", "value3"}},
	}

	mockReader := &mockLogsReader{logs: expectedLogs}

	wal := NewWAL(cfg)
	wal.logsReader = mockReader
	defer wal.Shutdown()

	var recoveredLogs []*LogEntry
	for log, err := range wal.Recover() {
		require.NoError(t, err)
		recoveredLogs = append(recoveredLogs, log)
	}

	assert.Equal(t, len(expectedLogs), len(recoveredLogs))
	for i, expected := range expectedLogs {
		assert.Equal(t, expected.LSN, recoveredLogs[i].LSN)
		assert.Equal(t, expected.Command, recoveredLogs[i].Command)
		assert.Equal(t, expected.Arguments, recoveredLogs[i].Arguments)
	}
}

func TestWAL_RecoverWithError(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	expectedError := errors.New("read error")
	mockReader := &mockLogsReader{readError: expectedError}

	wal := NewWAL(cfg)
	wal.logsReader = mockReader
	defer wal.Shutdown()

	var errorCount int
	for _, err := range wal.Recover() {
		if err != nil {
			errorCount++
			assert.Equal(t, expectedError, err)
		}
	}

	assert.Equal(t, 1, errorCount)
}

func TestWAL_SetLastLSN(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	// Set last LSN to 100
	wal.SetLastLSN(100)

	// Next operation should have LSN 101
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	lsn := wal.Set("key", "value")
	assert.Equal(t, uint64(101), lsn)
}

func TestWAL_ConcurrentOperations(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    100,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	const numGoroutines = 10
	const operationsPerGoroutine = 10

	var wg sync.WaitGroup
	lsnChan := make(chan uint64, numGoroutines*operationsPerGoroutine)

	// Start concurrent operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				if j%2 == 0 {
					future := wal.Set("key", "value")
					lsn, err := future.Wait()
					if err == nil {
						lsnChan <- lsn
					}
				} else {
					future := wal.Delete("key")
					lsn, err := future.Wait()
					if err == nil {
						lsnChan <- lsn
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(lsnChan)

	// Collect all LSNs
	var lsns []uint64
	for lsn := range lsnChan {
		lsns = append(lsns, lsn)
	}

	// Verify we got the expected number of LSNs
	expectedCount := numGoroutines * operationsPerGoroutine
	assert.Equal(t, expectedCount, len(lsns))

	// Verify LSNs are unique and in expected range
	lsnMap := make(map[uint64]bool)
	for _, lsn := range lsns {
		assert.False(t, lsnMap[lsn], "Duplicate LSN found: %d", lsn)
		lsnMap[lsn] = true
		assert.True(t, lsn >= 1 && lsn <= uint64(expectedCount))
	}
}

func TestWAL_ShutdownWithPendingBatch(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,    // High batch size to prevent auto-flush
		FlushBatchTimeout: 10000, // High timeout to prevent timeout flush
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Add operations without triggering flush
	future1 := wal.Set("key1", "value1")
	future2 := wal.Set("key2", "value2")

	// Shutdown should flush pending batch
	wal.Shutdown()

	// Wait for futures to complete (they should succeed because of shutdown flush)
	future1.Wait()
	future2.Wait()

	// Allow time for shutdown processing
	time.Sleep(50 * time.Millisecond)

	// Verify pending logs were written during shutdown
	writtenLogs := mockWriter.getWrittenLogs()
	assert.Equal(t, 2, len(writtenLogs))
}

func TestWAL_EmptyBatchFlush(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    2,
		FlushBatchTimeout: 50, // Short timeout
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Wait for timeout without adding any operations
	time.Sleep(100 * time.Millisecond)

	// No logs should be written for empty batch
	writtenLogs := mockWriter.getWrittenLogs()
	assert.Equal(t, 0, len(writtenLogs))
	assert.False(t, mockWriter.writeCalled)
}

func TestWAL_RecoverEmptyLogs(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	// Mock reader with no logs
	mockReader := &mockLogsReader{logs: []*LogEntry{}}

	wal := NewWAL(cfg)
	wal.logsReader = mockReader
	defer wal.Shutdown()

	var recoveredLogs []*LogEntry
	for log, err := range wal.Recover() {
		require.NoError(t, err)
		recoveredLogs = append(recoveredLogs, log)
	}

	assert.Equal(t, 0, len(recoveredLogs))
}

func TestWAL_SetLastLSNToZero(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	// Set last LSN to 0
	wal.SetLastLSN(0)

	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Next operation should have LSN 1
	future := wal.Set("key", "value")
	lsn, err := future.Wait()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), lsn)
}

func TestWAL_MultipleShutdowns(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)

	// Multiple shutdowns should not panic
	wal.Shutdown()
	wal.Shutdown() // Second shutdown should be safe
}

func TestWAL_LargeArgumentsInLogs(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    1,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Test with large key and value
	largeKey := string(make([]byte, 500))
	largeValue := string(make([]byte, 1000))

	future := wal.Set(largeKey, largeValue)
	lsn, err := future.Wait()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), lsn)

	// Allow time for processing
	time.Sleep(50 * time.Millisecond)

	writtenLogs := mockWriter.getWrittenLogs()
	assert.Equal(t, 1, len(writtenLogs))
	assert.Equal(t, largeKey, writtenLogs[0].Arguments[0])
	assert.Equal(t, largeValue, writtenLogs[0].Arguments[1])
}

func TestWAL_MixedOperations(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    5,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Mix of set and delete operations
	future1 := wal.Set("key1", "value1")
	future2 := wal.Delete("key2")
	future3 := wal.Set("key3", "value3")
	future4 := wal.Delete("key4")
	future5 := wal.Set("key5", "value5") // Should trigger flush

	// Wait for all operations and verify LSN sequence
	lsn1, err1 := future1.Wait()
	assert.NoError(t, err1)
	assert.Equal(t, uint64(1), lsn1)

	lsn2, err2 := future2.Wait()
	assert.NoError(t, err2)
	assert.Equal(t, uint64(2), lsn2)

	lsn3, err3 := future3.Wait()
	assert.NoError(t, err3)
	assert.Equal(t, uint64(3), lsn3)

	lsn4, err4 := future4.Wait()
	assert.NoError(t, err4)
	assert.Equal(t, uint64(4), lsn4)

	lsn5, err5 := future5.Wait()
	assert.NoError(t, err5)
	assert.Equal(t, uint64(5), lsn5)

	// Allow time for processing
	time.Sleep(50 * time.Millisecond)

	writtenLogs := mockWriter.getWrittenLogs()
	assert.Equal(t, 5, len(writtenLogs))

	// Verify command types
	assert.Equal(t, compute.SetCommandID, writtenLogs[0].Command)
	assert.Equal(t, compute.DelCommandID, writtenLogs[1].Command)
	assert.Equal(t, compute.SetCommandID, writtenLogs[2].Command)
	assert.Equal(t, compute.DelCommandID, writtenLogs[3].Command)
	assert.Equal(t, compute.SetCommandID, writtenLogs[4].Command)
}

// CRITICAL BUG TESTS - Testing the fixes made to WAL implementation

// Test that flush errors don't send responses to clients (CRITICAL FIX)
func TestWAL_FlushErrorHandling(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    2,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	// Create a mock writer that fails
	mockWriter := &mockLogsWriter{writeError: errors.New("disk full")}
	wal.logsWriter = mockWriter

	// Test that operations return 0 when flush fails
	done := make(chan bool, 2)

	go func() {
		future := wal.Set("key1", "value1")
		lsn, err := future.Wait()
		assert.Error(t, err)            // Should get error on failure
		assert.Equal(t, uint64(0), lsn) // LSN should be 0 on failure
		done <- true
	}()

	go func() {
		future := wal.Set("key2", "value2") // This will trigger flush
		lsn, err := future.Wait()
		assert.Error(t, err)            // Should get error on failure
		assert.Equal(t, uint64(0), lsn) // LSN should be 0 on failure
		done <- true
	}()

	// Wait for both operations to complete
	timeout := time.After(5 * time.Second)
	completedOps := 0
	for completedOps < 2 {
		select {
		case <-done:
			completedOps++
		case <-timeout:
			t.Fatal("Operations didn't complete - they should have returned 0 on flush error")
		}
	}

	// Verify write was attempted but failed
	assert.True(t, mockWriter.writeCalled)
	assert.Equal(t, 0, len(mockWriter.getWrittenLogs())) // No logs should be written on error
}

// Test shutdown race condition fix (CRITICAL FIX)
func TestWAL_ShutdownRaceCondition(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)

	// Start multiple goroutines calling shutdown simultaneously
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wal.Shutdown() // Should not panic
		}()
	}

	wg.Wait()
	// If we get here without panic, the race condition is fixed
	assert.True(t, wal.closed)
}

// Test that Set/Delete operations don't block during shutdown (CRITICAL FIX)
func TestWAL_OperationsDuringShutdown(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    10,
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Start operations that would normally block
	done := make(chan bool, 2)

	go func() {
		future := wal.Set("key1", "value1")
		lsn, err := future.Wait()
		// Should get error because of shutdown
		assert.Error(t, err)
		assert.Equal(t, uint64(0), lsn)
		done <- true
	}()

	go func() {
		future := wal.Delete("key2")
		lsn, err := future.Wait()
		// Should get error because of shutdown
		assert.Error(t, err)
		assert.Equal(t, uint64(0), lsn)
		done <- true
	}()

	// Shutdown immediately
	time.Sleep(10 * time.Millisecond) // Give operations time to start
	wal.Shutdown()

	// Operations should complete quickly and not block
	timeout := time.After(1 * time.Second)
	completedOps := 0
	for completedOps < 2 {
		select {
		case <-done:
			completedOps++
		case <-timeout:
			t.Fatal("Operations are blocking during shutdown")
		}
	}
}

// Test WriteLogs maintains single-writer guarantee (CRITICAL FIX)
func TestWAL_WriteLogsUsesNormalBatching(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    1, // Small batch size for immediate flushing
		FlushBatchTimeout: 1000,
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Create logs for replication
	logs := []*LogEntry{
		{LSN: 100, Command: compute.SetCommandID, Arguments: []string{"key1", "value1"}},
		{LSN: 101, Command: compute.SetCommandID, Arguments: []string{"key2", "value2"}},
	}

	// WriteLogs should use normal batching mechanism
	err := wal.WriteLogs(logs)
	assert.NoError(t, err)

	// Allow time for processing
	time.Sleep(100 * time.Millisecond)

	// Verify logs were written through normal mechanism
	writtenLogs := mockWriter.getWrittenLogs()
	assert.Equal(t, 2, len(writtenLogs))
	assert.Equal(t, uint64(100), writtenLogs[0].LSN)
	assert.Equal(t, uint64(101), writtenLogs[1].LSN)
}

// Mock implementations for testing
type mockLogsWriter struct {
	writeCalled bool
	writeError  error
	writtenLogs []*LogEntry
	mu          sync.Mutex
}

func (m *mockLogsWriter) Write(logs []*LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCalled = true
	if m.writeError != nil {
		return m.writeError
	}
	m.writtenLogs = append(m.writtenLogs, logs...)
	return nil
}

func (m *mockLogsWriter) getWrittenLogs() []*LogEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*LogEntry, len(m.writtenLogs))
	copy(result, m.writtenLogs)
	return result
}

func (m *mockLogsWriter) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCalled = false
	m.writeError = nil
	m.writtenLogs = nil
}

type mockLogsReader struct {
	logs      []*LogEntry
	readError error
}

func (m *mockLogsReader) Read() iter.Seq2[*LogEntry, error] {
	return func(yield func(*LogEntry, error) bool) {
		if m.readError != nil {
			yield(nil, m.readError)
			return
		}
		for _, log := range m.logs {
			if !yield(log, nil) {
				return
			}
		}
	}
}

func (m *mockLogsReader) ReadFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*LogEntry, error] {
	return func(yield func(*LogEntry, error) bool) {
		if m.readError != nil {
			yield(nil, m.readError)
			return
		}
		for _, log := range m.logs {
			if log.LSN >= lsn {
				if !yield(log, nil) {
					return
				}
			}
		}
	}
}

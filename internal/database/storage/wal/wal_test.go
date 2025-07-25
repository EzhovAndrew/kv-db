package wal

import (
	"context"
	"errors"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
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
			result := future.Wait()
			lsn, err := result.LSN(), result.Error()
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
			result := future.Wait()
			lsn, err := result.LSN(), result.Error()
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

	future := wal.Set("key", "value")
	result := future.Wait()
	lsn, err := result.LSN(), result.Error()
	assert.NoError(t, err)
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
	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := range operationsPerGoroutine {
				if j%2 == 0 {
					future := wal.Set("key", "value")
					result := future.Wait()
					lsn, err := result.LSN(), result.Error()
					if err == nil {
						lsnChan <- lsn
					}
				} else {
					future := wal.Delete("key")
					result := future.Wait()
					lsn, err := result.LSN(), result.Error()
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
	assert.Equal(t, compute.SetCommandID, writtenLogs[0].Command)
	assert.Equal(t, compute.SetCommandID, writtenLogs[1].Command)
	assert.Equal(t, []string{"key1", "value1"}, writtenLogs[0].Arguments)
	assert.Equal(t, []string{"key2", "value2"}, writtenLogs[1].Arguments)
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
	result := future.Wait()
	lsn, err := result.LSN(), result.Error()
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
	result := future.Wait()
	lsn, err := result.LSN(), result.Error()
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
	result1 := future1.Wait()
	assert.NoError(t, result1.Error())
	assert.Equal(t, uint64(1), result1.LSN())

	result2 := future2.Wait()
	assert.NoError(t, result2.Error())
	assert.Equal(t, uint64(2), result2.LSN())

	result3 := future3.Wait()
	assert.NoError(t, result3.Error())
	assert.Equal(t, uint64(3), result3.LSN())

	result4 := future4.Wait()
	assert.NoError(t, result4.Error())
	assert.Equal(t, uint64(4), result4.LSN())

	result5 := future5.Wait()
	assert.NoError(t, result5.Error())
	assert.Equal(t, uint64(5), result5.LSN())

	// Allow time for processing
	time.Sleep(50 * time.Millisecond)

	writtenLogs := mockWriter.getWrittenLogs()
	assert.Equal(t, 5, len(writtenLogs))

	// Verify command types
	assert.Equal(t, compute.SetCommandID, writtenLogs[0].Command)
	assert.Equal(t, []string{"key1", "value1"}, writtenLogs[0].Arguments)
	assert.Equal(t, compute.DelCommandID, writtenLogs[1].Command)
	assert.Equal(t, []string{"key2"}, writtenLogs[1].Arguments)
	assert.Equal(t, compute.SetCommandID, writtenLogs[2].Command)
	assert.Equal(t, []string{"key3", "value3"}, writtenLogs[2].Arguments)
	assert.Equal(t, compute.DelCommandID, writtenLogs[3].Command)
	assert.Equal(t, []string{"key4"}, writtenLogs[3].Arguments)
	assert.Equal(t, compute.SetCommandID, writtenLogs[4].Command)
	assert.Equal(t, []string{"key5", "value5"}, writtenLogs[4].Arguments)
}

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
		result := future.Wait()
		lsn, err := result.LSN(), result.Error()
		assert.Error(t, err) // Should get error on failure
		assert.Equal(t, uint64(0), lsn)
		done <- true
	}()

	go func() {
		future := wal.Set("key2", "value2") // This will trigger flush
		result := future.Wait()
		lsn, err := result.LSN(), result.Error()
		assert.Error(t, err) // Should get error on failure
		assert.Equal(t, uint64(0), lsn)
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
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wal.Shutdown() // Should not panic
		}()
	}

	wg.Wait()
	assert.True(t, wal.closed)
}

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
		result := future.Wait()
		// Should be flushed during shutdown
		assert.NoError(t, result.Error())
		done <- true
	}()

	go func() {
		future := wal.Delete("key2")
		result := future.Wait()
		// Should be flushed during shutdown
		assert.NoError(t, result.Error())
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

	// After shutdown, Set and Delete should return error immediately
	future := wal.Set("key3", "value3")
	result := future.Wait()
	lsn, err := result.LSN(), result.Error()
	assert.Error(t, err, ErrWALShuttingDown)
	assert.Zero(t, lsn, "LSN should be zero on error")

	future = wal.Delete("key4")
	result = future.Wait()
	assert.Error(t, result.Error(), ErrWALShuttingDown)
	assert.Zero(t, result.LSN(), "LSN should be zero on error")
}

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

// Test single-writer guarantee
func TestWAL_WriteLogsAndSetDelAreBatchedTogether(t *testing.T) {
	cfg := &configuration.WALConfig{
		FlushBatchSize:    5,
		FlushBatchTimeout: 1000000, // Large timeout to avoid auto-flush
		MaxSegmentSize:    1024,
		DataDirectory:     t.TempDir(),
	}

	wal := NewWAL(cfg)
	defer wal.Shutdown()

	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Prepare 2 logs for WriteLogs
	writeLogs := []*LogEntry{
		{LSN: 200, Command: compute.SetCommandID, Arguments: []string{"k1", "v1"}},
		{LSN: 201, Command: compute.SetCommandID, Arguments: []string{"k2", "v2"}},
	}

	// Call WriteLogs (should be batched)
	err := wal.WriteLogs(writeLogs)
	assert.NoError(t, err)

	// Now add 3 more operations via Set/Del
	f1 := wal.Set("k3", "v3")
	f2 := wal.Set("k4", "v4")
	f3 := wal.Delete("k5")

	// Wait for all futures to complete (should only complete after batch flush)
	result1 := f1.Wait()
	result2 := f2.Wait()
	result3 := f3.Wait()
	assert.NoError(t, result1.Error())
	assert.NoError(t, result2.Error())
	assert.NoError(t, result3.Error())

	// Allow time for processing
	time.Sleep(100 * time.Millisecond)

	// All 5 logs should be flushed as a single batch
	writtenLogs := mockWriter.getWrittenLogs()
	assert.Equal(t, 5, len(writtenLogs), "All 5 logs should be flushed together")

	// The first two logs should be the ones from WriteLogs
	assert.Equal(t, uint64(200), writtenLogs[0].LSN)
	assert.Equal(t, uint64(201), writtenLogs[1].LSN)

	// The next three should be Set/Set/Del, in order
	assert.Equal(t, compute.SetCommandID, writtenLogs[2].Command)
	assert.Equal(t, "k3", writtenLogs[2].Arguments[0])
	assert.Equal(t, compute.SetCommandID, writtenLogs[3].Command)
	assert.Equal(t, "k4", writtenLogs[3].Arguments[0])
	assert.Equal(t, compute.DelCommandID, writtenLogs[4].Command)
	assert.Equal(t, "k5", writtenLogs[4].Arguments[0])
}

func TestWAL_Counter_SameKeyGetsTotalCount(t *testing.T) {
	tempDir := t.TempDir()

	config := &configuration.WALConfig{
		DataDirectory:     tempDir,
		MaxSegmentSize:    1024 * 1024,
		FlushBatchSize:    5,
		FlushBatchTimeout: 100000,
	}

	wal := NewWAL(config)
	defer wal.Shutdown()

	// Use mock writer to control when flush happens
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Same key multiple times in one batch
	future1 := wal.Set("same_key", "value1")
	future2 := wal.Set("same_key", "value2")
	future3 := wal.Set("same_key", "value3")
	future4 := wal.Delete("same_key")

	// Different key
	future5 := wal.Set("other_key", "value5")

	// Get results (now they should all be in the same batch)
	result1 := future1.Wait()
	result2 := future2.Wait()
	result3 := future3.Wait()
	result4 := future4.Wait()
	result5 := future5.Wait()

	// Verify no errors
	assert.NoError(t, result1.Error())
	assert.NoError(t, result2.Error())
	assert.NoError(t, result3.Error())
	assert.NoError(t, result4.Error())
	assert.NoError(t, result5.Error())

	// All same_key operations should have count=4 (total same_key operations)
	assert.Equal(t, 4, result1.Count(), "All same_key operations should have count=4")
	assert.Equal(t, 4, result2.Count(), "All same_key operations should have count=4")
	assert.Equal(t, 4, result3.Count(), "All same_key operations should have count=4")
	assert.Equal(t, 4, result4.Count(), "All same_key operations should have count=4")

	// other_key operation should have count=1
	assert.Equal(t, 1, result5.Count(), "other_key operation should have count=1")

	// Verify LSNs are sequential
	assert.Equal(t, uint64(1), result1.LSN())
	assert.Equal(t, uint64(2), result2.LSN())
	assert.Equal(t, uint64(3), result3.LSN())
	assert.Equal(t, uint64(4), result4.LSN())
	assert.Equal(t, uint64(5), result5.LSN())
}

func TestWAL_Counter_CounterResetsBetweenBatches(t *testing.T) {
	tempDir := t.TempDir()

	config := &configuration.WALConfig{
		DataDirectory:     tempDir,
		MaxSegmentSize:    1024 * 1024,
		FlushBatchSize:    3,
		FlushBatchTimeout: 100000,
	}

	wal := NewWAL(config)
	defer wal.Shutdown()

	// Use mock writer to control when flush happens
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// First batch: 2 operations on same key + 1 filler to trigger flush
	future1 := wal.Set("test_key", "value1")
	future2 := wal.Set("test_key", "value2")
	fillerFuture1 := wal.Set("filler1", "filler_value") // Should trigger flush

	result1 := future1.Wait()
	result2 := future2.Wait()
	fillerResult1 := fillerFuture1.Wait()

	// Both test_key operations should have count=2 (total test_key in first batch)
	assert.NoError(t, result1.Error())
	assert.NoError(t, result2.Error())
	assert.NoError(t, fillerResult1.Error())
	assert.Equal(t, 2, result1.Count(), "First batch: test_key operations should have count=2")
	assert.Equal(t, 2, result2.Count(), "First batch: test_key operations should have count=2")
	assert.Equal(t, 1, fillerResult1.Count(), "First batch: filler operation should have count=1")

	// Second batch: 3 operations on same key (exactly fills batch)
	future3 := wal.Set("test_key", "value3")
	future4 := wal.Set("test_key", "value4")
	future5 := wal.Set("test_key", "value5") // Should trigger flush

	result3 := future3.Wait()
	result4 := future4.Wait()
	result5 := future5.Wait()

	// All should have count=3 (total test_key in second batch, counter was reset)
	assert.NoError(t, result3.Error())
	assert.NoError(t, result4.Error())
	assert.NoError(t, result5.Error())
	assert.Equal(t, 3, result3.Count(), "Second batch: all test_key operations should have count=3")
	assert.Equal(t, 3, result4.Count(), "Second batch: all test_key operations should have count=3")
	assert.Equal(t, 3, result5.Count(), "Second batch: all test_key operations should have count=3")
}

func TestWAL_Counter_MultipleKeysInSameBatch(t *testing.T) {
	tempDir := t.TempDir()

	config := &configuration.WALConfig{
		DataDirectory:     tempDir,
		MaxSegmentSize:    1024 * 1024,
		FlushBatchSize:    6,
		FlushBatchTimeout: 100000,
	}

	wal := NewWAL(config)
	defer wal.Shutdown()

	// Use mock writer to control when flush happens
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Operations on key_a (3 times)
	futureA1 := wal.Set("key_a", "value1")
	futureA2 := wal.Set("key_a", "value2")
	futureA3 := wal.Delete("key_a")

	// Operations on key_b (2 times)
	futureB1 := wal.Set("key_b", "value1")
	futureB2 := wal.Set("key_b", "value2")

	// Operations on key_c (1 time)
	futureC1 := wal.Set("key_c", "value1")

	// Get all results
	resultA1 := futureA1.Wait()
	resultA2 := futureA2.Wait()
	resultA3 := futureA3.Wait()
	resultB1 := futureB1.Wait()
	resultB2 := futureB2.Wait()
	resultC1 := futureC1.Wait()

	// Verify no errors
	assert.NoError(t, resultA1.Error())
	assert.NoError(t, resultA2.Error())
	assert.NoError(t, resultA3.Error())
	assert.NoError(t, resultB1.Error())
	assert.NoError(t, resultB2.Error())
	assert.NoError(t, resultC1.Error())

	// All key_a operations should have count=3
	assert.Equal(t, 3, resultA1.Count(), "All key_a operations should have count=3")
	assert.Equal(t, 3, resultA2.Count(), "All key_a operations should have count=3")
	assert.Equal(t, 3, resultA3.Count(), "All key_a operations should have count=3")

	// All key_b operations should have count=2
	assert.Equal(t, 2, resultB1.Count(), "All key_b operations should have count=2")
	assert.Equal(t, 2, resultB2.Count(), "All key_b operations should have count=2")

	// key_c operation should have count=1
	assert.Equal(t, 1, resultC1.Count(), "key_c operation should have count=1")
}

func TestWAL_Counter_ErrorCase(t *testing.T) {
	tempDir := t.TempDir()

	config := &configuration.WALConfig{
		DataDirectory:     tempDir,
		MaxSegmentSize:    1024 * 1024,
		FlushBatchSize:    2,
		FlushBatchTimeout: 1000000,
	}

	wal := NewWAL(config)
	defer wal.Shutdown()

	// Replace with failing mock writer
	mockWriter := &mockLogsWriter{writeError: assert.AnError}
	wal.logsWriter = mockWriter

	// Operations that will fail during flush
	future1 := wal.Set("error_key", "value1")
	future2 := wal.Set("error_key", "value2") // Should trigger flush and fail

	result1 := future1.Wait()
	result2 := future2.Wait()

	// Both should have errors but still correct counts
	assert.Error(t, result1.Error())
	assert.Error(t, result2.Error())
	assert.Equal(t, uint64(0), result1.LSN(), "LSN should be 0 on error")
	assert.Equal(t, uint64(0), result2.LSN(), "LSN should be 0 on error")

	// Count should still be correct even on error
	assert.Equal(t, 2, result1.Count(), "Count should be correct even on error")
	assert.Equal(t, 2, result2.Count(), "Count should be correct even on error")
}

func TestWAL_Counter_EmptyKey(t *testing.T) {
	tempDir := t.TempDir()

	config := &configuration.WALConfig{
		DataDirectory:     tempDir,
		MaxSegmentSize:    1024 * 1024,
		FlushBatchSize:    4,
		FlushBatchTimeout: 100000,
	}

	wal := NewWAL(config)
	defer wal.Shutdown()

	// Use mock writer to control when flush happens
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Operations with empty key (edge case)
	future1 := wal.Set("", "value1")
	future2 := wal.Set("", "value2")
	future3 := wal.Delete("")

	// Operations with non-empty key
	future4 := wal.Set("normal_key", "value")

	result1 := future1.Wait()
	result2 := future2.Wait()
	result3 := future3.Wait()
	result4 := future4.Wait()

	// Verify no errors
	assert.NoError(t, result1.Error())
	assert.NoError(t, result2.Error())
	assert.NoError(t, result3.Error())
	assert.NoError(t, result4.Error())

	// Empty key operations should have count=3
	assert.Equal(t, 3, result1.Count(), "Empty key operations should have count=3")
	assert.Equal(t, 3, result2.Count(), "Empty key operations should have count=3")
	assert.Equal(t, 3, result3.Count(), "Empty key operations should have count=3")

	// Normal key operation should have count=1
	assert.Equal(t, 1, result4.Count(), "Normal key operation should have count=1")
}

func TestWAL_Counter_WithTimeoutFlush(t *testing.T) {
	tempDir := t.TempDir()

	config := &configuration.WALConfig{
		DataDirectory:     tempDir,
		MaxSegmentSize:    1024 * 1024,
		FlushBatchSize:    10,
		FlushBatchTimeout: 100000,
	}

	wal := NewWAL(config)
	defer wal.Shutdown()

	// Use mock writer to control when flush happens
	mockWriter := &mockLogsWriter{}
	wal.logsWriter = mockWriter

	// Add operations that will be flushed together when batch is full
	future1 := wal.Set("timeout_key", "value1")
	future2 := wal.Set("timeout_key", "value2")
	future3 := wal.Set("other_key", "value3") // This triggers flush

	result1 := future1.Wait()
	result2 := future2.Wait()
	result3 := future3.Wait()

	// Verify no errors
	assert.NoError(t, result1.Error())
	assert.NoError(t, result2.Error())
	assert.NoError(t, result3.Error())

	// timeout_key operations should have count=2
	assert.Equal(t, 2, result1.Count(), "timeout_key operations should have count=2")
	assert.Equal(t, 2, result2.Count(), "timeout_key operations should have count=2")

	// other_key operation should have count=1
	assert.Equal(t, 1, result3.Count(), "other_key operation should have count=1")

	// Verify LSNs are correct
	assert.Equal(t, uint64(1), result1.LSN())
	assert.Equal(t, uint64(2), result2.LSN())
	assert.Equal(t, uint64(3), result3.LSN())
}

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

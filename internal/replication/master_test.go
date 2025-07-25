package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/wal"
	"github.com/EzhovAndrew/kv-db/internal/logging"
)

// Mock implementations for testing

type mockLogsReader struct {
	logs   []*wal.LogEntry
	errors []error
	ctx    context.Context
}

func (m *mockLogsReader) ReadLogsFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*wal.LogEntry, error] {
	m.ctx = ctx
	return func(yield func(*wal.LogEntry, error) bool) {
		// Filter logs by LSN
		for i, log := range m.logs {
			if log.LSN > lsn {
				// Return error if available
				var err error
				if i < len(m.errors) {
					err = m.errors[i]
				}

				select {
				case <-ctx.Done():
					return
				default:
					if !yield(log, err) {
						return
					}
				}
			}
		}
	}
}

// Helper functions for tests

func init() {
	// Initialize logging for tests to prevent nil pointer panics
	loggingCfg := &configuration.LoggingConfig{
		Level:  "info",
		Output: "stdout",
	}
	logging.Init(loggingCfg)
}

func createTestReplicationManager() *ReplicationManager {
	cfg := &configuration.Config{
		Network: configuration.NetworkConfig{
			MaxMessageSize: 1024,
			IdleTimeout:    30,
		},
		Replication: &configuration.ReplicationConfig{
			Role: "master",
		},
	}
	return NewReplicationManager(cfg)
}

func createTestSlaveConnection(id string, lsn uint64) *SlaveConnection {
	return &SlaveConnection{
		ID:      id,
		LastLSN: lsn,
	}
}

func createTestWALLogEntry(lsn uint64, command int, args []string) *wal.LogEntry {
	return &wal.LogEntry{
		LSN:       lsn,
		Command:   command,
		Arguments: args,
	}
}

// Tests for parseSlaveHandshake

func TestParseSlaveHandshake_ValidWithSlaveID(t *testing.T) {
	rm := createTestReplicationManager()

	lastLSN := uint64(100)
	handshake := LSNSyncMessage{
		Type:    "lsn_sync",
		LastLSN: &lastLSN,
		SlaveID: "test-slave-123",
	}
	data, _ := json.Marshal(handshake)

	slave, err := rm.parseSlaveHandshake(data)

	require.NoError(t, err)
	assert.Equal(t, "test-slave-123", slave.ID)
	assert.Equal(t, uint64(100), slave.LastLSN)
}

func TestParseSlaveHandshake_ValidWithoutSlaveID(t *testing.T) {
	rm := createTestReplicationManager()

	lastLSN := uint64(200)
	handshake := LSNSyncMessage{
		Type:    "lsn_sync",
		LastLSN: &lastLSN,
	}
	data, _ := json.Marshal(handshake)

	slave, err := rm.parseSlaveHandshake(data)

	require.NoError(t, err)
	assert.Contains(t, slave.ID, "slave-") // Generated ID should contain "slave-"
	assert.Contains(t, slave.ID, "-200")   // Should contain LSN
	assert.Equal(t, uint64(200), slave.LastLSN)
}

func TestParseSlaveHandshake_InvalidJSON(t *testing.T) {
	rm := createTestReplicationManager()

	invalidData := []byte("{invalid json")

	slave, err := rm.parseSlaveHandshake(invalidData)

	assert.Nil(t, slave)
	assert.ErrorIs(t, err, ErrInvalidLSNSyncMsg)
}

func TestParseSlaveHandshake_WrongMessageType(t *testing.T) {
	rm := createTestReplicationManager()

	lastLSN := uint64(100)
	handshake := LSNSyncMessage{
		Type:    "wrong_type",
		LastLSN: &lastLSN,
	}
	data, _ := json.Marshal(handshake)

	slave, err := rm.parseSlaveHandshake(data)

	assert.Nil(t, slave)
	assert.ErrorIs(t, err, ErrExpectedLSNSyncMsg)
}

func TestParseSlaveHandshake_InvalidLastLSNType(t *testing.T) {
	rm := createTestReplicationManager()

	invalidData := []byte(`{"type": "lsn_sync", "last_lsn": "not-a-number"}`)

	slave, err := rm.parseSlaveHandshake(invalidData)

	assert.Nil(t, slave)
	assert.ErrorIs(t, err, ErrInvalidLSNSyncMsg)
}

func TestParseSlaveHandshake_MissingLastLSN(t *testing.T) {
	rm := createTestReplicationManager()

	// Test with missing last_lsn field
	invalidData := []byte(`{"type": "lsn_sync"}`)

	slave, err := rm.parseSlaveHandshake(invalidData)

	assert.Nil(t, slave)
	assert.ErrorIs(t, err, ErrInvalidLastLSN)
}

// Tests for extractOrGenerateSlaveID

func TestExtractOrGenerateSlaveID_ExistingID(t *testing.T) {
	rm := createTestReplicationManager()

	id := rm.extractOrGenerateSlaveID("existing-slave-id", 100)

	assert.Equal(t, "existing-slave-id", id)
}

func TestExtractOrGenerateSlaveID_EmptyID(t *testing.T) {
	rm := createTestReplicationManager()

	id := rm.extractOrGenerateSlaveID("", 150)

	assert.Contains(t, id, "slave-")
	assert.Contains(t, id, "-150")
}

func TestExtractOrGenerateSlaveID_MissingID(t *testing.T) {
	rm := createTestReplicationManager()

	id := rm.extractOrGenerateSlaveID("", 200)

	assert.Contains(t, id, "slave-")
	assert.Contains(t, id, "-200")
}

// Tests for registerSlave and removeSlave

func TestRegisterSlave_NewSlave(t *testing.T) {
	rm := createTestReplicationManager()
	masterState := &MasterState{
		slaves: make(map[string]*SlaveConnection),
	}
	slave := createTestSlaveConnection("test-slave", 100)

	rm.registerSlave(masterState, slave)

	assert.Len(t, masterState.slaves, 1)
	assert.Equal(t, slave, masterState.slaves["test-slave"])
}

func TestRegisterSlave_ConcurrentAccess(t *testing.T) {
	rm := createTestReplicationManager()
	masterState := &MasterState{
		slaves: make(map[string]*SlaveConnection),
	}

	// Register multiple slaves concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			slave := createTestSlaveConnection(fmt.Sprintf("slave-%d", id), uint64(id*10))
			rm.registerSlave(masterState, slave)
		}(i)
	}

	wg.Wait()

	assert.Len(t, masterState.slaves, 10)
}

func TestRemoveSlave_ExistingSlave(t *testing.T) {
	rm := createTestReplicationManager()
	masterState := &MasterState{
		slaves: map[string]*SlaveConnection{
			"test-slave": createTestSlaveConnection("test-slave", 100),
		},
	}

	rm.removeSlave(masterState, "test-slave")

	assert.Len(t, masterState.slaves, 0)
}

func TestRemoveSlave_NonExistentSlave(t *testing.T) {
	rm := createTestReplicationManager()
	masterState := &MasterState{
		slaves: make(map[string]*SlaveConnection),
	}

	// Should not panic
	rm.removeSlave(masterState, "non-existent")

	assert.Len(t, masterState.slaves, 0)
}

// Tests for sendHandshakeResponse

func TestSendHandshakeResponse_Success(t *testing.T) {
	rm := createTestReplicationManager()
	slave := createTestSlaveConnection("test-slave", 100)

	var receivedData []byte
	var receivedError error
	yieldFunc := func(data []byte, err error) bool {
		receivedData = data
		receivedError = err
		return true
	}

	result := rm.sendHandshakeResponse(yieldFunc, slave)

	assert.True(t, result)
	assert.NoError(t, receivedError)

	var response map[string]any
	err := json.Unmarshal(receivedData, &response)
	require.NoError(t, err)
	assert.Equal(t, "OK", response["status"])
	assert.Equal(t, "test-slave", response["slave_id"])
}

func TestSendHandshakeResponse_YieldFails(t *testing.T) {
	rm := createTestReplicationManager()
	slave := createTestSlaveConnection("test-slave", 100)

	yieldFunc := func(data []byte, err error) bool {
		return false // Simulate connection closed
	}

	result := rm.sendHandshakeResponse(yieldFunc, slave)

	assert.False(t, result)
}

// Tests for addLogToBatch

func TestAddLogToBatch_ConvertWALToLogEvent(t *testing.T) {
	rm := createTestReplicationManager()

	walLog := createTestWALLogEntry(250, 5, []string{"SET", "key", "value"})
	var batch []LogEvent

	batch = rm.addLogToBatch(batch, walLog)

	assert.Len(t, batch, 1)
	assert.Equal(t, uint64(250), batch[0].LSN)
	assert.Equal(t, 5, batch[0].CmdID)
	assert.Equal(t, []string{"SET", "key", "value"}, batch[0].Args)
}

func TestAddLogToBatch_MultipleEntries(t *testing.T) {
	rm := createTestReplicationManager()

	var batch []LogEvent

	log1 := createTestWALLogEntry(100, 1, []string{"SET", "a", "1"})
	log2 := createTestWALLogEntry(101, 2, []string{"GET", "a"})

	batch = rm.addLogToBatch(batch, log1)
	batch = rm.addLogToBatch(batch, log2)

	assert.Len(t, batch, 2)
	assert.Equal(t, uint64(100), batch[0].LSN)
	assert.Equal(t, uint64(101), batch[1].LSN)
}

// Tests for GetMinimumSlaveLSN

func TestGetMinimumSlaveLSN_NoSlaves(t *testing.T) {
	rm := createTestReplicationManager()
	rm.masterState = &MasterState{
		slaves: make(map[string]*SlaveConnection),
	}

	minLSN := rm.GetMinimumSlaveLSN()

	assert.Equal(t, ^uint64(0), minLSN) // MaxUint64
}

func TestGetMinimumSlaveLSN_SingleSlave(t *testing.T) {
	rm := createTestReplicationManager()
	rm.masterState = &MasterState{
		slaves: map[string]*SlaveConnection{
			"slave1": createTestSlaveConnection("slave1", 150),
		},
	}

	minLSN := rm.GetMinimumSlaveLSN()

	assert.Equal(t, uint64(150), minLSN)
}

func TestGetMinimumSlaveLSN_MultipleSlaves(t *testing.T) {
	rm := createTestReplicationManager()
	rm.masterState = &MasterState{
		slaves: map[string]*SlaveConnection{
			"slave1": createTestSlaveConnection("slave1", 200),
			"slave2": createTestSlaveConnection("slave2", 100),
			"slave3": createTestSlaveConnection("slave3", 300),
		},
	}

	minLSN := rm.GetMinimumSlaveLSN()

	assert.Equal(t, uint64(100), minLSN) // Should return minimum
}

func TestGetMinimumSlaveLSN_NonMaster(t *testing.T) {
	cfg := &configuration.Config{
		Replication: &configuration.ReplicationConfig{
			Role: "slave",
		},
	}
	rm := NewReplicationManager(cfg)

	minLSN := rm.GetMinimumSlaveLSN()

	assert.Equal(t, ^uint64(0), minLSN) // MaxUint64 for non-masters
}

func TestGetMinimumSlaveLSN_NoMasterState(t *testing.T) {
	rm := createTestReplicationManager()
	rm.masterState = nil

	minLSN := rm.GetMinimumSlaveLSN()

	assert.Equal(t, ^uint64(0), minLSN) // MaxUint64 when no master state
}

// Tests for getLogsChan

func TestGetLogsChan_ConvertsIteratorToChannel(t *testing.T) {
	rm := createTestReplicationManager()
	ctx := context.Background()

	mockReader := &mockLogsReader{
		logs: []*wal.LogEntry{
			createTestWALLogEntry(100, 1, []string{"SET", "a", "1"}),
			createTestWALLogEntry(101, 2, []string{"GET", "a"}),
		},
	}

	iterator := mockReader.ReadLogsFromLSN(ctx, 50)
	logChan := rm.getLogsChan(ctx, iterator)

	// Read from channel
	var results []LogIteration
	for logIter := range logChan {
		results = append(results, logIter)
	}

	assert.Len(t, results, 2)
	assert.Equal(t, uint64(100), results[0].log.LSN)
	assert.Equal(t, uint64(101), results[1].log.LSN)
	assert.NoError(t, results[0].err)
	assert.NoError(t, results[1].err)
}

func TestGetLogsChan_HandlesContextCancellation(t *testing.T) {
	rm := createTestReplicationManager()
	ctx, cancel := context.WithCancel(context.Background())

	mockReader := &mockLogsReader{
		logs: []*wal.LogEntry{
			createTestWALLogEntry(100, 1, []string{"SET", "a", "1"}),
		},
	}

	iterator := mockReader.ReadLogsFromLSN(ctx, 50)
	logChan := rm.getLogsChan(ctx, iterator)

	// Cancel context immediately
	cancel()

	// Channel should close without hanging
	select {
	case <-logChan:
		// Channel should close
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Channel should have closed when context was cancelled")
	}
}

func TestGetLogsChan_HandlesIteratorErrors(t *testing.T) {
	rm := createTestReplicationManager()
	ctx := context.Background()

	testError := fmt.Errorf("iterator error")
	mockReader := &mockLogsReader{
		logs: []*wal.LogEntry{
			createTestWALLogEntry(100, 1, []string{"SET", "a", "1"}),
		},
		errors: []error{testError},
	}

	iterator := mockReader.ReadLogsFromLSN(ctx, 50)
	logChan := rm.getLogsChan(ctx, iterator)

	// Read from channel
	logIter := <-logChan

	assert.NotNil(t, logIter.log)
	assert.Equal(t, testError, logIter.err)
}

// Tests for sendLogBatch

func TestSendLogBatch_EmptyBatch(t *testing.T) {
	rm := createTestReplicationManager()
	slave := createTestSlaveConnection("test-slave", 100)

	yieldFunc := func(data []byte, err error) bool {
		t.Fatal("Yield should not be called for empty batch")
		return true
	}

	result := rm.sendLogBatch(slave, []LogEvent{}, yieldFunc)

	assert.True(t, result)
}

func TestSendLogBatch_ValidBatch(t *testing.T) {
	rm := createTestReplicationManager()
	slave := createTestSlaveConnection("test-slave", 100)

	logBatch := []LogEvent{
		{LSN: 101, CmdID: 1, Args: []string{"SET", "a", "1"}},
		{LSN: 102, CmdID: 2, Args: []string{"GET", "a"}},
	}

	var receivedData []byte
	var receivedError error
	yieldFunc := func(data []byte, err error) bool {
		receivedData = data
		receivedError = err
		return true
	}

	result := rm.sendLogBatch(slave, logBatch, yieldFunc)

	assert.True(t, result)
	assert.NoError(t, receivedError)

	var batch LogBatch
	err := json.Unmarshal(receivedData, &batch)
	require.NoError(t, err)
	assert.Equal(t, 2, batch.Total)
	assert.Len(t, batch.Events, 2)
	assert.Equal(t, uint64(101), batch.Events[0].LSN)
	assert.Equal(t, uint64(102), batch.Events[1].LSN)

	// Verify slave LastLSN was updated
	assert.Equal(t, uint64(102), slave.LastLSN)
}

func TestSendLogBatch_YieldFails(t *testing.T) {
	rm := createTestReplicationManager()
	slave := createTestSlaveConnection("test-slave", 100)

	logBatch := []LogEvent{
		{LSN: 101, CmdID: 1, Args: []string{"SET", "a", "1"}},
	}

	yieldFunc := func(data []byte, err error) bool {
		return false // Simulate connection closed
	}

	result := rm.sendLogBatch(slave, logBatch, yieldFunc)

	assert.False(t, result)
	// LastLSN should not be updated when yield fails
	assert.Equal(t, uint64(100), slave.LastLSN)
}

func TestSendLogBatch_UpdatesSlaveLastLSN(t *testing.T) {
	rm := createTestReplicationManager()
	slave := createTestSlaveConnection("test-slave", 100)

	logBatch := []LogEvent{
		{LSN: 150, CmdID: 1, Args: []string{"SET", "a", "1"}},
		{LSN: 160, CmdID: 2, Args: []string{"GET", "a"}},
	}

	yieldFunc := func(data []byte, err error) bool {
		return true
	}

	result := rm.sendLogBatch(slave, logBatch, yieldFunc)

	assert.True(t, result)
	assert.Equal(t, uint64(160), slave.LastLSN) // Should update to highest LSN
}

func TestSendLogBatch_DoesNotDecreaseLSN(t *testing.T) {
	rm := createTestReplicationManager()
	slave := createTestSlaveConnection("test-slave", 200)

	logBatch := []LogEvent{
		{LSN: 150, CmdID: 1, Args: []string{"SET", "a", "1"}},
	}

	yieldFunc := func(data []byte, err error) bool {
		return true
	}

	result := rm.sendLogBatch(slave, logBatch, yieldFunc)

	assert.True(t, result)
	assert.Equal(t, uint64(200), slave.LastLSN) // Should not decrease
}

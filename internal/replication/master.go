package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/wal"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/network"
)

// Master replication constants
const (
	DefaultMasterPort            = "3333"
	ReplicationMessageMultiplier = 100 * 2 // Large enough for batches
	ReplicationIdleMultiplier    = 10      // Longer timeout for replication
	MaxSlaveConnections          = 100     // Support multiple slaves
	BatchTimeoutMs               = 20      // Batch timeout in milliseconds
	MaxBatchSize                 = 50      // Maximum logs per batch
)

// Master replication errors
var (
	ErrInvalidLSNSyncMsg     = errors.New("invalid LSN sync message")
	ErrExpectedLSNSyncMsg    = errors.New("expected lsn_sync message")
	ErrInvalidLastLSN        = errors.New("invalid last_lsn")
	ErrMarshalResponseFailed = errors.New("failed to marshal response")
)

type SlaveConnection struct {
	ID string
	// In future it will be used to prevent wal segments
	// that are not send to replica from compaction or recycling
	LastLSN uint64
}

type MasterState struct {
	slaves map[string]*SlaveConnection
	mu     sync.RWMutex
}

type LogIteration struct {
	log *wal.LogEntry
	err error
}

func (rm *ReplicationManager) startMaster(ctx context.Context) {
	logging.Info("Starting master replication server")

	if rm.logsReader == nil {
		logging.Error("LogReader not configured for master")
		return
	}

	if rm.cfg.Replication.MasterAddress == "" {
		rm.cfg.Replication.MasterAddress = rm.cfg.Network.Ip
	}

	if rm.cfg.Replication.MasterPort == "" {
		rm.cfg.Replication.MasterPort = DefaultMasterPort
	}

	// Create network configuration for replication server
	replicationCfg := &configuration.NetworkConfig{
		Ip:                      rm.cfg.Replication.MasterAddress,
		Port:                    rm.cfg.Replication.MasterPort,
		MaxMessageSize:          rm.cfg.Network.MaxMessageSize * ReplicationMessageMultiplier,
		IdleTimeout:             rm.cfg.Network.IdleTimeout * ReplicationIdleMultiplier,
		MaxConnections:          MaxSlaveConnections,
		GracefulShutdownTimeout: rm.cfg.Network.GracefulShutdownTimeout,
	}

	server, err := network.NewTCPServer(replicationCfg)
	if err != nil {
		logging.Error("Failed to create replication server", zap.Error(err))
		return
	}

	masterState := &MasterState{
		slaves: make(map[string]*SlaveConnection),
	}

	// Store master state for access by other methods
	rm.masterState = masterState

	go server.HandleStreamRequests(ctx, func(ctx context.Context, initialData []byte) iter.Seq2[[]byte, error] {
		return rm.handleSlaveConnection(ctx, initialData, masterState)
	})

	logging.Info("Master replication server started",
		zap.String("address", net.JoinHostPort(replicationCfg.Ip, replicationCfg.Port)))

	<-ctx.Done()

	logging.Info("Master replication server stopped")
}

func (rm *ReplicationManager) handleSlaveConnection(
	ctx context.Context,
	initialData []byte,
	masterState *MasterState,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		slave, err := rm.parseSlaveHandshake(initialData)
		if err != nil {
			logging.Error("Failed to parse slave handshake", zap.Error(err))
			yield(nil, err)
			return
		}

		rm.registerSlave(masterState, slave)

		logging.Info("Slave connected",
			zap.String("slave_id", slave.ID),
			zap.Uint64("last_lsn", slave.LastLSN))

		if !rm.sendHandshakeResponse(yield, slave) {
			rm.removeSlave(masterState, slave.ID)
			return
		}

		rm.streamLogsToSlave(ctx, slave, yield)

		rm.removeSlave(masterState, slave.ID)
		logging.Info("Slave disconnected", zap.String("slave_id", slave.ID))
	}
}

// parseSlaveHandshake parses initial LSN sync message and creates SlaveConnection
func (rm *ReplicationManager) parseSlaveHandshake(initialData []byte) (*SlaveConnection, error) {
	var lsnSync LSNSyncMessage
	if err := json.Unmarshal(initialData, &lsnSync); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLSNSyncMsg, err)
	}

	// Validate LSN sync message
	if lsnSync.Type != "lsn_sync" {
		return nil, ErrExpectedLSNSyncMsg
	}

	if lsnSync.LastLSN == nil {
		return nil, ErrInvalidLastLSN
	}

	lastLSN := *lsnSync.LastLSN
	slaveID := rm.extractOrGenerateSlaveID(lsnSync.SlaveID, lastLSN)

	return &SlaveConnection{
		ID:      slaveID,
		LastLSN: lastLSN,
	}, nil
}

// extractOrGenerateSlaveID gets slave ID from message or generates new one
func (rm *ReplicationManager) extractOrGenerateSlaveID(slaveID string, lastLSN uint64) string {
	if slaveID != "" {
		return slaveID
	}
	return fmt.Sprintf("slave-%d-%d", time.Now().Unix(), lastLSN)
}

// registerSlave adds slave to master state
func (rm *ReplicationManager) registerSlave(masterState *MasterState, slave *SlaveConnection) {
	concurrency.WithLock(&masterState.mu, func() error { // nolint:errcheck
		masterState.slaves[slave.ID] = slave
		return nil
	})
}

// sendHandshakeResponse sends response to slave after successful handshake
func (rm *ReplicationManager) sendHandshakeResponse(
	yield func([]byte, error) bool,
	slave *SlaveConnection,
) bool {
	response := LSNSyncResponse{
		Status:  "OK",
		SlaveID: slave.ID,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		logging.Error("Failed to marshal response", zap.Error(ErrMarshalResponseFailed))
		return yield([]byte("OK"), nil) // Fallback to simple response
	}

	return yield(responseBytes, nil)
}

func (rm *ReplicationManager) streamLogsToSlave(
	ctx context.Context,
	slave *SlaveConnection,
	yield func([]byte, error) bool,
) {
	startLSN := slave.LastLSN + 1

	logging.Info("Starting log streaming for slave",
		zap.String("slave_id", slave.ID),
		zap.Uint64("start_lsn", startLSN))

	// Create a context that can be cancelled when the slave disconnects
	slaveCtx, slaveCancel := context.WithCancel(ctx)
	defer slaveCancel()

	// Use the LogReader iterator to get logs starting from the slave's last LSN
	logIterator := rm.logsReader.ReadLogsFromLSN(slaveCtx, startLSN)
	logChan := rm.getLogsChan(slaveCtx, logIterator)

	rm.runLogBatchingLoop(ctx, slave, logChan, yield)
}

// runLogBatchingLoop handles the main batching loop for streaming logs
func (rm *ReplicationManager) runLogBatchingLoop(
	ctx context.Context,
	slave *SlaveConnection,
	logChan <-chan LogIteration,
	yield func([]byte, error) bool,
) {
	batchTimeout := time.NewTimer(BatchTimeoutMs * time.Millisecond)
	defer batchTimeout.Stop()

	var logBatch []LogEvent

	for {
		select {
		case <-ctx.Done():
			return

		case <-batchTimeout.C:
			if len(logBatch) > 0 {
				if !rm.sendLogBatch(slave, logBatch, yield) {
					return
				}
				logBatch = logBatch[:0]
			}
			batchTimeout.Reset(BatchTimeoutMs * time.Millisecond)

		case logData := <-logChan:
			if logData.err != nil {
				logging.Error("Error reading log from iterator",
					zap.String("slave_id", slave.ID),
					zap.Error(logData.err))
				yield(nil, logData.err)
				return
			}

			if logData.log != nil {
				logBatch = rm.addLogToBatch(logBatch, logData.log)

				if len(logBatch) >= MaxBatchSize {
					if !rm.sendLogBatch(slave, logBatch, yield) {
						return
					}
					logBatch = logBatch[:0]
					batchTimeout.Reset(BatchTimeoutMs * time.Millisecond)
				}
			}
		}
	}
}

// addLogToBatch converts WAL log entry to LogEvent and adds to batch
func (rm *ReplicationManager) addLogToBatch(logBatch []LogEvent, log *wal.LogEntry) []LogEvent {
	logEvent := LogEvent{
		LSN:   log.LSN,
		CmdID: log.Command,
		Args:  log.Arguments,
	}
	return append(logBatch, logEvent)
}

func (rm *ReplicationManager) removeSlave(masterState *MasterState, slaveID string) {
	concurrency.WithLock(&masterState.mu, func() error { // nolint:errcheck
		delete(masterState.slaves, slaveID)
		return nil
	})
}

// GetMinimumSlaveLSN returns the minimum LSN across all connected slaves.
// This can be used by WAL to determine which logs can be safely compacted.
// Returns MaxUint64 if no slaves are connected (meaning all logs can be compacted).
func (rm *ReplicationManager) GetMinimumSlaveLSN() uint64 {
	if !rm.IsMaster() {
		return ^uint64(0) // MaxUint64 - non-masters can compact everything
	}

	// Access master state through a type assertion
	// This is safe because we know rm is master at this point
	if rm.masterState == nil {
		return ^uint64(0) // MaxUint64 - all logs can be compacted
	}

	minLSN := ^uint64(0) // Start with MaxUint64
	hasSlaves := false

	concurrency.WithLock(&rm.masterState.mu, func() error { // nolint:errcheck
		for _, slave := range rm.masterState.slaves {
			hasSlaves = true
			if slave.LastLSN < minLSN {
				minLSN = slave.LastLSN
			}
		}
		return nil
	})

	// If no slaves connected, return MaxUint64 to allow all logs to be compacted
	if !hasSlaves {
		return ^uint64(0) // MaxUint64
	}

	return minLSN
}

func (rm *ReplicationManager) getLogsChan(
	ctx context.Context,
	logIterator iter.Seq2[*wal.LogEntry, error],
) <-chan LogIteration {
	logChan := make(chan LogIteration, 100)
	go func() {
		defer close(logChan)
		for log, err := range logIterator {
			select {
			case logChan <- LogIteration{log: log, err: err}:
				// Continue to next log entry
			case <-ctx.Done():
				return
			}
		}
	}()
	return logChan
}

func (rm *ReplicationManager) sendLogBatch(
	slave *SlaveConnection,
	logBatch []LogEvent,
	yield func([]byte, error) bool,
) bool {
	if len(logBatch) == 0 {
		return true
	}

	batch := LogBatch{
		Total:  len(logBatch),
		Events: logBatch,
	}

	data, err := json.Marshal(batch)
	if err != nil {
		logging.Error("Failed to marshal log batch",
			zap.String("slave_id", slave.ID),
			zap.Error(err))
		yield(nil, err)
		return false
	}

	if !yield(data, nil) {
		logging.Warn("Failed to send log batch to slave (connection closed)",
			zap.String("slave_id", slave.ID))
		return false
	}

	// Update slave's last LSN to the highest LSN in the batch
	if len(logBatch) > 0 {
		lastLSN := logBatch[len(logBatch)-1].LSN
		if lastLSN > slave.LastLSN {
			slave.LastLSN = lastLSN
		}

		logging.Info("Sent log batch to slave",
			zap.String("slave_id", slave.ID),
			zap.Int("batch_size", len(logBatch)),
			zap.Uint64("first_lsn", logBatch[0].LSN),
			zap.Uint64("last_lsn", lastLSN))
	}

	return true
}

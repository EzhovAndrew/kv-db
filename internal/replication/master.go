package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net"
	"sync"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/concurrency"
	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/wal"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/network"
	"go.uber.org/zap"
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
	log *wal.Log
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
		rm.cfg.Replication.MasterPort = "3333"
	}

	// Create network configuration for replication server
	replicationCfg := &configuration.NetworkConfig{
		Ip:                      rm.cfg.Replication.MasterAddress,
		Port:                    rm.cfg.Replication.MasterPort,
		MaxMessageSize:          rm.cfg.Network.MaxMessageSize * 100 * 2, // Large enough for batches
		IdleTimeout:             rm.cfg.Network.IdleTimeout * 10,         // Longer timeout for replication
		MaxConnections:          100,                                     // Support multiple slaves
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

	go server.HandleStreamRequests(ctx, func(ctx context.Context, initialData []byte) iter.Seq2[[]byte, error] {
		return rm.handleSlaveConnection(ctx, initialData, masterState)
	})

	logging.Info("Master replication server started",
		zap.String("address", net.JoinHostPort(replicationCfg.Ip, replicationCfg.Port)))

	<-ctx.Done()

	logging.Info("Master replication server stopped")
}

func (rm *ReplicationManager) handleSlaveConnection(ctx context.Context, initialData []byte, masterState *MasterState) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		// Parse initial LSN sync message from slave
		var lsnSync map[string]interface{}
		if err := json.Unmarshal(initialData, &lsnSync); err != nil {
			logging.Error("Failed to parse LSN sync message", zap.Error(err))
			yield(nil, fmt.Errorf("invalid LSN sync message: %w", err))
			return
		}

		// Validate LSN sync message
		if lsnSync["type"] != "lsn_sync" {
			logging.Error("Invalid message type", zap.Any("type", lsnSync["type"]))
			yield(nil, fmt.Errorf("expected lsn_sync message"))
			return
		}

		lastLSN, ok := lsnSync["last_lsn"].(float64) // JSON numbers are float64
		if !ok {
			logging.Error("Invalid last_lsn in sync message")
			yield(nil, fmt.Errorf("invalid last_lsn"))
			return
		}

		slave := rm.NewSlaveWithLSN(uint64(lastLSN))

		concurrency.WithLock(&masterState.mu, func() error {
			masterState.slaves[slave.ID] = slave
			return nil
		})

		logging.Info("Slave connected",
			zap.String("slave_id", slave.ID),
			zap.Uint64("last_lsn", slave.LastLSN))

		if !yield([]byte("OK"), nil) {
			rm.removeSlave(masterState, slave.ID)
			return
		}

		rm.streamLogsToSlave(ctx, slave, masterState, yield)

		rm.removeSlave(masterState, slave.ID)
		logging.Info("Slave disconnected", zap.String("slave_id", slave.ID))
	}
}

func (rm *ReplicationManager) streamLogsToSlave(ctx context.Context, slave *SlaveConnection, masterState *MasterState, yield func([]byte, error) bool) {

	startLSN := slave.LastLSN

	logging.Info("Starting log streaming for slave",
		zap.String("slave_id", slave.ID),
		zap.Uint64("start_lsn", startLSN))

	// Create a context that can be cancelled when the slave disconnects
	slaveCtx, slaveCancel := context.WithCancel(ctx)
	defer slaveCancel()

	// Use the LogReader iterator to get logs starting from the slave's last LSN
	logIterator := rm.logsReader.ReadLogsFromLSN(slaveCtx, startLSN)

	var logBatch []LogEvent
	batchTimeout := time.NewTimer(20 * time.Millisecond) // Send batch after 20 ms even if not full
	defer batchTimeout.Stop()

	// Create a channel to receive logs from the iterator
	// This channel will be closed when the iterator is exhausted
	// This is for non-blocking reads from the iterator
	// This is needed to catch ctx.Done() and exit the loop
	logChan := rm.getLogsChan(slaveCtx, logIterator)

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
			batchTimeout.Reset(20 * time.Millisecond)

		default:
			select {
			case logData := <-logChan:
				if logData.err != nil {
					logging.Error("Error reading log from iterator",
						zap.String("slave_id", slave.ID),
						zap.Error(logData.err))
					yield(nil, logData.err)
					return
				}

				if logData.log != nil {
					logEvent := LogEvent{
						LSN:   logData.log.LSN,
						CmdID: logData.log.Command,
						Args:  logData.log.Arguments,
					}
					logBatch = append(logBatch, logEvent)

					if len(logBatch) >= 50 { // TODO: make batch size configured
						if !rm.sendLogBatch(slave, logBatch, yield) {
							return
						}
						logBatch = logBatch[:0]
						batchTimeout.Reset(time.Second)
					}
				}
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

func (rm *ReplicationManager) NewSlaveWithLSN(lastLSN uint64) *SlaveConnection {
	slave := &SlaveConnection{
		ID:      fmt.Sprintf("slave-%d", time.Now().Unix()),
		LastLSN: lastLSN,
	}
	return slave
}

func (rm *ReplicationManager) removeSlave(masterState *MasterState, slaveID string) {
	concurrency.WithLock(&masterState.mu, func() error { // nolint:errcheck
		delete(masterState.slaves, slaveID)
		return nil
	})
}

func (rm *ReplicationManager) getLogsChan(ctx context.Context, logIterator iter.Seq2[*wal.Log, error]) <-chan LogIteration {
	logChan := make(chan LogIteration, 100)
	go func() {
		defer close(logChan)
		for log, err := range logIterator {
			select {
			case logChan <- LogIteration{log: log, err: err}:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return logChan
}

func (rm *ReplicationManager) sendLogBatch(slave *SlaveConnection, logBatch []LogEvent, yield func([]byte, error) bool) bool {
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

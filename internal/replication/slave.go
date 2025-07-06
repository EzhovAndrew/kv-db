package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/wal"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/network"
	"go.uber.org/zap"
)

func (rm *ReplicationManager) startSlave(ctx context.Context) {

	// Create network configuration for master connection
	masterCfg := &configuration.NetworkConfig{
		Ip:                      rm.cfg.Replication.MasterAddress,
		Port:                    rm.cfg.Replication.MasterPort,
		MaxMessageSize:          rm.cfg.Network.MaxMessageSize * 100 * 2, // max message size * 100(batch size) * 2(for additional fields)
		IdleTimeout:             rm.cfg.Network.IdleTimeout,
		MaxConnections:          1, // Not used for client
		GracefulShutdownTimeout: rm.cfg.Network.GracefulShutdownTimeout,
	}

	// Create TCP client for master connection
	client := network.NewTCPClient(masterCfg)
	rm.client = client

	// Connect to master
	if err := rm.connectToMaster(ctx, client); err != nil {
		logging.Error("Failed to connect to master", zap.Error(err))
		return
	}

	// Start health monitoring
	client.StartHealthMonitoring(ctx)

	// Start push mode to receive logs from master
	if err := client.StartPushMode(ctx, rm.handleMasterMessage); err != nil {
		logging.Error("Failed to start push mode", zap.Error(err))
		return
	}

	logging.Info("Slave replication started successfully",
		zap.String("master_address", net.JoinHostPort(rm.cfg.Replication.MasterAddress, rm.cfg.Replication.MasterPort)))

	<-ctx.Done()

	// Cleanup
	client.StopPushMode()
	client.StopHealthMonitoring()
	if err := client.Close(); err != nil {
		logging.Warn("Error closing master connection", zap.Error(err))
	}

	logging.Info("Slave replication stopped")
}

// connectToMaster establishes connection to master with retries
func (rm *ReplicationManager) connectToMaster(ctx context.Context, client *network.TCPClient) error {
	maxRetries := 5
	baseDelay := 1 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := client.Connect(ctx)
		if err == nil {
			logging.Info("Successfully connected to master",
				zap.String("master_address", net.JoinHostPort(rm.cfg.Replication.MasterAddress, rm.cfg.Replication.MasterPort)),
				zap.Int("attempt", attempt))

			if err := rm.sendLastLSNToMaster(ctx, client); err != nil {
				logging.Error("Failed to send last LSN to master", zap.Error(err))
				client.Close() // nolint: errcheck
				return fmt.Errorf("failed to sync LSN with master: %w", err)
			}
			return nil
		}

		logging.Warn("Failed to connect to master, retrying",
			zap.Error(err),
			zap.Int("attempt", attempt),
			zap.Int("max_retries", maxRetries))

		if attempt < maxRetries {
			delay := time.Duration(attempt) * baseDelay
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// Continue to next attempt
			}
		}
	}

	return fmt.Errorf("failed to connect to master after %d attempts", maxRetries)
}

// handleMasterMessage processes incoming log messages from master
func (rm *ReplicationManager) handleMasterMessage(ctx context.Context, message []byte) []byte {
	// Parse JSON log batch
	var batch LogBatch
	if err := json.Unmarshal(message, &batch); err != nil {
		logging.Error("Failed to parse log batch from master",
			zap.Error(err),
			zap.String("raw_message", string(message)))
		return []byte("ERROR: Invalid JSON format")
	}

	// Validate batch
	if err := rm.validateLogBatch(&batch); err != nil {
		logging.Error("Invalid log batch received",
			zap.Error(err),
			zap.Int("total", batch.Total),
			zap.Int("events_count", len(batch.Events)))
		return []byte("ERROR: Invalid batch format")
	}

	walLogs := rm.convertToWALLogs(batch.Events)

	if err := rm.applyToStorage(walLogs); err != nil {
		logging.Error("Failed to apply logs to storage",
			zap.Error(err),
			zap.Int("log_count", len(walLogs)))
		return []byte("ERROR: Storage application failed")
	}

	logging.Info("Successfully applied log batch",
		zap.Int("total_logs", batch.Total),
		zap.Int("applied_logs", len(batch.Events)))

	return []byte("OK")
}

// validateLogBatch validates the received log batch
func (rm *ReplicationManager) validateLogBatch(batch *LogBatch) error {
	if batch.Total <= 0 {
		return fmt.Errorf("invalid total count: %d", batch.Total)
	}

	if len(batch.Events) == 0 {
		return fmt.Errorf("empty events list")
	}

	if len(batch.Events) > batch.Total {
		return fmt.Errorf("events count %d exceeds total %d", len(batch.Events), batch.Total)
	}

	// Validate each event
	for i, event := range batch.Events {
		if event.LSN == 0 {
			return fmt.Errorf("invalid LSN in event %d: %d", i, event.LSN)
		}

		if len(event.Args) == 0 {
			return fmt.Errorf("empty args in event %d", i)
		}

		// Validate command ID (assuming valid range)
		if event.CmdID < 0 {
			return fmt.Errorf("invalid command ID in event %d: %d", i, event.CmdID)
		}
	}

	return nil
}

// convertToWALLogs converts LogEvents to WAL Log structures
func (rm *ReplicationManager) convertToWALLogs(events []LogEvent) []*wal.LogEntry {
	logs := make([]*wal.LogEntry, len(events))

	for i, event := range events {
		logs[i] = &wal.LogEntry{
			LSN:       event.LSN,
			Command:   event.CmdID,
			Arguments: event.Args,
		}
	}

	return logs
}

// applyToStorage applies logs to the storage engine
func (rm *ReplicationManager) applyToStorage(logs []*wal.LogEntry) error {
	if rm.storageApplier == nil {
		return fmt.Errorf("storage applier not configured")
	}

	if err := rm.storageApplier.ApplyLogs(logs); err != nil {
		return fmt.Errorf("failed to apply logs to storage: %w", err)
	}

	logging.Info("Applied commands to storage",
		zap.Int("command_count", len(logs)))

	return nil
}

func (rm *ReplicationManager) sendLastLSNToMaster(ctx context.Context, client *network.TCPClient) error {
	var lastLSN uint64

	if rm.storageApplier != nil {
		lastLSN = rm.storageApplier.GetLastLSN()
	}

	lsnMessage := map[string]any{
		"type":     "lsn_sync",
		"last_lsn": lastLSN,
	}

	messageBytes, err := json.Marshal(lsnMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal LSN sync message: %w", err)
	}

	logging.Info("Sending last LSN to master",
		zap.Uint64("last_lsn", lastLSN))

	response, err := client.SendMessageWithTimeout(ctx, messageBytes, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to send LSN sync message: %w", err)
	}

	responseStr := string(response)
	if responseStr != "OK" {
		return fmt.Errorf("master rejected LSN sync: %s", responseStr)
	}

	logging.Info("Successfully synchronized LSN with master",
		zap.Uint64("last_lsn", lastLSN))

	return nil
}

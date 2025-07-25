package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/wal"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/network"
)

// Slave replication constants
const (
	SlaveReplicationMessageMultiplier = 100 * 2 // max message size * 100(batch size) * 2(for additional fields)
	SyncTimeoutSeconds                = 10      // Timeout for LSN sync message
	MaxRetries                        = 5       // Maximum connection retry attempts
	BaseDelaySeconds                  = 1       // Base delay between retries in seconds
)

// Slave replication errors
var (
	ErrInvalidJSONFormat        = errors.New("invalid JSON format")
	ErrInvalidBatchFormat       = errors.New("invalid batch format")
	ErrStorageApplicationFailed = errors.New("storage application failed")
	ErrMarshalLSNSyncFailed     = errors.New("failed to marshal LSN sync message")
	ErrSendLSNSyncFailed        = errors.New("failed to send LSN sync message")
	ErrMasterRejectedLSNSync    = errors.New("master rejected LSN sync")
	ErrFailedToSyncLSN          = errors.New("failed to sync LSN with master")
	ErrConnectToMasterFailed    = errors.New("failed to connect to master")
	ErrInvalidTotalCount        = errors.New("invalid total count")
	ErrEmptyEventsList          = errors.New("empty events list")
	ErrEventCountExceedsTotal   = errors.New("events count exceeds total")
	ErrEmptyArgsInEvent         = errors.New("empty args in event")
	ErrInvalidCommandID         = errors.New("invalid command ID in event")
)

func (rm *ReplicationManager) startSlave(ctx context.Context) {
	// Create network configuration for master connection
	masterCfg := &configuration.NetworkConfig{
		Ip:                      rm.cfg.Replication.MasterAddress,
		Port:                    rm.cfg.Replication.MasterPort,
		MaxMessageSize:          rm.cfg.Network.MaxMessageSize * SlaveReplicationMessageMultiplier,
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
	if err := client.Close(); err != nil {
		logging.Warn("Error closing master connection", zap.Error(err))
	}

	logging.Info("Slave replication stopped")
}

// connectToMaster establishes connection to master with retries
func (rm *ReplicationManager) connectToMaster(ctx context.Context, client *network.TCPClient) error {
	for attempt := 1; attempt <= MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := rm.attemptConnection(ctx, client, attempt); err == nil {
			return nil
		}

		if attempt < MaxRetries {
			delay := time.Duration(attempt) * BaseDelaySeconds * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// Continue to next attempt
			}
		}
	}

	return fmt.Errorf("%w after %d attempts", ErrConnectToMasterFailed, MaxRetries)
}

// attemptConnection tries to connect to master and sync LSN
func (rm *ReplicationManager) attemptConnection(ctx context.Context, client *network.TCPClient, attempt int) error {
	err := client.Connect(ctx)
	if err != nil {
		logging.Warn("Failed to connect to master, retrying",
			zap.Error(err),
			zap.Int("attempt", attempt),
			zap.Int("max_retries", MaxRetries))
		return err
	}

	logging.Info("Successfully connected to master",
		zap.String("master_address", net.JoinHostPort(rm.cfg.Replication.MasterAddress, rm.cfg.Replication.MasterPort)),
		zap.Int("attempt", attempt))

	if err := rm.sendLastLSNToMaster(ctx, client); err != nil {
		logging.Error("Failed to send last LSN to master", zap.Error(err))
		client.Close() // nolint: errcheck
		return fmt.Errorf("%w: %v", ErrFailedToSyncLSN, err)
	}

	return nil
}

// handleMasterMessage processes incoming log messages from master
func (rm *ReplicationManager) handleMasterMessage(ctx context.Context, message []byte) []byte {
	var batch LogBatch
	if err := json.Unmarshal(message, &batch); err != nil {
		logging.Error("Failed to parse log batch from master",
			zap.Error(err),
			zap.String("raw_message", string(message)))
		return rm.createErrorResponse(ErrInvalidJSONFormat.Error())
	}

	if err := rm.validateLogBatch(&batch); err != nil {
		logging.Error("Invalid log batch received",
			zap.Error(err),
			zap.Int("total", batch.Total),
			zap.Int("events_count", len(batch.Events)))
		return rm.createErrorResponse(ErrInvalidBatchFormat.Error())
	}

	walLogs := rm.convertToWALLogs(batch.Events)

	if err := rm.applyToStorage(walLogs); err != nil {
		logging.Error("Failed to apply logs to storage",
			zap.Error(err),
			zap.Int("log_count", len(walLogs)))
		return rm.createErrorResponse(ErrStorageApplicationFailed.Error())
	}

	logging.Info("Successfully applied log batch",
		zap.Int("total_logs", batch.Total),
		zap.Int("applied_logs", len(batch.Events)))

	return []byte("OK")
}

// createErrorResponse creates a standardized error response
func (rm *ReplicationManager) createErrorResponse(errorMsg string) []byte {
	return []byte("ERROR: " + errorMsg)
}

// validateLogBatch validates the received log batch
func (rm *ReplicationManager) validateLogBatch(batch *LogBatch) error {
	if batch.Total <= 0 {
		return ErrInvalidTotalCount
	}

	if len(batch.Events) == 0 {
		return ErrEmptyEventsList
	}

	if len(batch.Events) > batch.Total {
		return ErrEventCountExceedsTotal
	}

	for i, event := range batch.Events {
		if len(event.Args) == 0 {
			return fmt.Errorf("%w %d", ErrEmptyArgsInEvent, i)
		}

		if event.CmdID < 0 {
			return fmt.Errorf("%w %d: %d", ErrInvalidCommandID, i, event.CmdID)
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

	messageBytes, err := rm.createLSNSyncMessage(lastLSN)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMarshalLSNSyncFailed, err)
	}

	logging.Info("Sending last LSN to master",
		zap.Uint64("last_lsn", lastLSN),
		zap.String("slave_id", rm.cfg.Replication.SlaveID))

	response, err := client.SendMessageWithTimeout(ctx, messageBytes, SyncTimeoutSeconds*time.Second)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSendLSNSyncFailed, err)
	}

	if err := rm.handleMasterResponse(response); err != nil {
		return err
	}

	logging.Info("Successfully synchronized LSN with master",
		zap.Uint64("last_lsn", lastLSN),
		zap.String("slave_id", rm.cfg.Replication.SlaveID))

	return nil
}

// createLSNSyncMessage creates the LSN sync message to send to master
func (rm *ReplicationManager) createLSNSyncMessage(lastLSN uint64) ([]byte, error) {
	lsnMessage := LSNSyncMessage{
		Type:    "lsn_sync",
		LastLSN: &lastLSN,
		SlaveID: rm.cfg.Replication.SlaveID,
	}

	return json.Marshal(lsnMessage)
}

// handleMasterResponse processes the response from master after LSN sync
func (rm *ReplicationManager) handleMasterResponse(response []byte) error {
	var responseData LSNSyncResponse
	if err := json.Unmarshal(response, &responseData); err == nil {
		return rm.processJSONResponse(responseData)
	}

	// Simple string response (backward compatibility)
	return rm.processStringResponse(string(response))
}

// processJSONResponse handles JSON response from master
func (rm *ReplicationManager) processJSONResponse(responseData LSNSyncResponse) error {
	// Master sent JSON response with slave ID
	if responseData.SlaveID != "" && rm.cfg.Replication.SlaveID == "" {
		rm.cfg.Replication.SlaveID = responseData.SlaveID
		logging.Info("Master assigned slave ID",
			zap.String("slave_id", responseData.SlaveID))
		// TODO: In production, persist this ID to config file
	}

	if responseData.Status != "" && responseData.Status != "OK" {
		return fmt.Errorf("%w: %s", ErrMasterRejectedLSNSync, responseData.Status)
	}

	return nil
}

// processStringResponse handles simple string response from master (backward compatibility)
func (rm *ReplicationManager) processStringResponse(responseStr string) error {
	if responseStr != "OK" {
		return fmt.Errorf("%w: %s", ErrMasterRejectedLSNSync, responseStr)
	}
	return nil
}

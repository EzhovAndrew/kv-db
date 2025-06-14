package replication

import (
	"context"
	"iter"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/wal"
	"github.com/EzhovAndrew/kv-db/internal/network"
)

// LogEvent represents a single replication event in JSON format
type LogEvent struct {
	LSN   uint64   `json:"lsn"`
	CmdID int      `json:"cmdId"`
	Args  []string `json:"args"`
}

// LogBatch represents a batch of replication events
type LogBatch struct {
	Total  int        `json:"total"`
	Events []LogEvent `json:"events"`
}

type StorageApplier interface {
	ApplyLogs(logs []*wal.Log) error
	GetLastLSN() uint64
}

type LogsReader interface {
	ReadLogsFromLSN(ctx context.Context, lsn uint64) iter.Seq2[*wal.Log, error]
}

type ReplicationManager struct {
	role string
	cfg  *configuration.Config

	client         *network.TCPClient
	storageApplier StorageApplier
	logsReader     LogsReader
}

func NewReplicationManager(cfg *configuration.Config) *ReplicationManager {
	return &ReplicationManager{
		role: cfg.Replication.Role,
		cfg:  cfg,
	}
}

func (rm *ReplicationManager) Start(ctx context.Context) {
	if rm.IsMaster() {
		rm.startMaster(ctx)
	} else if rm.IsSlave() {
		rm.startSlave(ctx)
	}
}

func (rm *ReplicationManager) IsMaster() bool {
	return rm.role == "master"
}

func (rm *ReplicationManager) IsSlave() bool {
	return rm.role == "slave"
}

func (rm *ReplicationManager) SetStorageApplier(applier StorageApplier) {
	rm.storageApplier = applier
}

func (rm *ReplicationManager) SetLogsReader(reader LogsReader) {
	rm.logsReader = reader
}

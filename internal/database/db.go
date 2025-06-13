package database

import (
	"context"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/database/storage"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/wal"
	"github.com/EzhovAndrew/kv-db/internal/logging"
)

type Storage interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	Shutdown()

	// for replication purposes
	ApplyLogs(logs []*wal.Log) error
	GetLastLSN() uint64
}

type Database struct {
	compute      *compute.Compute
	storage      Storage
	readOnlyMode bool
}

func NewDatabase(cfg *configuration.Config) (*Database, error) {
	compute := compute.NewCompute()
	storage, err := storage.NewStorage(cfg)
	if err != nil {
		return nil, err
	}
	return &Database{compute: compute, storage: storage, readOnlyMode: false}, nil
}

func (db *Database) Start(ctx context.Context) error {
	return nil
}

func (db *Database) SetReadOnly() {
	db.readOnlyMode = true
}

func (db *Database) HandleRequest(ctx context.Context, data []byte) []byte {
	query, err := db.compute.Parse(string(data))
	if err != nil {
		return []byte(err.Error())
	}
	switch query.CommandID() {
	case compute.GetCommandID:
		return db.HandleGetRequest(ctx, query)
	case compute.SetCommandID:
		if db.readOnlyMode {
			return []byte("This instance is in read-only mode")
		}
		return db.HandleSetRequest(ctx, query)
	case compute.DelCommandID:
		if db.readOnlyMode {
			return []byte("This instance is in read-only mode")
		}
		return db.HandleDelRequest(ctx, query)
	default:
		logging.Error("Compute layer is incorrect and returns an unknown command")
		return []byte("Internal error")
	}
}

func (db *Database) HandleGetRequest(ctx context.Context, query compute.Query) []byte {
	value, err := db.storage.Get(ctx, query.Arguments()[0])
	if err != nil {
		return []byte(err.Error())
	}
	return []byte(value)
}

func (db *Database) HandleSetRequest(ctx context.Context, query compute.Query) []byte {
	err := db.storage.Set(ctx, query.Arguments()[0], query.Arguments()[1])
	if err != nil {
		return []byte(err.Error())
	}
	return []byte("OK")
}

func (db *Database) HandleDelRequest(ctx context.Context, query compute.Query) []byte {
	err := db.storage.Delete(ctx, query.Arguments()[0])
	if err != nil {
		return []byte(err.Error())
	}
	return []byte("OK")
}

func (db *Database) Shutdown() {
	db.storage.Shutdown()
}

// for replication purposes
func (db *Database) ApplyLogs(logs []*wal.Log) error {
	return db.storage.ApplyLogs(logs)
}

func (db *Database) GetLastLSN() uint64 {
	return db.storage.GetLastLSN()
}

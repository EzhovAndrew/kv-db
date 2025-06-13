package initialization

import (
	"context"
	"errors"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/network"
	"github.com/EzhovAndrew/kv-db/internal/replication"
)

var ErrConfigIsNil = errors.New("config is nil")

type Database interface {
	Start(ctx context.Context) error
	HandleRequest(ctx context.Context, data []byte) []byte
	SetReadOnly()
	Shutdown()
}

type TCPServer interface {
	HandleRequests(ctx context.Context, handler network.TCPHandler)
}

type ReplicationManager interface {
	Start(ctx context.Context)
	SetStorageApplier(replication.StorageApplier)
	IsSlave() bool
}

type Initializer struct {
	server TCPServer
	db     Database
	rm     ReplicationManager
}

func NewInitializer(cfg *configuration.Config) (*Initializer, error) {
	if cfg == nil {
		return nil, ErrConfigIsNil
	}
	db, err := database.NewDatabase(cfg)
	if err != nil {
		return nil, err
	}
	logging.Info("Database configured")
	server, err := network.NewTCPServer(&cfg.Network)
	if err != nil {
		return nil, err
	}
	logging.Info("Server configured")
	var rm ReplicationManager = nil
	if cfg.Replication != nil {
		rm = replication.NewReplicationManager(cfg)
		rm.SetStorageApplier(db)
		logging.Info("Replication configured")
		if rm.IsSlave() {
			db.SetReadOnly()
		}
	}
	return &Initializer{
		server: server,
		db:     db,
		rm:     rm,
	}, nil
}

func (i *Initializer) StartDatabase(ctx context.Context) error {
	if err := i.db.Start(ctx); err != nil {
		return err
	}
	defer i.db.Shutdown()
	logging.Info("Database started")
	if i.rm != nil {
		go i.rm.Start(ctx)
		logging.Info("Replication started")
	}
	i.server.HandleRequests(ctx, i.db.HandleRequest)
	return nil
}

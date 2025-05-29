package initialization

import (
	"context"
	"errors"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database"
	"github.com/EzhovAndrew/kv-db/internal/logging"
	"github.com/EzhovAndrew/kv-db/internal/network"
)

var ErrConfigIsNil = errors.New("config is nil")

type Initializer struct {
	server *network.TCPServer
	db     *database.Database
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
	return &Initializer{
		server: server,
		db:     db,
	}, nil
}

func (i *Initializer) StartDatabase(ctx context.Context) error {
	if err := i.db.Start(ctx); err != nil {
		return err
	}
	logging.Info("Database started")
	i.server.HandleRequests(ctx, i.db.HandleRequest)
	return nil
}

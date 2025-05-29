package database

import (
	"context"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/database/storage"
)

type Storage interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
}

type Database struct {
	compute *compute.Compute
	storage Storage
}

func NewDatabase(cfg *configuration.Config) (*Database, error) {
	compute := compute.NewCompute()
	storage, err := storage.NewStorage(&cfg.Engine)
	if err != nil {
		return nil, err
	}
	return &Database{compute: compute, storage: storage}, nil
}

func (db *Database) Start(ctx context.Context) error {
	return nil
}

func (db *Database) HandleRequest(ctx context.Context, data []byte) []byte {
	return []byte("response")
}

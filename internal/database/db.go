package database

import (
	"context"
	"errors"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/database/compute"
	"github.com/EzhovAndrew/kv-db/internal/database/storage/engine/in_memory"
)

var ErrUnknownEngine = errors.New("unknown engine type")

type Engine interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
}

type Database struct {
	compute *compute.Compute
	engine  Engine
}

func NewDatabase(cfg *configuration.Config) (*Database, error) {
	compute := compute.NewCompute()
	var engine Engine
	switch cfg.Engine.Type {
	case configuration.EngineInMemoryKey:
		inMemoryEngine, err := in_memory.NewEngine()
		if err != nil {
			return nil, err
		}
		engine = inMemoryEngine
	default:
		return nil, ErrUnknownEngine
	}
	return &Database{compute: compute, engine: engine}, nil
}

func (db *Database) Start(ctx context.Context) error {
	return nil
}

func (db *Database) HandleRequest(ctx context.Context, data []byte) []byte {
	return []byte("response")
}

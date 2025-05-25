package initialization

import (
	"context"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
)

type Initializer struct {
	Config *configuration.Config
}

func NewInitializer(cfg *configuration.Config) (*Initializer, error) {
	return &Initializer{
		Config: cfg,
	}, nil
}

func (i *Initializer) StartDatabase(ctx context.Context) error {
	return nil
}

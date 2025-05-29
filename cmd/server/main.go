package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/EzhovAndrew/kv-db/internal/configuration"
	"github.com/EzhovAndrew/kv-db/internal/initialization"
	"github.com/EzhovAndrew/kv-db/internal/logging"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := configuration.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
	logging.Init(&cfg.Logging)
	logging.Info("Parse config")

	initializer, err := initialization.NewInitializer(cfg)
	if err != nil {
		logging.Fatal(err.Error())
	}

	logging.Info("Start database")
	if err := initializer.StartDatabase(ctx); err != nil {
		logging.Fatal(err.Error())
	}
}

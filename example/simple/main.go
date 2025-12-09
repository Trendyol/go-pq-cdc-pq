package main

import (
	"context"
	"log"
	"log/slog"
	"os"

	gopqcdcpq "github.com/Trendyol/go-pq-cdc-pq"
	"github.com/Trendyol/go-pq-cdc-pq/config"
	slogctx "github.com/veqryn/slog-context"
)

func main() {
	// Setup logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx := slogctx.Append(context.Background(), "logger", logger)

	// Load configuration
	configPath := os.Getenv("CONFIG_FILE")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create connector
	connector, err := gopqcdcpq.NewConnector(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to create connector: %v", err)
	}
	defer connector.Close()

	// Start connector
	connector.Start(ctx)

	// Wait for shutdown signal
	if err := connector.WaitForShutdown(ctx); err != nil {
		log.Fatalf("Error waiting for shutdown: %v", err)
	}

	logger.Info("Shutting down gracefully...")
}

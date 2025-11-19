package app

import (
	"context"
	"fmt"
	"log/slog"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-pq/internal/config"
	"github.com/Trendyol/go-pq-cdc-pq/internal/database"
	"github.com/Trendyol/go-pq-cdc-pq/pq/connector"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	slogctx "github.com/veqryn/slog-context"
)

// =============================================================================
// CDC Service Functions
// =============================================================================

// StartService initializes and starts the CDC service
func StartService(ctx context.Context, cfg *config.Config, pubTable []publication.Table) error {
	return StartCDC(ctx, cfg, pubTable)
}

// StartCDC initializes and starts the complete CDC service
func StartCDC(ctx context.Context, cfg *config.Config, pubTable []publication.Table) error {
	log := slogctx.FromCtx(ctx)

	// Add service context
	ctx = slogctx.Append(ctx, "service", "cdc")

	// Build CDC configuration
	cdcConfig := BuildCDCConfig(cfg, pubTable)

	// Create message channel for communication between components
	messages := make(chan connector.Message, cfg.Connector.BulkSize)

	// Initialize database pool for target database
	pool, err := database.TargetDatabasePool(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to create database pool: %w", err)
	}

	// Create batch executor context
	batchCtx := slogctx.Append(ctx, "component", "BatchExecutor")

	// Create and start batch executor
	batchConfig := CreateBatchConfig(cfg)
	batchExecutor := connector.NewBatchExecutor(
		pool,
		batchConfig,
		slog.Default().With("component", "BatchExecutor"),
	)
	go batchExecutor.Start(batchCtx, messages)

	// Create replication listener context
	listenerCtx := slogctx.Append(ctx, "component", "ReplicationListener")

	// Create replication listener
	listener := connector.NewReplicationListener(
		messages,
		connector.DefaultPrimarykey,
	)

	// Create and start CDC connector
	cdcConnector, err := cdc.NewConnector(ctx, cdcConfig, listener.ListenerFunc(listenerCtx))
	if err != nil {
		return fmt.Errorf("failed to create CDC connector: %w", err)
	}

	log.Info("Starting CDC service...")
	cdcConnector.Start(ctx)
	return nil
}

package app

import (
	"fmt"
	"log/slog"

	"github.com/Trendyol/go-pq-cdc-pq/internal/config"
	"github.com/Trendyol/go-pq-cdc-pq/pq/connector"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

// =============================================================================
// Configuration Functions
// =============================================================================

// CreatePublicationTables defines the tables to be replicated
func CreatePublicationTables(cfg *config.Config) []publication.Table {
	return []publication.Table{
		{
			Name:            cfg.Connector.Table,
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Schema:          cfg.Connector.Schema,
		},
	}
}

// CreateBatchConfig creates batch executor configuration
func CreateBatchConfig(cfg *config.Config) connector.BatchConfig {
	return connector.BatchConfig{
		BulkSize:   cfg.Connector.BulkSize,
		Timeout:    cfg.Connector.Timeout,
		MaxRetries: cfg.Connector.MaxRetries,
		RetryDelay: cfg.Connector.RetryDelay,
	}
}

// BuildCDCConfig creates the CDC configuration from app config
func BuildCDCConfig(cfg *config.Config, pubTable []publication.Table) cdcconfig.Config {
	source := cfg.Postgres.Source
	return cdcconfig.Config{
		DebugMode: cfg.Connector.DebugMode,
		Host:      fmt.Sprintf("%s:%d", source.Host, source.Port),
		Username:  source.User,
		Password:  source.Password,
		Database:  source.DB,
		Publication: publication.Config{
			CreateIfNotExists: cfg.Connector.CreateIfNotExists,
			Name:              cfg.Connector.Publication,
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationTruncate,
				publication.OperationUpdate,
			},
			Tables: pubTable,
		},
		Slot: slot.Config{
			CreateIfNotExists:           cfg.Connector.CreateIfNotExists,
			Name:                        cfg.Connector.Slot,
			SlotActivityCheckerInterval: cfg.Connector.SlotActivityCheckerInterval,
		},
		Metric: cdcconfig.MetricConfig{
			Port: cfg.Server.Port,
		},
		Logger: cdcconfig.LoggerConfig{
			LogLevel: resolveLoggerLevel(cfg.Connector.LogLevel),
		},
	}
}

func resolveLoggerLevel(level int) slog.Level {
	switch level {
	case 0:
		return slog.LevelDebug
	case 1:
		return slog.LevelInfo
	case 2:
		return slog.LevelWarn
	case 3:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

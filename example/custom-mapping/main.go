package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"

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

	// Create connector with custom mapper
	connector, err := gopqcdcpq.NewConnector(ctx, cfg,
		gopqcdcpq.WithMapper(customMapper),
	)
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

// customMapper transforms CDC events from source 'users' table
// to target 'user_profiles' table with different column names:
//   - id -> user_id
//   - name -> full_name
//   - email -> email_address
//   - created_at -> registered_at
//   - updated_at -> last_modified_at
func customMapper(event *gopqcdcpq.Message) []gopqcdcpq.QueryAction {
	// Only process events from 'users' table
	if event.TableName != "users" {
		return nil
	}

	switch event.Type {
	case gopqcdcpq.InsertMessage, gopqcdcpq.UpdateMessage, gopqcdcpq.SnapshotMessage:
		return handleUpsert(event)
	case gopqcdcpq.DeleteMessage:
		return handleDelete(event)
	default:
		return nil
	}
}

func handleUpsert(event *gopqcdcpq.Message) []gopqcdcpq.QueryAction {
	data := event.NewData
	if data == nil {
		return nil
	}

	// Map source columns to target columns
	query := `
		INSERT INTO user_profiles (user_id, full_name, email_address, registered_at, last_modified_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (user_id) DO UPDATE SET
			full_name = EXCLUDED.full_name,
			email_address = EXCLUDED.email_address,
			registered_at = EXCLUDED.registered_at,
			last_modified_at = EXCLUDED.last_modified_at`

	args := []any{
		data["id"],         // user_id
		data["name"],       // full_name
		data["email"],      // email_address
		data["created_at"], // registered_at
		data["updated_at"], // last_modified_at
	}

	return []gopqcdcpq.QueryAction{{
		Query: strings.TrimSpace(query),
		Args:  args,
	}}
}

func handleDelete(event *gopqcdcpq.Message) []gopqcdcpq.QueryAction {
	data := event.OldData
	if data == nil {
		return nil
	}

	id, ok := data["id"]
	if !ok {
		return nil
	}

	query := fmt.Sprintf("DELETE FROM user_profiles WHERE user_id = $1")

	return []gopqcdcpq.QueryAction{{
		Query: query,
		Args:  []any{id},
	}}
}

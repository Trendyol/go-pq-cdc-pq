package database

import (
	"context"
	"fmt"

	"github.com/Trendyol/go-pq-cdc-pq/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool defines the interface for database connection pool operations.
// It abstracts the underlying database driver implementation.
type Pool interface {
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Close()
}

// NewTargetPool creates a new database connection pool for the target database.
func NewTargetPool(ctx context.Context, cfg *config.Connector) (Pool, error) {
	poolConfig, err := buildPGXConfig(cfg.Postgres.Target)
	if err != nil {
		return nil, fmt.Errorf("build pgx config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	return &pgxPool{pool: pool}, nil
}

func (p *pgxPool) Close() {
	p.pool.Close()
}

// pgxPool implements the Pool interface using pgxpool.
type pgxPool struct {
	pool *pgxpool.Pool
}

func (p *pgxPool) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return p.pool.SendBatch(ctx, batch)
}

func (p *pgxPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

func (p *pgxPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.pool.Query(ctx, sql, args...)
}

func (p *pgxPool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return p.pool.Exec(ctx, sql, arguments...)
}

// buildPGXConfig creates a pgxpool.Config from the application database configuration.
func buildPGXConfig(db config.DatabaseConfig) (*pgxpool.Config, error) {
	port := db.Port
	if port == 0 {
		port = 5432
	}

	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		db.User,
		db.Password,
		db.Host,
		port,
		db.DB)

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}

	return config, nil
}

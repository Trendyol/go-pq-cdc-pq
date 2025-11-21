package database

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/Trendyol/go-pq-cdc-pq/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabasePool interface for testability
type DatabasePool interface {
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (DatabaseTx, error)
	Close()
}

// DatabaseTx interface for database transactions
type DatabaseTx interface {
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Rows interface for query results
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close()
}

// TargetDatabasePool creates a new database connection pool for target database
func TargetDatabasePool(ctx context.Context, cfg *config.Connector) (DatabasePool, error) {
	connString := buildConnString(cfg.Postgres.Target)

	slog.Info("Connecting to PostgreSQL target...", "connString", connString)
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &PoolWrapper{pool: pool}, nil
}

// SourceDatabasePool creates a new database connection pool for source database
func SourceDatabasePool(ctx context.Context, cfg *config.Connector) (DatabasePool, error) {
	connString := buildConnString(cfg.Postgres.Source)

	slog.Info("Connecting to PostgreSQL source...", "connString", connString)
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &PoolWrapper{pool: pool}, nil
}

// NewDatabasePool creates a new database connection pool with custom connection string
func NewDatabasePool(ctx context.Context, connString string) (DatabasePool, error) {
	slog.Info("Connecting to PostgreSQL...", "connString", connString)
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &PoolWrapper{pool: pool}, nil
}

// PoolWrapper wraps pgxpool.Pool to implement types.DatabasePool interface
type PoolWrapper struct {
	pool *pgxpool.Pool
}

func (p *PoolWrapper) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return p.pool.SendBatch(ctx, batch)
}

func (p *PoolWrapper) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

func (p *PoolWrapper) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.pool.Query(ctx, sql, args...)
}

func (p *PoolWrapper) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return p.pool.Exec(ctx, sql, arguments...)
}

func (p *PoolWrapper) Begin(ctx context.Context) (DatabaseTx, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &TxWrapper{tx: tx}, nil
}

func (p *PoolWrapper) Close() {
	p.pool.Close()
}

// TxWrapper wraps pgx.Tx to implement types.DatabaseTx interface
type TxWrapper struct {
	tx pgx.Tx
}

func (t *TxWrapper) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return t.tx.SendBatch(ctx, batch)
}

func (t *TxWrapper) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

func (t *TxWrapper) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.tx.Query(ctx, sql, args...)
}

func (t *TxWrapper) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return t.tx.Exec(ctx, sql, arguments...)
}

func (t *TxWrapper) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *TxWrapper) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

func buildConnString(db config.DatabaseConfig) string {
	port := db.Port
	if port == 0 {
		port = 5432
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		db.User,
		db.Password,
		db.Host,
		strconv.Itoa(port),
		db.DB)
}

package gopqcdcpq

import (
	"context"
	"log/slog"
	"time"

	"github.com/Trendyol/go-pq-cdc-pq/config"

	"github.com/jackc/pgx/v5"
	slogctx "github.com/veqryn/slog-context"

	"github.com/Trendyol/go-pq-cdc-pq/internal/database"
	"github.com/Trendyol/go-pq-cdc-pq/internal/sqlutil"
)

// TargetPQ applies CDC messages to the target PostgreSQL database.
type TargetPQ struct {
	pool   database.DatabasePool
	config config.BatchConfig

	// Current buffered statements to be flushed.
	queue []*pgx.QueuedQuery
	acks  []func() error
}

// NewTargetPQ instantiates a TargetPQ.
func NewTargetPQ(pool database.DatabasePool, cfg config.BatchConfig, logger *slog.Logger) *TargetPQ {
	return &TargetPQ{
		pool:   pool,
		config: cfg,
		queue:  make([]*pgx.QueuedQuery, 0, cfg.BulkSize),
		acks:   make([]func() error, 0, cfg.BulkSize),
	}
}

// Start consumes messages and applies them to the target database.
func (t *TargetPQ) Start(ctx context.Context, messages <-chan Message) {
	ctx = slogctx.Append(ctx, "component", "TargetPQ")
	ticker := time.NewTicker(t.config.Timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(t.queue) > 0 {
				t.flush(ctx)
			}
			return

		case message := <-messages:
			t.enqueue(message)
			if len(t.queue) >= t.config.BulkSize {
				t.flush(ctx)
			}

		case <-ticker.C:
			if len(t.queue) > 0 {
				t.flush(ctx)
			}
		}
	}
}

// enqueue buffers a CDC message for later application.
func (t *TargetPQ) enqueue(message Message) {
	t.queue = append(t.queue, &pgx.QueuedQuery{
		SQL:       message.Query,
		Arguments: message.Args,
	})
	t.acks = append(t.acks, message.Ack)
}

// flush writes buffered statements to the target database.
func (t *TargetPQ) flush(ctx context.Context) {
	if len(t.queue) == 0 {
		return
	}

	log := slogctx.FromCtx(ctx)
	queryCount := len(t.queue)

	ctx = slogctx.Append(ctx,
		"queryCount", queryCount,
		"maxRetries", t.config.MaxRetries)

	log.Info("flushing CDC statements to target database")

	lastAck := t.acks[len(t.acks)-1]
	err := t.executeWithRetry(ctx, t.queue, lastAck)

	if err != nil {
		log.Error("failed to flush CDC statements to target database",
			"error", err,
			"queryCount", queryCount)
	} else {
		log.Info("cdc statements flushed to target database", "queryCount", queryCount)
	}

	t.resetQueue()
}

// resetQueue clears buffered statements.
func (t *TargetPQ) resetQueue() {
	t.queue = make([]*pgx.QueuedQuery, 0, t.config.BulkSize)
	t.acks = make([]func() error, 0, t.config.BulkSize)
}

// executeWithRetry flushes buffered statements with retry logic.
func (t *TargetPQ) executeWithRetry(ctx context.Context, queue []*pgx.QueuedQuery, ackFunc func() error) error {
	var lastErr error
	for attempt := 0; attempt < t.config.MaxRetries; attempt++ {
		attemptCtx := slogctx.Append(ctx, "attempt", attempt+1)
		attemptLog := slogctx.FromCtx(attemptCtx)

		attemptLog.Debug("applying CDC statements to target database")

		if err := t.executeBatch(attemptCtx, queue, ackFunc); err != nil {
			lastErr = err
			if attempt < t.config.MaxRetries-1 {
				attemptLog.Warn("target apply attempt failed, will retry",
					"error", err)

				time.Sleep(t.config.RetryDelay * time.Duration(attempt+1))
				continue
			}

			attemptLog.Error("target apply failed after all retries",
				"error", err)
		} else {
			attemptLog.Info("cdc statements applied to target database")
			return nil
		}
	}

	return lastErr
}

// executeBatch sends a single batch of statements to the target database.
func (t *TargetPQ) executeBatch(ctx context.Context, queue []*pgx.QueuedQuery, ackFunc func() error) error {
	if len(queue) == 0 {
		return nil
	}

	log := slogctx.FromCtx(ctx)
	log.Debug("executing target database batch")

	results := t.pool.SendBatch(ctx, &pgx.Batch{QueuedQueries: queue})

	if err := sqlutil.ExecBatch(ctx, results, len(queue)); err != nil {
		return err
	}

	if err := ackFunc(); err != nil {
		log.Error("failed to acknowledge target apply", "error", err)
		return err
	}

	return nil
}

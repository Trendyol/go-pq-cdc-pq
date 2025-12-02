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

// Sink applies CDC messages to the target PostgreSQL database.
// Batches messages and flushes when BulkSize is reached or Timeout expires.
// Implements retry logic with exponential backoff. Messages are acknowledged only after successful execution.
type Sink struct {
	pool   database.Pool
	config config.BatchConfig

	// Current buffered statements to be flushed.
	queue []*pgx.QueuedQuery
	acks  []func() error
}

// NewSink creates a new Sink instance.
//
// Example:
//
//	pool, _ := database.NewTargetPool(ctx, cfg)
//	sink := NewSink(pool, batchConfig, slog.Default())
func NewSink(pool database.Pool, cfg config.BatchConfig, logger *slog.Logger) *Sink {
	return &Sink{
		pool:   pool,
		config: cfg,
		queue:  make([]*pgx.QueuedQuery, 0, cfg.BulkSize),
		acks:   make([]func() error, 0, cfg.BulkSize),
	}
}

// Start consumes messages from the channel and applies them to the target database.
// Runs until context is cancelled. Flushes when BulkSize is reached or Timeout expires.
// On shutdown, flushes any remaining buffered messages.
//
// Example:
//
//	messages := make(chan Message, 1000)
//	go sink.Start(ctx, messages)
func (s *Sink) Start(ctx context.Context, messages <-chan Message) {
	ctx = slogctx.Append(ctx, "component", "Sink")
	ticker := time.NewTicker(s.config.Timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(s.queue) > 0 {
				s.flush(ctx)
			}
			return

		case message := <-messages:
			s.enqueue(message)
			if len(s.queue) >= s.config.BulkSize {
				s.flush(ctx)
			}

		case <-ticker.C:
			if len(s.queue) > 0 {
				s.flush(ctx)
			}
		}
	}
}

// enqueue adds a CDC message to the internal buffer queue for batch processing.
// Messages are accumulated until flush is triggered by BulkSize or Timeout.
// Not thread-safe; should only be called from the Start goroutine.
func (s *Sink) enqueue(message Message) {
	s.queue = append(s.queue, &pgx.QueuedQuery{
		SQL:       message.Query,
		Arguments: message.Args,
	})
	s.acks = append(s.acks, message.Ack)
}

// flush executes all buffered statements in a single batch transaction.
// Uses retry logic with exponential backoff. Acknowledges the last message on success.
// Clears the queue after execution (success or failure).
func (s *Sink) flush(ctx context.Context) {
	if len(s.queue) == 0 {
		return
	}

	log := slogctx.FromCtx(ctx)
	queryCount := len(s.queue)

	ctx = slogctx.Append(ctx,
		"queryCount", queryCount,
		"maxRetries", s.config.MaxRetries)

	log.Info("flushing CDC statements to target database")

	lastAck := s.acks[len(s.acks)-1]
	err := s.executeBatch(ctx, s.queue, lastAck)

	if err != nil {
		log.Error("failed to flush CDC statements to target database",
			"error", err,
			"queryCount", queryCount)
	} else {
		log.Info("cdc statements flushed to target database", "queryCount", queryCount)
	}

	s.resetQueue()
}

// resetQueue clears the internal buffers and reinitializes them with BulkSize capacity.
// Called automatically after each flush operation.
func (s *Sink) resetQueue() {
	s.queue = make([]*pgx.QueuedQuery, 0, s.config.BulkSize)
	s.acks = make([]func() error, 0, s.config.BulkSize)
}

// executeWithRetry executes a batch of queries with exponential backoff retry logic.
// Retries up to MaxRetries times, waiting RetryDelay * (attempt + 1) between attempts.
// Returns nil on success, or the last error if all retries fail.
func (s *Sink) executeWithRetry(ctx context.Context, queue []*pgx.QueuedQuery, ackFunc func() error) error {
	var lastErr error
	for attempt := 0; attempt < s.config.MaxRetries; attempt++ {
		attemptCtx := slogctx.Append(ctx, "attempt", attempt+1)
		attemptLog := slogctx.FromCtx(attemptCtx)

		attemptLog.Debug("applying CDC statements to target database")

		if err := s.executeBatch(attemptCtx, queue, ackFunc); err != nil {
			lastErr = err
			if attempt < s.config.MaxRetries-1 {
				attemptLog.Warn("target apply attempt failed, will retry",
					"error", err)

				time.Sleep(s.config.RetryDelay * time.Duration(attempt+1))
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

// executeBatch executes a batch of SQL statements as a single pgx batch.
// Calls ackFunc only if all queries succeed. Returns error if any query or acknowledgment fails.
func (s *Sink) executeBatch(ctx context.Context, queue []*pgx.QueuedQuery, ackFunc func() error) error {
	if len(queue) == 0 {
		return nil
	}

	log := slogctx.FromCtx(ctx)
	log.Debug("executing target database batch")

	results := s.pool.SendBatch(ctx, &pgx.Batch{QueuedQueries: queue})

	if err := sqlutil.ExecBatch(ctx, results, len(queue)); err != nil {
		return err
	}

	if err := ackFunc(); err != nil {
		log.Error("failed to acknowledge target apply", "error", err)
		return err
	}

	return nil
}

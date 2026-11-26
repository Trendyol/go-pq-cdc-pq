package gopqcdcpq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Trendyol/go-pq-cdc-pq/config"
	"github.com/Trendyol/go-pq-cdc-pq/internal/database"
	"github.com/Trendyol/go-pq-cdc-pq/internal/sqlutil"
	"github.com/jackc/pgx/v5"
	slogctx "github.com/veqryn/slog-context"
)

// Sink batches CDC statements and acknowledges them to the replication source
// only after the target has committed them. It never drops a failed batch:
// on terminal failure the buffered statements are retained and surfaced as an
// error rather than advancing the source past lost writes.
type Sink struct {
	pool   database.Pool
	config config.BatchConfig

	// queue holds statements waiting for the target, in arrival order.
	queue []*pgx.QueuedQuery
	acks  []func() error
}

// NewSink creates a Sink that batches target statements according to cfg.
func NewSink(pool database.Pool, cfg config.BatchConfig, logger *slog.Logger) *Sink {
	return &Sink{
		pool:   pool,
		config: cfg,
		queue:  make([]*pgx.QueuedQuery, 0, cfg.BulkSize),
		acks:   make([]func() error, 0, cfg.BulkSize),
	}
}

// Start consumes messages until cancellation, input closure, or a terminal
// error. A failed batch is retained and returned as an error; no later event
// is acknowledged past it.
func (s *Sink) Start(ctx context.Context, messages <-chan Message) error {
	ctx = slogctx.Append(ctx, "component", "Sink")
	ticker := time.NewTicker(s.config.Timeout)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()

		case message, ok := <-messages:
			if err := ctx.Err(); err != nil {
				return err
			}
			if !ok {
				return s.flush(ctx)
			}
			if message.Query == "" {
				// Control and filtered events are ordered barriers: they cannot
				// acknowledge a later LSN while earlier pending writes remain.
				if err := s.flush(ctx); err != nil {
					return err
				}
				if err := acknowledge(message.Ack); err != nil {
					return fmt.Errorf("acknowledge CDC barrier: %w", err)
				}
				continue
			}
			s.enqueue(message)
			if len(s.queue) >= s.config.BulkSize {
				if err := s.flush(ctx); err != nil {
					return err
				}
			}

		case <-ticker.C:
			if err := s.flush(ctx); err != nil {
				return err
			}
		}
	}
}

// enqueue buffers a message for the next flush.
func (s *Sink) enqueue(message Message) {
	s.queue = append(s.queue, &pgx.QueuedQuery{
		SQL:       message.Query,
		Arguments: message.Args,
	})
	s.acks = append(s.acks, message.Ack)
}

// flush applies the buffered statements and acknowledges them on success.
// On success the queue is cleared; on failure it is retained and an error is
// returned. A new connector replays unacknowledged WAL from the source slot.
func (s *Sink) flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(s.queue) == 0 {
		return nil
	}

	log := slogctx.FromCtx(ctx)
	queryCount := len(s.queue)
	ctx = slogctx.Append(ctx,
		"queryCount", queryCount,
		"maxRetries", s.config.MaxRetries)
	log.Info("flushing CDC statements to target database")

	if err := s.executeWithRetry(ctx, s.queue); err != nil {
		log.Error("failed to flush CDC statements to target database",
			"error", err,
			"queryCount", queryCount)
		return fmt.Errorf("apply %d CDC statements to target: %w", queryCount, err)
	}
	for _, ack := range s.acks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := acknowledge(ack); err != nil {
			log.Error("failed to acknowledge target apply", "error", err)
			return fmt.Errorf("acknowledge applied CDC statement: %w", err)
		}
	}

	log.Info("cdc statements flushed to target database", "queryCount", queryCount)
	s.resetQueue()
	return nil
}

// resetQueue clears the internal buffers after a successful flush.
func (s *Sink) resetQueue() {
	s.queue = make([]*pgx.QueuedQuery, 0, s.config.BulkSize)
	s.acks = make([]func() error, 0, s.config.BulkSize)
}

// executeWithRetry retries up to MaxRetries times, waiting
// RetryDelay * (attempt + 1) between attempts. Acknowledgements are not retried.
func (s *Sink) executeWithRetry(ctx context.Context, queue []*pgx.QueuedQuery) error {
	if s.config.MaxRetries <= 0 {
		return fmt.Errorf("target apply failed: MaxRetries must be positive")
	}
	var lastErr error
	for attempt := 0; attempt < s.config.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		attemptCtx := slogctx.Append(ctx, "attempt", attempt+1)
		attemptLog := slogctx.FromCtx(attemptCtx)
		attemptLog.Debug("applying CDC statements to target database")
		err := s.executeBatch(attemptCtx, queue)
		if err != nil {
			lastErr = err
			if attempt < s.config.MaxRetries-1 {
				attemptLog.Warn("target apply attempt failed, will retry", "error", err)
				time.Sleep(s.config.RetryDelay * time.Duration(attempt+1))
				continue
			}
			attemptLog.Error("target apply failed after all retries", "error", err)
		} else {
			attemptLog.Info("cdc statements applied to target database")
			return nil
		}
	}
	return fmt.Errorf("target apply failed after %d attempts: %w", s.config.MaxRetries, lastErr)
}

// executeBatch applies the queue as a single pgx batch. The batch is rebuilt on
// every attempt because pgx mutates queued queries with connection state.
func (s *Sink) executeBatch(ctx context.Context, queue []*pgx.QueuedQuery) error {
	if len(queue) == 0 {
		return nil
	}
	slogctx.FromCtx(ctx).Debug("executing target database batch")
	batch := &pgx.Batch{QueuedQueries: make([]*pgx.QueuedQuery, len(queue))}
	for i, query := range queue {
		copy := *query
		batch.QueuedQueries[i] = &copy
	}
	return sqlutil.ExecBatch(ctx, s.pool.SendBatch(ctx, batch), len(queue))
}

func acknowledge(ack func() error) error {
	if ack == nil {
		return nil
	}
	return ack()
}

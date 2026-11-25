package gopqcdcpq

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
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
// error so the process stops rather than advancing the source past lost writes.
type Sink struct {
	pool   database.Pool
	config config.BatchConfig
	logger *slog.Logger

	// queue holds statements waiting for the target, in arrival order.
	queue []pendingStatement
}

type pendingStatement struct {
	query string
	args  []any
	ack   func() error
}

// NewSink creates a Sink that batches target statements according to cfg.
func NewSink(pool database.Pool, cfg config.BatchConfig, logger *slog.Logger) *Sink {
	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = config.DefaultApplyTimeout
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Sink{
		pool:   pool,
		config: cfg,
		logger: logger,
		queue:  make([]pendingStatement, 0, cfg.BulkSize),
	}
}

// Start consumes messages until cancellation, input closure, or a terminal
// error. A failed batch is retained and returned as an error; no later event
// is acknowledged past it.
func (s *Sink) Start(ctx context.Context, messages <-chan Message) error {
	if err := s.config.Validate(); err != nil {
		return err
	}
	if s.pool == nil {
		return fmt.Errorf("target database pool is required")
	}
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
	s.queue = append(s.queue, pendingStatement{
		query: message.Query,
		args:  message.Args,
		ack:   message.Ack,
	})
}

// flush applies the buffered statements and acknowledges them on success.
// On success the queue is cleared; on failure it is retained so retries and a
// restart can replay it, and an error is returned.
func (s *Sink) flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(s.queue) == 0 {
		return nil
	}

	queryCount := len(s.queue)
	if err := s.executeWithRetry(ctx, s.queue); err != nil {
		return fmt.Errorf("apply %d CDC statements to target: %w", queryCount, err)
	}
	for _, statement := range s.queue {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := acknowledge(statement.ack); err != nil {
			return fmt.Errorf("acknowledge applied CDC statement: %w", err)
		}
	}

	clear(s.queue)
	s.queue = s.queue[:0]
	return nil
}

// executeWithRetry applies the queue, retrying up to MaxRetries times. Each
// attempt is bounded by ApplyTimeout so a silently unreachable target cannot
// stall the batch forever. It returns the last error if all attempts fail; the
// queue is kept intact.
func (s *Sink) executeWithRetry(ctx context.Context, queue []pendingStatement) error {
	var lastErr error
	for attempt := 0; attempt < s.config.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		attemptCtx := slogctx.Append(ctx, "attempt", attempt+1)
		attemptCtx, cancel := context.WithTimeout(attemptCtx, s.config.ApplyTimeout)
		err := s.executeBatch(attemptCtx, queue)
		cancel()
		if err != nil {
			lastErr = err
			if attempt == s.config.MaxRetries-1 {
				break
			}
			s.logger.Warn("target apply attempt failed, will retry", "attempt", attempt+1, "error", err)
			time.Sleep(s.config.RetryDelay * time.Duration(attempt+1))
			continue
		}
		return nil
	}
	err := fmt.Errorf("target apply failed after %d attempts: %w", s.config.MaxRetries, lastErr)
	s.logger.Error("target apply failed after all retries", "error", err)
	return err
}

// executeBatch applies the queue as a single pgx batch. The batch is rebuilt on
// every attempt because pgx mutates queued queries with connection state.
func (s *Sink) executeBatch(ctx context.Context, queue []pendingStatement) error {
	batch := &pgx.Batch{}
	for _, statement := range queue {
		batch.Queue(statement.query, slices.Clone(statement.args)...)
	}
	return sqlutil.ExecBatch(ctx, s.pool.SendBatch(ctx, batch), len(queue))
}

func acknowledge(ack func() error) error {
	if ack == nil {
		return nil
	}
	return ack()
}

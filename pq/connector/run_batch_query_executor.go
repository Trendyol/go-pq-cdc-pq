package connector

import (
	"context"
	"log/slog"
	"time"

	"github.com/Trendyol/go-pq-cdc-pq/internal/tracer"

	"github.com/jackc/pgx/v5"
	slogctx "github.com/veqryn/slog-context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Trendyol/go-pq-cdc-pq/internal/database"
	"github.com/Trendyol/go-pq-cdc-pq/internal/sqlutil"
)

// BatchExecutor handles batch processing of CDC messages
type BatchExecutor struct {
	pool   database.DatabasePool
	config BatchConfig

	// Current batch state
	queue []*pgx.QueuedQuery
	acks  []func() error
}

// NewBatchExecutor creates a new batch executor
func NewBatchExecutor(pool database.DatabasePool, config BatchConfig, logger *slog.Logger) *BatchExecutor {
	return &BatchExecutor{
		pool:   pool,
		config: config,
		queue:  make([]*pgx.QueuedQuery, 0, config.BulkSize),
		acks:   make([]func() error, 0, config.BulkSize),
	}
}

// Start starts the batch executor to process messages
func (b *BatchExecutor) Start(ctx context.Context, messages <-chan Message) {
	// Create context with component information for structured logging

	ctx = slogctx.Append(ctx, "component", "BatchExecutor")
	ticker := time.NewTicker(b.config.Timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Process remaining messages before shutdown
			if len(b.queue) > 0 {
				b.processBatch(ctx)
			}
			return

		case message := <-messages:
			b.addMessage(message)

			// Process batch if it's full
			if len(b.queue) >= b.config.BulkSize {
				b.processBatch(ctx)
			}

		case <-ticker.C:
			// Process batch on timeout if there are pending messages
			if len(b.queue) > 0 {
				b.processBatch(ctx)
			}
		}
	}
}

// addMessage adds a message to the current batch
func (b *BatchExecutor) addMessage(message Message) {
	b.queue = append(b.queue, &pgx.QueuedQuery{
		SQL:       message.Query,
		Arguments: message.Args,
	})
	b.acks = append(b.acks, message.Ack)
}

// processBatch processes the current batch
func (b *BatchExecutor) processBatch(ctx context.Context) {
	if len(b.queue) == 0 {
		return
	}

	log := slogctx.FromCtx(ctx)
	batchSize := len(b.queue)

	// Add batch context
	ctx = slogctx.Append(ctx,
		"batchSize", batchSize,
		"maxRetries", b.config.MaxRetries)

	log.Info("Processing batch")

	// Get the last ack function (represents the batch completion)
	lastAck := b.acks[len(b.acks)-1]

	// Execute batch with retry logic
	err := b.executeBatchWithRetry(ctx, b.queue, lastAck)

	if err != nil {
		log.Error("Failed to execute batch after all retries",
			"error", err,
			"batchSize", batchSize)
	} else {
		log.Info("Batch processed successfully", "batchSize", batchSize)
	}

	// Reset batch state
	b.resetBatch()
}

// resetBatch resets the batch state
func (b *BatchExecutor) resetBatch() {
	b.queue = make([]*pgx.QueuedQuery, 0, b.config.BulkSize)
	b.acks = make([]func() error, 0, b.config.BulkSize)
}

// executeBatchWithRetry executes a batch with retry logic
func (b *BatchExecutor) executeBatchWithRetry(ctx context.Context, queue []*pgx.QueuedQuery, ackFunc func() error) error {

	ctx, span := tracer.StartSpan(ctx, "batch.execute_with_retry",
		trace.WithAttributes(attribute.Int("retry.max_attempts", b.config.MaxRetries)))
	defer span.End()

	var lastErr error
	for attempt := 0; attempt < b.config.MaxRetries; attempt++ {
		attemptCtx := slogctx.Append(ctx, "attempt", attempt+1)
		attemptLog := slogctx.FromCtx(attemptCtx)

		attemptLog.Debug("Attempting batch execution")

		if err := b.executeBatch(attemptCtx, queue, ackFunc); err != nil {
			lastErr = err
			if attempt < b.config.MaxRetries-1 {
				attemptLog.Warn("Batch execution attempt failed, will retry",
					"error", err)

				// Wait before retry
				time.Sleep(b.config.RetryDelay * time.Duration(attempt+1))
				continue
			} else {
				attemptLog.Error("Batch execution failed after all retries",
					"error", err)
			}
		} else {
			// Success
			attemptLog.Info("Batch execution succeeded")
			span.SetStatus(codes.Ok, "Batch executed successfully")
			return nil
		}
	}

	// All retries failed
	span.RecordError(lastErr)
	span.SetStatus(codes.Error, "Batch execution failed after all retries")
	return lastErr
}

// executeBatch executes a single batch attempt
func (b *BatchExecutor) executeBatch(ctx context.Context, queue []*pgx.QueuedQuery, ackFunc func() error) error {
	if len(queue) == 0 {
		return nil
	}

	log := slogctx.FromCtx(ctx)

	// Add database operation context
	ctx = slogctx.Append(ctx,
		"operation", "batch_execute",
		"table", "multiple_tables",
		"schema", DefaultSchema)

	ctx, span := tracer.StartSpan(ctx, "db.batch_execute",
		trace.WithAttributes(attribute.String("db.operation", "batch_execute"),
			attribute.String("db.sql.table", "multiple_tables"),
			attribute.String("db.schema", DefaultSchema),
			attribute.String("db.system", "postgresql")))
	defer span.End()

	log.Debug("Executing database batch operation")

	startTime := time.Now()

	// Send batch to database
	batchResults := b.pool.SendBatch(ctx, &pgx.Batch{QueuedQueries: queue})

	// Execute all queries in batch
	if err := sqlutil.ExecBatch(ctx, batchResults, len(queue)); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Batch execution failed")
		return err
	}

	// Add timing information to current span
	duration := time.Since(startTime)
	span.SetAttributes(
		attribute.Int64("batch_execution.duration_ms", duration.Milliseconds()),
		attribute.String("batch_execution.duration", duration.String()),
	)

	// Acknowledge successful batch
	if err := ackFunc(); err != nil {
		log.Error("Failed to acknowledge batch", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to acknowledge batch")
		return err
	}

	log.Debug("Batch executed successfully", "queryCount", len(queue))
	span.SetStatus(codes.Ok, "Batch executed successfully")
	return nil
}

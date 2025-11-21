package gopqcdcpq

import (
	"context"
	"fmt"
	"log/slog"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-pq/config"
	"github.com/Trendyol/go-pq-cdc-pq/internal/database"
	"github.com/Trendyol/go-pq-cdc-pq/internal/sqlutil"
	"github.com/Trendyol/go-pq-cdc-pq/internal/tracer"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	slogctx "github.com/veqryn/slog-context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	Close()
}

type connector struct {
	cdc           cdc.Connector
	cfg           *config.Connector
	targetPQ      *TargetPQ
	messages      chan Message
	readyCh       chan struct{}
	pool          database.DatabasePool
	primaryKey    string
	defaultSchema string
}

func NewConnector(ctx context.Context, cfg *config.Connector, options ...Option) (Connector, error) {
	cfg.SetDefault()

	// Apply options
	opts := defaultConfig()
	for _, option := range options {
		option(&opts)
	}

	pqConnector := &connector{
		cfg:           cfg,
		readyCh:       make(chan struct{}, 1),
		primaryKey:    opts.PrimaryKey,
		defaultSchema: opts.DefaultSchema,
	}

	// Initialize database pool for target database
	pool, err := database.TargetDatabasePool(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create database pool: %w", err)
	}
	pqConnector.pool = pool

	// Create message channel
	pqConnector.messages = make(chan Message, cfg.ConnectorConfig.BulkSize)

	// Create target query executor
	batchConfig := config.BatchConfig{
		BulkSize:   cfg.BatchConfig.BulkSize,
		Timeout:    cfg.BatchConfig.Timeout,
		MaxRetries: cfg.BatchConfig.MaxRetries,
		RetryDelay: cfg.BatchConfig.RetryDelay,
	}
	pqConnector.targetPQ = NewTargetPQ(
		pool,
		batchConfig,
		slog.Default().With("component", "TargetPQ"),
	)

	// Build CDC configuration

	// Create CDC connector
	pqCDC, err := cdc.NewConnector(ctx, cfg.CDC, pqConnector.listener)
	if err != nil {
		return nil, fmt.Errorf("failed to create CDC connector: %w", err)
	}
	pqConnector.cdc = pqCDC

	return pqConnector, nil
}

func (c *connector) Start(ctx context.Context) {
	go func() {
		log := slogctx.FromCtx(ctx)
		log.Info("waiting for connector start...")

		if err := c.cdc.WaitUntilReady(ctx); err != nil {
			panic(err)
		}

		log.Info("bulk process started")

		// Start target query executor
		targetCtx := slogctx.Append(ctx, "component", "TargetPQ")
		// Convert Message channel to pqconnector.Message channel
		pqMessages := make(chan Message, cap(c.messages))
		go func() {
			for msg := range c.messages {
				pqMessages <- Message{
					Query: msg.Query,
					Args:  msg.Args,
					Ack:   msg.Ack,
				}
			}
			close(pqMessages)
		}()
		go c.targetPQ.Start(targetCtx, pqMessages)

		c.readyCh <- struct{}{}
	}()

	c.cdc.Start(ctx)
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) Close() {
	if !isClosed(c.readyCh) {
		close(c.readyCh)
	}
	c.cdc.Close()
	close(c.messages)
	if c.pool != nil {
		c.pool.Close()
	}
}

func (c *connector) listener(ctx *replication.ListenerContext) {
	// Handle keepalive messages
	if ctx.Message == nil {
		return
	}

	// Create a new root span for each CDC message processing
	msgCtx, span := tracer.StartSpan(context.Background(), "cdc.message_received",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("message.type", fmt.Sprintf("%T", ctx.Message)),
		))
	defer span.End()

	// Process the message
	c.processMessage(msgCtx, ctx)

	span.SetStatus(codes.Ok, "CDC message processing completed")
}

func (c *connector) processMessage(ctx context.Context, replCtx *replication.ListenerContext) {
	log := slogctx.FromCtx(ctx)

	// Start span for message processing
	ctx, span := tracer.StartSpanWithAttrs(ctx, "replication.process_message")
	defer span.End()

	log.Debug("Processing replication message", "messageType", fmt.Sprintf("%T", replCtx.Message))

	switch msg := replCtx.Message.(type) {
	case *format.Insert:
		c.processInsertMessage(ctx, msg, replCtx.Ack)

	case *format.Delete:
		c.processDeleteMessage(ctx, msg, replCtx.Ack)

	case *format.Update:
		c.processUpdateMessage(ctx, msg, replCtx.Ack)

	case *format.Relation:
		log.Debug("Relation message received",
			"namespace", msg.Namespace,
			"table", msg.Name,
			"columns", len(msg.Columns))
		if replCtx.Ack != nil {
			replCtx.Ack()
		}

	default:
		log.Warn("Unknown message type received",
			"type", fmt.Sprintf("%T", msg),
			"message", msg)
	}

	span.SetStatus(codes.Ok, "Message processed successfully")
}

func (c *connector) processInsertMessage(ctx context.Context, msg *format.Insert, ack func() error) {

	msgObj := NewInsertMessage(msg)
	// Set internal fields for target database query processing
	querySQL, args := sqlutil.BuildUpsertQuery(msg.TableName, msg.Decoded, c.primaryKey)
	msgObj.Query = querySQL
	msgObj.Args = args
	msgObj.Ack = ack
	msgObj.Schema = c.defaultSchema
	msgObj.Table = msg.TableName
	msgObj.Action = "INSERT"
	msgObj.OldKeys = nil
	msgObj.NewValues = msg.Decoded

	c.sendMessage(*msgObj)

}

func (c *connector) processDeleteMessage(ctx context.Context, msg *format.Delete, ack func() error) {
	msgObj := NewDeleteMessage(msg)
	// Set internal fields for target database query processing
	querySQL, args := sqlutil.BuildDeleteQuery(msg.TableName, msg.OldDecoded, c.primaryKey)
	msgObj.Query = querySQL
	msgObj.Args = args
	msgObj.Ack = ack
	msgObj.Schema = c.defaultSchema
	msgObj.Table = msg.TableName
	msgObj.Action = "DELETE"
	msgObj.OldKeys = msg.OldDecoded
	msgObj.NewValues = nil

	c.sendMessage(*msgObj)

}

func (c *connector) processUpdateMessage(ctx context.Context, msg *format.Update, ack func() error) {

	msgObj := NewUpdateMessage(msg)
	// Set internal fields for target database query processing
	querySQL, args := sqlutil.BuildUpsertQuery(msg.TableName, msg.NewDecoded, c.primaryKey)
	msgObj.Query = querySQL
	msgObj.Args = args
	msgObj.Ack = ack
	msgObj.Schema = c.defaultSchema
	msgObj.Table = msg.TableName
	msgObj.Action = "UPDATE"
	msgObj.OldKeys = msg.OldDecoded
	msgObj.NewValues = msg.NewDecoded

	c.sendMessage(*msgObj)

}

func (c *connector) sendMessage(message Message) {
	c.messages <- message
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
	}
	return false
}

package gopqcdcpq

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-pq/config"
	"github.com/Trendyol/go-pq-cdc-pq/internal/database"
	"github.com/Trendyol/go-pq-cdc-pq/internal/sqlutil"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	slogctx "github.com/veqryn/slog-context"
)

type Connector interface {
	Start(ctx context.Context)
	WaitForShutdown(ctx context.Context) error
	Close()
}

type connector struct {
	cdc           cdc.Connector
	cfg           *config.Connector
	sink          *Sink
	messages      chan Message
	pool          database.Pool
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
		primaryKey:    opts.PrimaryKey,
		defaultSchema: opts.DefaultSchema,
	}

	// Initialize database pool for target database
	pool, err := database.NewTargetPool(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create database pool: %w", err)
	}
	pqConnector.pool = pool

	// Create message channel
	pqConnector.messages = make(chan Message, cfg.ConnectorConfig.BulkSize)

	// Create sink
	batchConfig := config.BatchConfig{
		BulkSize:   cfg.BatchConfig.BulkSize,
		Timeout:    cfg.BatchConfig.Timeout,
		MaxRetries: cfg.BatchConfig.MaxRetries,
		RetryDelay: cfg.BatchConfig.RetryDelay,
	}
	pqConnector.sink = NewSink(
		pool,
		batchConfig,
		slog.Default().With("component", "Sink"),
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

		// Start sink
		sinkCtx := slogctx.Append(ctx, "component", "Sink")
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
		go c.sink.Start(sinkCtx, pqMessages)
	}()

	c.cdc.Start(ctx)
}

func (c *connector) WaitForShutdown(ctx context.Context) error {
	log := slogctx.FromCtx(ctx)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Info("shutdown signal received", "signal", sig.String())
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) Close() {
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

	// Process the message
	c.processMessage(context.Background(), ctx)
}

func (c *connector) processMessage(ctx context.Context, replCtx *replication.ListenerContext) {
	log := slogctx.FromCtx(ctx)

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

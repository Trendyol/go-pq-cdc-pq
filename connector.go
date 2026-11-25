package gopqcdcpq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

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
	mapper        Mapper

	lifecycleMu   sync.RWMutex
	lifecycleOnce sync.Once
	closeOnce     sync.Once
	runCtx        context.Context
	cancel        context.CancelFunc
	fatalErr      error
	stopped       chan struct{}
	sinkStopped   chan struct{}
	cdcStopped    chan struct{}
	cdcReady      chan struct{}
	cdcReadyOnce  sync.Once
	cdcReadyErr   error
	cdcStartDone  chan struct{}
	started       bool
	closing       bool

	// sendMu preserves event order across concurrent source/snapshot callbacks.
	sendMu sync.Mutex
}

func NewConnector(ctx context.Context, cfg *config.Connector, options ...Option) (Connector, error) {
	if cfg == nil {
		return nil, fmt.Errorf("connector configuration is required")
	}
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid connector configuration: %w", err)
	}

	opts := defaultConfig()
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}
	if opts.Mapper == nil {
		return nil, fmt.Errorf("mapper must not be nil")
	}

	pqConnector := &connector{
		cfg:           cfg,
		primaryKey:    opts.PrimaryKey,
		defaultSchema: opts.DefaultSchema,
		mapper:        opts.Mapper,
		messages:      make(chan Message, cfg.ConnectorConfig.BulkSize),
	}
	pqConnector.initLifecycle()

	pool, err := database.NewTargetPool(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create target database pool: %w", err)
	}
	pqConnector.pool = pool
	pqConnector.sink = NewSink(pool, cfg.BatchConfig, slog.Default())

	pqCDC, err := cdc.NewConnector(ctx, cfg.CDC, pqConnector.listener)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create CDC connector: %w", err)
	}
	pqConnector.cdc = pqCDC

	return pqConnector, nil
}

func (c *connector) listener(replCtx *replication.ListenerContext) {
	if replCtx == nil || replCtx.Message == nil {
		return
	}

	ctx := c.processingContext()
	if err := ctx.Err(); err != nil {
		return
	}
	// Wait for the CDC connector to be ready before relaying data events. This
	// prevents acknowledging snapshot/relation metadata ahead of target writes.
	if c.cfg == nil || !c.cfg.CDC.IsSnapshotOnlyMode() {
		if err := c.waitUntilReady(ctx); err != nil {
			return
		}
	}
	c.processMessage(ctx, replCtx)

	// Snapshot data callbacks do not wait on Ack. Insert a sink barrier so
	// snapshot completion metadata cannot mark unapplied rows complete.
	if snapshot, ok := replCtx.Message.(*format.Snapshot); ok && snapshot.EventType == format.SnapshotEventTypeData {
		done := make(chan struct{})
		c.sendAcknowledgement(func() error {
			close(done)
			return nil
		})
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
}

func (c *connector) processMessage(ctx context.Context, replCtx *replication.ListenerContext) {
	log := slogctx.FromCtx(ctx)
	log.Debug("processing replication message", "messageType", fmt.Sprintf("%T", replCtx.Message))

	switch msg := replCtx.Message.(type) {
	case *format.Insert:
		c.processInsertMessage(ctx, msg, replCtx.Ack)
	case *format.Delete:
		c.processDeleteMessage(ctx, msg, replCtx.Ack)
	case *format.Update:
		c.processUpdateMessage(ctx, msg, replCtx.Ack)
	case *format.Snapshot:
		c.processSnapshotMessage(ctx, msg, replCtx.Ack)
	case *format.Relation:
		// Control metadata is acknowledged only behind earlier target writes.
		log.Debug("relation message received", "namespace", msg.Namespace, "table", msg.Name, "columns", len(msg.Columns))
		c.sendAcknowledgement(replCtx.Ack)
	default:
		c.fail(fmt.Errorf("unsupported replication message %T; refusing to acknowledge past it", msg))
	}
}

func (c *connector) processInsertMessage(_ context.Context, msg *format.Insert, ack func() error) {
	msgObj := NewInsertMessage(msg)
	primaryKey := c.resolvePrimaryKey(msg.TableName)
	msgObj.Query, msgObj.Args = sqlutil.BuildUpsertQuery(msg.TableName, msg.Decoded, primaryKey)
	msgObj.Ack = ack
	msgObj.Schema = c.defaultSchema
	msgObj.Table = msg.TableName
	msgObj.Action = "INSERT"
	msgObj.NewValues = msg.Decoded
	c.sendMessage(*msgObj)
}

func (c *connector) processDeleteMessage(_ context.Context, msg *format.Delete, ack func() error) {
	msgObj := NewDeleteMessage(msg)
	primaryKey := c.resolvePrimaryKey(msg.TableName)
	msgObj.Query, msgObj.Args = sqlutil.BuildDeleteQuery(msg.TableName, msg.OldDecoded, primaryKey)
	msgObj.Ack = ack
	msgObj.Schema = c.defaultSchema
	msgObj.Table = msg.TableName
	msgObj.Action = "DELETE"
	msgObj.OldKeys = msg.OldDecoded
	c.sendMessage(*msgObj)
}

func (c *connector) processSnapshotMessage(ctx context.Context, msg *format.Snapshot, ack func() error) {
	log := slogctx.FromCtx(ctx)

	switch msg.EventType {
	case format.SnapshotEventTypeBegin:
		log.Info("snapshot started", "lsn", msg.LSN.String(), "totalRows", msg.TotalRows)
		c.sendAcknowledgement(ack)

	case format.SnapshotEventTypeEnd:
		log.Info("snapshot completed", "lsn", msg.LSN.String(), "isLast", msg.IsLast)
		c.sendAcknowledgement(ack)

	case format.SnapshotEventTypeData:
		msgObj := NewSnapshotMessage(msg)
		primaryKey := c.resolvePrimaryKey(msg.Table)
		msgObj.Query, msgObj.Args = sqlutil.BuildUpsertQuery(msg.Table, msg.Data, primaryKey)
		msgObj.Ack = ack
		msgObj.Schema = c.defaultSchema
		msgObj.Table = msg.Table
		msgObj.Action = "SNAPSHOT"
		msgObj.NewValues = msg.Data
		c.sendMessage(*msgObj)

	default:
		c.fail(fmt.Errorf("unsupported snapshot event type %v; refusing to acknowledge it", msg.EventType))
	}
}

func (c *connector) processUpdateMessage(_ context.Context, msg *format.Update, ack func() error) {
	msgObj := NewUpdateMessage(msg)
	primaryKey := c.resolvePrimaryKey(msg.TableName)
	msgObj.Query, msgObj.Args = sqlutil.BuildUpsertQuery(msg.TableName, msg.NewDecoded, primaryKey)
	msgObj.Ack = ack
	msgObj.Schema = c.defaultSchema
	msgObj.Table = msg.TableName
	msgObj.Action = "UPDATE"
	msgObj.OldKeys = msg.OldDecoded
	msgObj.NewValues = msg.NewDecoded
	c.sendMessage(*msgObj)
}

// sendMessage applies the mapper and queues the resulting actions in order,
// attaching the acknowledgement to the final action. An empty action set
// becomes an ordered barrier so the event cannot acknowledge past pending writes.
func (c *connector) sendMessage(message Message) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	ctx := c.processingContext()
	if ctx.Err() != nil {
		return
	}

	actions := c.mapper(&message)
	if len(actions) == 0 {
		c.sendLocked(ctx, Message{Ack: message.Ack})
		return
	}

	for i, action := range actions {
		msg := Message{
			Query:          action.Query,
			Args:           action.Args,
			EventTime:      message.EventTime,
			TableName:      message.TableName,
			TableNamespace: message.TableNamespace,
			Type:           message.Type,
			Schema:         message.Schema,
			Table:          message.Table,
			Action:         message.Action,
			OldKeys:        message.OldKeys,
			NewValues:      message.NewValues,
			OldData:        message.OldData,
			NewData:        message.NewData,
		}
		// Only set Ack on the last action so all queries commit before ack.
		if i == len(actions)-1 {
			msg.Ack = message.Ack
		}
		if !c.sendLocked(ctx, msg) {
			return
		}
	}
}

// sendAcknowledgement queues a sink barrier so control and filtered events
// cannot advance the source position past earlier unapplied writes.
func (c *connector) sendAcknowledgement(ack func() error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	ctx := c.processingContext()
	if ctx.Err() != nil {
		return
	}
	c.sendLocked(ctx, Message{Ack: ack})
}

// sendLocked holds sendMu. It blocks until the message is accepted or the
// processing context is cancelled (during shutdown), avoiding send panics.
func (c *connector) sendLocked(ctx context.Context, message Message) bool {
	if c.messages == nil {
		return true
	}
	select {
	case c.messages <- message:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *connector) resolvePrimaryKey(tableName string) string {
	if c.cfg.TablePrimaryKeys == nil {
		return c.primaryKey
	}
	if pk, ok := c.cfg.TablePrimaryKeys[tableName]; ok {
		if pk != "" {
			return pk
		}
	}
	return c.primaryKey
}

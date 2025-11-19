package connector

import (
	"context"
	"fmt"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	slogctx "github.com/veqryn/slog-context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Trendyol/go-pq-cdc-pq/internal/sqlutil"
	"github.com/Trendyol/go-pq-cdc-pq/internal/tracer"
)

// ReplicationListener handles CDC replication messages
type ReplicationListener struct {
	messages   chan Message
	primaryKey string
}

// NewReplicationListener creates a new replication listener
func NewReplicationListener(messages chan Message, primaryKey string) *ReplicationListener {
	return &ReplicationListener{
		messages:   messages,
		primaryKey: primaryKey,
	}
}

// ListenerFunc returns the listener function for CDC
func (r *ReplicationListener) ListenerFunc(ctx context.Context) replication.ListenerFunc {
	return func(replCtx *replication.ListenerContext) {
		// Handle keepalive messages
		if replCtx.Message == nil {
			return
		}

		// Create a new root span for each CDC message processing
		// This is necessary because go-pq-cdc doesn't propagate our context
		msgCtx, span := tracer.StartSpan(ctx, "cdc.message_received",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("message.type", fmt.Sprintf("%T", replCtx.Message)),
			))
		defer span.End()

		// Process the message with the new context
		r.processMessage(msgCtx, replCtx)

		span.SetStatus(codes.Ok, "CDC message processing completed")
	}
}

// processMessage processes different types of replication messages
func (r *ReplicationListener) processMessage(ctx context.Context, replCtx *replication.ListenerContext) {
	// Get the logger from context
	log := slogctx.FromCtx(ctx)

	// Start span for message processing
	ctx, span := tracer.StartSpanWithAttrs(ctx, "replication.process_message")
	defer span.End()

	log.Debug("Processing replication message", "messageType", fmt.Sprintf("%T", replCtx.Message))

	switch msg := replCtx.Message.(type) {
	case *format.Insert:
		r.processInsertMessage(ctx, msg, replCtx.Ack)

	case *format.Delete:
		r.processDeleteMessage(ctx, msg, replCtx.Ack)

	case *format.Update:
		r.processUpdateMessage(ctx, msg, replCtx.Ack)

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

// processInsertMessage processes INSERT messages with logging and tracing
func (r *ReplicationListener) processInsertMessage(ctx context.Context, msg *format.Insert, ack func() error) {
	log := slogctx.FromCtx(ctx)

	// Add CDC-specific context
	ctx = slogctx.Append(ctx,
		"action", ActionInsert,
		"table", msg.TableName,
		"recordCount", len(msg.Decoded))

	ctx, span := tracer.StartSpanWithAttrs(ctx, "cdc.process_insert",
		attribute.String("cdc.action", ActionInsert),
		attribute.String("cdc.table", msg.TableName),
		attribute.Int("cdc.record_count", len(msg.Decoded)))
	defer span.End()

	log.Info("Processing CDC INSERT message")

	message := r.processInsert(msg)
	message.Ack = ack
	r.sendMessage(message)

	log.Debug("CDC INSERT message processed successfully")
	span.SetStatus(codes.Ok, "INSERT processed successfully")
}

// processDeleteMessage processes DELETE messages with logging and tracing
func (r *ReplicationListener) processDeleteMessage(ctx context.Context, msg *format.Delete, ack func() error) {
	log := slogctx.FromCtx(ctx)

	// Add CDC-specific context
	ctx = slogctx.Append(ctx,
		"action", ActionDelete,
		"table", msg.TableName,
		"recordCount", len(msg.OldDecoded))

	ctx, span := tracer.StartSpanWithAttrs(ctx, "cdc.process_delete",
		attribute.String("cdc.action", ActionDelete),
		attribute.String("cdc.table", msg.TableName),
		attribute.Int("cdc.record_count", len(msg.OldDecoded)))
	defer span.End()

	log.Info("Processing CDC DELETE message")

	message := r.processDelete(msg)
	message.Ack = ack
	r.sendMessage(message)

	log.Debug("CDC DELETE message processed successfully")
	span.SetStatus(codes.Ok, "DELETE processed successfully")
}

// processUpdateMessage processes UPDATE messages with logging and tracing
func (r *ReplicationListener) processUpdateMessage(ctx context.Context, msg *format.Update, ack func() error) {
	log := slogctx.FromCtx(ctx)

	// Add CDC-specific context
	ctx = slogctx.Append(ctx,
		"action", ActionUpdate,
		"table", msg.TableName,
		"recordCount", len(msg.NewDecoded))

	ctx, span := tracer.StartSpanWithAttrs(ctx, "cdc.process_update",
		attribute.String("cdc.action", ActionUpdate),
		attribute.String("cdc.table", msg.TableName),
		attribute.Int("cdc.record_count", len(msg.NewDecoded)))
	defer span.End()

	log.Info("Processing CDC UPDATE message")

	message := r.processUpdate(msg)
	message.Ack = ack
	r.sendMessage(message)

	log.Debug("CDC UPDATE message processed successfully")
	span.SetStatus(codes.Ok, "UPDATE processed successfully")
}

// processInsert processes INSERT messages
func (r *ReplicationListener) processInsert(msg *format.Insert) Message {
	querySQL, args := sqlutil.BuildUpsertQuery(msg.TableName, msg.Decoded, r.primaryKey)
	return Message{
		Query:     querySQL,
		Args:      args,
		Schema:    DefaultSchema,
		Table:     msg.TableName,
		Action:    ActionInsert,
		OldKeys:   nil,
		NewValues: msg.Decoded,
	}
}

// processDelete processes DELETE messages
func (r *ReplicationListener) processDelete(msg *format.Delete) Message {
	querySQL, args := sqlutil.BuildDeleteQuery(msg.TableName, msg.OldDecoded, r.primaryKey)
	return Message{
		Query:     querySQL,
		Args:      args,
		Schema:    DefaultSchema,
		Table:     msg.TableName,
		Action:    ActionDelete,
		OldKeys:   msg.OldDecoded,
		NewValues: nil,
	}
}

// processUpdate processes UPDATE messages
func (r *ReplicationListener) processUpdate(msg *format.Update) Message {
	querySQL, args := sqlutil.BuildUpsertQuery(msg.TableName, msg.NewDecoded, r.primaryKey)
	return Message{
		Query:     querySQL,
		Args:      args,
		Schema:    DefaultSchema,
		Table:     msg.TableName,
		Action:    ActionUpdate,
		OldKeys:   msg.OldDecoded,
		NewValues: msg.NewDecoded,
	}
}

// sendMessage sends the processed message to the channel
func (r *ReplicationListener) sendMessage(message Message) {
	r.messages <- message
}

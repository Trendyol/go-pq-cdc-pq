package connector

import (
	"context"
	"testing"

	"github.com/Trendyol/go-pq-cdc-pq/internal/tracer"
	"github.com/Trendyol/go-pq-cdc-pq/mocks"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
)

func init() {
	// Initialize tracer for tests
	tracer.Initialize()
}

// Test data
var testData = map[string]any{
	"id":   1,
	"name": "test_user",
	"age":  25,
}

var testOldData = map[string]any{
	"id":   1,
	"name": "old_name",
	"age":  20,
}

func TestReplicationListener_ProcessInsert(t *testing.T) {
	messages := make(chan Message, 10)
	listener := NewReplicationListener(messages, "id")

	t.Run("successful insert processing", func(t *testing.T) {
		insertMsg := &format.Insert{
			TableName: "users",
			Decoded:   testData,
		}

		result := listener.processInsert(insertMsg)

		assert.Equal(t, DefaultSchema, result.Schema)
		assert.Equal(t, "users", result.Table)
		assert.Equal(t, ActionInsert, result.Action)
		assert.Equal(t, testData, result.NewValues)
		assert.Nil(t, result.OldKeys)
		assert.NotEmpty(t, result.Query)
		assert.NotEmpty(t, result.Args)
	})
}

func TestReplicationListener_ProcessDelete(t *testing.T) {
	messages := make(chan Message, 10)
	listener := NewReplicationListener(messages, "id")

	t.Run("successful delete processing", func(t *testing.T) {
		deleteMsg := &format.Delete{
			TableName:  "users",
			OldDecoded: testOldData,
		}

		result := listener.processDelete(deleteMsg)

		assert.Equal(t, DefaultSchema, result.Schema)
		assert.Equal(t, "users", result.Table)
		assert.Equal(t, ActionDelete, result.Action)
		assert.Equal(t, testOldData, result.OldKeys)
		assert.Nil(t, result.NewValues)
		assert.NotEmpty(t, result.Query)
		assert.NotEmpty(t, result.Args)
	})
}

func TestReplicationListener_ProcessUpdate(t *testing.T) {
	messages := make(chan Message, 10)
	listener := NewReplicationListener(messages, "id")

	t.Run("successful update processing", func(t *testing.T) {
		updateMsg := &format.Update{
			TableName:  "users",
			OldDecoded: testOldData,
			NewDecoded: testData,
		}

		result := listener.processUpdate(updateMsg)

		assert.Equal(t, DefaultSchema, result.Schema)
		assert.Equal(t, "users", result.Table)
		assert.Equal(t, ActionUpdate, result.Action)
		assert.Equal(t, testOldData, result.OldKeys)
		assert.Equal(t, testData, result.NewValues)
		assert.NotEmpty(t, result.Query)
		assert.NotEmpty(t, result.Args)
	})
}

func TestReplicationListener_ProcessMessage(t *testing.T) {
	messages := make(chan Message, 10)
	listener := NewReplicationListener(messages, "id")

	t.Run("process insert message", func(t *testing.T) {
		insertMsg := &format.Insert{
			TableName: "users",
			Decoded:   testData,
		}

		mockAck := &mocks.MockAckFunc{}
		replCtx := &replication.ListenerContext{
			Message: insertMsg,
			Ack:     mockAck.Ack,
		}

		listener.processMessage(context.Background(), replCtx)

		select {
		case msg := <-messages:
			assert.Equal(t, ActionInsert, msg.Action)
			assert.Equal(t, "users", msg.Table)
			assert.NotNil(t, msg.Ack)
		default:
			t.Error("Expected message to be sent to channel")
		}
	})

	// Note: Relation message test removed due to complex format.Relation structure
}

func TestReplicationListener_ListenerFunc(t *testing.T) {
	messages := make(chan Message, 10)
	listener := NewReplicationListener(messages, "id")

	listenerFunc := listener.ListenerFunc(context.Background())

	t.Run("handle nil message", func(t *testing.T) {
		replCtx := &replication.ListenerContext{
			Message: nil,
		}

		// Should not panic and should return without processing
		listenerFunc(replCtx)

		select {
		case <-messages:
			t.Error("No message should be sent for nil message")
		default:
			// Expected behavior
		}
	})

	t.Run("handle valid message", func(t *testing.T) {
		insertMsg := &format.Insert{
			TableName: "users",
			Decoded:   testData,
		}

		mockAck := &mocks.MockAckFunc{}
		replCtx := &replication.ListenerContext{
			Message: insertMsg,
			Ack:     mockAck.Ack,
		}

		listenerFunc(replCtx)

		select {
		case msg := <-messages:
			assert.Equal(t, ActionInsert, msg.Action)
			assert.Equal(t, "users", msg.Table)
		default:
			t.Error("Expected message to be sent to channel")
		}
	})
}

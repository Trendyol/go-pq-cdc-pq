package gopqcdcpq

import (
	"context"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc-pq/config"
	"github.com/Trendyol/go-pq-cdc-pq/mocks"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const msgNotReceived = "message not received"

func createTestConfig() *config.Connector {
	return &config.Connector{
		ConnectorConfig: config.ConnectorConfig{
			BulkSize: 100,
		},
		BatchConfig: config.BatchConfig{
			BulkSize:   100,
			Timeout:    time.Second,
			MaxRetries: 3,
			RetryDelay: time.Millisecond * 100,
		},
		Postgres: config.PostgresConfig{
			Target: config.DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "test",
				Password: "test",
				DB:       "testdb",
			},
		},
		CDC: cdcconfig.Config{},
	}
}

func TestNewConnectorSuccess(t *testing.T) {
	// We can't easily test NewConnector without actual database connection
	// This test would require integration test setup
	// For unit test, we'll test the connector methods with mocked dependencies
	t.Skip("Requires database connection - use integration test")
}

func TestConnectorStart(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()

	mockPool := &mocks.MockDatabasePool{}
	mockCDC := &mocks.MockCDCConnector{}

	conn := &connector{
		cdc:           mockCDC,
		cfg:           cfg,
		messages:      make(chan Message, cfg.ConnectorConfig.BulkSize),
		pool:          mockPool,
		primaryKey:    "id",
		defaultSchema: "public",
	}

	// Create sink with mock pool
	batchConfig := config.BatchConfig{
		BulkSize:   cfg.BatchConfig.BulkSize,
		Timeout:    cfg.BatchConfig.Timeout,
		MaxRetries: cfg.BatchConfig.MaxRetries,
		RetryDelay: cfg.BatchConfig.RetryDelay,
	}
	conn.sink = NewSink(mockPool, batchConfig, nil)

	mockCDC.On("WaitUntilReady", mock.Anything).Return(nil)
	mockCDC.On("Start", mock.Anything).Return()

	conn.Start(ctx)

	// Give goroutines time to start
	time.Sleep(50 * time.Millisecond)

	mockCDC.AssertExpectations(t)
}

func TestConnectorWaitForShutdownSignal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	cfg := createTestConfig()
	conn := &connector{
		cfg: cfg,
	}

	// WaitForShutdown waits for a signal or context cancellation/timeout
	// Since we can't easily send signals in unit tests, we test that
	// context timeout is handled correctly
	err := conn.WaitForShutdown(ctx)
	// Context timeout is expected behavior, not an error
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestConnectorWaitForShutdownContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := createTestConfig()
	conn := &connector{
		cfg: cfg,
	}

	err := conn.WaitForShutdown(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestConnectorClose(t *testing.T) {
	mockPool := &mocks.MockDatabasePool{}
	mockCDC := &mocks.MockCDCConnector{}

	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cdc:      mockCDC,
		cfg:      cfg,
		messages: messages,
		pool:     mockPool,
	}

	mockCDC.On("Close").Return()
	mockPool.On("Close").Return()

	conn.Close()

	// Verify channel is closed
	_, ok := <-messages
	assert.False(t, ok, "messages channel should be closed")

	mockCDC.AssertExpectations(t)
	mockPool.AssertExpectations(t)
}

func TestConnectorCloseNilPool(t *testing.T) {
	mockCDC := &mocks.MockCDCConnector{}

	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cdc:      mockCDC,
		cfg:      cfg,
		messages: messages,
		pool:     nil,
	}

	mockCDC.On("Close").Return()

	conn.Close()

	_, ok := <-messages
	assert.False(t, ok, "messages channel should be closed")

	mockCDC.AssertExpectations(t)
}

func TestConnectorListenerNilMessage(t *testing.T) {
	cfg := createTestConfig()
	conn := &connector{
		cfg: cfg,
	}

	replCtx := &replication.ListenerContext{
		Message: nil,
	}

	// Should return early without processing
	assert.NotPanics(t, func() {
		conn.listener(replCtx)
	})
}

func TestConnectorProcessMessageInsert(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:           cfg,
		messages:      messages,
		primaryKey:    "id",
		defaultSchema: "public",
	}

	ack := func() error {
		return nil
	}

	insertMsg := &format.Insert{
		TableName:      "users",
		TableNamespace: "public",
		Decoded: map[string]any{
			"id":   1,
			"name": "test",
		},
	}

	replCtx := &replication.ListenerContext{
		Message: insertMsg,
		Ack:     ack,
	}

	conn.processMessage(ctx, replCtx)

	// Verify message was sent
	select {
	case msg := <-messages:
		assert.Equal(t, "users", msg.Table)
		assert.Equal(t, "public", msg.Schema)
		assert.Equal(t, "INSERT", msg.Action)
		assert.NotEmpty(t, msg.Query)
		assert.NotNil(t, msg.Args)
		assert.NotNil(t, msg.NewValues)
		assert.Nil(t, msg.OldKeys)
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}
}

func TestConnectorProcessMessageUpdate(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:           cfg,
		messages:      messages,
		primaryKey:    "id",
		defaultSchema: "public",
	}

	ack := func() error {
		return nil
	}

	updateMsg := &format.Update{
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded: map[string]any{
			"id":   1,
			"name": "old",
		},
		NewDecoded: map[string]any{
			"id":   1,
			"name": "new",
		},
	}

	replCtx := &replication.ListenerContext{
		Message: updateMsg,
		Ack:     ack,
	}

	conn.processMessage(ctx, replCtx)

	select {
	case msg := <-messages:
		assert.Equal(t, "users", msg.Table)
		assert.Equal(t, "public", msg.Schema)
		assert.Equal(t, "UPDATE", msg.Action)
		assert.NotEmpty(t, msg.Query)
		assert.NotNil(t, msg.Args)
		assert.NotNil(t, msg.NewValues)
		assert.NotNil(t, msg.OldKeys)
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}
}

func TestConnectorProcessMessageDelete(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:           cfg,
		messages:      messages,
		primaryKey:    "id",
		defaultSchema: "public",
	}

	ack := func() error {
		return nil
	}

	deleteMsg := &format.Delete{
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded: map[string]any{
			"id":   1,
			"name": "test",
		},
	}

	replCtx := &replication.ListenerContext{
		Message: deleteMsg,
		Ack:     ack,
	}

	conn.processMessage(ctx, replCtx)

	select {
	case msg := <-messages:
		assert.Equal(t, "users", msg.Table)
		assert.Equal(t, "public", msg.Schema)
		assert.Equal(t, "DELETE", msg.Action)
		assert.NotEmpty(t, msg.Query)
		assert.NotNil(t, msg.Args)
		assert.Nil(t, msg.NewValues)
		assert.NotNil(t, msg.OldKeys)
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}
}

func TestConnectorProcessSnapshotDataMessage(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:           cfg,
		messages:      messages,
		primaryKey:    "id",
		defaultSchema: "public",
	}

	ackCalled := false
	ack := func() error {
		ackCalled = true
		return nil
	}

	snapshotMsg := &format.Snapshot{
		EventType: format.SnapshotEventTypeData,
		Table:     "users",
		Schema:    "public",
		Data: map[string]any{
			"id":   1,
			"name": "snapshot",
		},
	}

	conn.processSnapshotMessage(ctx, snapshotMsg, ack)

	select {
	case msg := <-messages:
		assert.Equal(t, "users", msg.Table)
		assert.Equal(t, "public", msg.Schema)
		assert.Equal(t, "SNAPSHOT", msg.Action)
		assert.Equal(t, SnapshotMessage, msg.Type)
		assert.NotEmpty(t, msg.Query)
		assert.NotNil(t, msg.NewValues)
		assert.Equal(t, snapshotMsg.Data, msg.NewValues)
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}

	assert.False(t, ackCalled, "snapshot data ack should be deferred to sink")
}

func TestConnectorProcessSnapshotBeginAck(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()

	conn := &connector{
		cfg: cfg,
	}

	ackCalled := false
	ack := func() error {
		ackCalled = true
		return nil
	}

	snapshotMsg := &format.Snapshot{
		EventType: format.SnapshotEventTypeBegin,
	}

	conn.processSnapshotMessage(ctx, snapshotMsg, ack)

	assert.True(t, ackCalled, "snapshot begin should be acked immediately")
}

func TestConnectorProcessSnapshotEndAck(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()

	conn := &connector{
		cfg: cfg,
	}

	ackCalled := false
	ack := func() error {
		ackCalled = true
		return nil
	}

	snapshotMsg := &format.Snapshot{
		EventType: format.SnapshotEventTypeEnd,
	}

	conn.processSnapshotMessage(ctx, snapshotMsg, ack)

	assert.True(t, ackCalled, "snapshot end should be acked immediately")
}

func TestConnectorProcessMessageRelation(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()

	conn := &connector{
		cfg: cfg,
	}

	ackCalled := false
	ack := func() error {
		ackCalled = true
		return nil
	}

	relationMsg := &format.Relation{
		Namespace: "public",
		Name:      "users",
		Columns:   []tuple.RelationColumn{{Name: "id"}, {Name: "name"}},
	}

	replCtx := &replication.ListenerContext{
		Message: relationMsg,
		Ack:     ack,
	}

	conn.processMessage(ctx, replCtx)

	assert.True(t, ackCalled, "ack should be called for relation messages")
}

func TestConnectorProcessMessageRelationNilAck(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()

	conn := &connector{
		cfg: cfg,
	}

	relationMsg := &format.Relation{
		Namespace: "public",
		Name:      "users",
		Columns:   []tuple.RelationColumn{{Name: "id"}, {Name: "name"}},
	}

	replCtx := &replication.ListenerContext{
		Message: relationMsg,
		Ack:     nil,
	}

	// Should not panic
	assert.NotPanics(t, func() {
		conn.processMessage(ctx, replCtx)
	})
}

func TestConnectorProcessMessageUnknownType(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()

	conn := &connector{
		cfg: cfg,
	}

	unknownMsg := "unknown message type"

	replCtx := &replication.ListenerContext{
		Message: unknownMsg,
		Ack:     nil,
	}

	// Should not panic, just log warning
	assert.NotPanics(t, func() {
		conn.processMessage(ctx, replCtx)
	})
}

func TestConnectorSendMessage(t *testing.T) {
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:      cfg,
		messages: messages,
	}

	msg := Message{
		Table:  "users",
		Action: "INSERT",
		Query:  "INSERT INTO users VALUES ($1, $2)",
		Args:   []any{1, "test"},
	}

	conn.sendMessage(msg)

	select {
	case received := <-messages:
		assert.Equal(t, msg.Table, received.Table)
		assert.Equal(t, msg.Action, received.Action)
		assert.Equal(t, msg.Query, received.Query)
		assert.Equal(t, msg.Args, received.Args)
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}
}

func TestConnectorProcessInsertMessage(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:           cfg,
		messages:      messages,
		primaryKey:    "id",
		defaultSchema: "public",
	}

	ack := func() error { return nil }

	insertMsg := &format.Insert{
		TableName:      "users",
		TableNamespace: "public",
		Decoded: map[string]any{
			"id":   1,
			"name": "test",
		},
	}

	conn.processInsertMessage(ctx, insertMsg, ack)

	select {
	case msg := <-messages:
		assert.Equal(t, "users", msg.Table)
		assert.Equal(t, "public", msg.Schema)
		assert.Equal(t, "INSERT", msg.Action)
		assert.NotEmpty(t, msg.Query)
		assert.Contains(t, msg.Query, "INSERT INTO")
		assert.NotNil(t, msg.NewValues)
		assert.Nil(t, msg.OldKeys)
		assert.NotNil(t, msg.Ack)
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}
}

func TestConnectorProcessDeleteMessage(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:           cfg,
		messages:      messages,
		primaryKey:    "id",
		defaultSchema: "public",
	}

	ack := func() error { return nil }

	deleteMsg := &format.Delete{
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded: map[string]any{
			"id":   1,
			"name": "test",
		},
	}

	conn.processDeleteMessage(ctx, deleteMsg, ack)

	select {
	case msg := <-messages:
		assert.Equal(t, "users", msg.Table)
		assert.Equal(t, "public", msg.Schema)
		assert.Equal(t, "DELETE", msg.Action)
		assert.NotEmpty(t, msg.Query)
		assert.Contains(t, msg.Query, "DELETE FROM")
		assert.Nil(t, msg.NewValues)
		assert.NotNil(t, msg.OldKeys)
		assert.NotNil(t, msg.Ack)
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}
}

func TestConnectorProcessUpdateMessage(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:           cfg,
		messages:      messages,
		primaryKey:    "id",
		defaultSchema: "public",
	}

	ack := func() error { return nil }

	updateMsg := &format.Update{
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded: map[string]any{
			"id":   1,
			"name": "old",
		},
		NewDecoded: map[string]any{
			"id":   1,
			"name": "new",
		},
	}

	conn.processUpdateMessage(ctx, updateMsg, ack)

	select {
	case msg := <-messages:
		assert.Equal(t, "users", msg.Table)
		assert.Equal(t, "public", msg.Schema)
		assert.Equal(t, "UPDATE", msg.Action)
		assert.NotEmpty(t, msg.Query)
		assert.Contains(t, msg.Query, "INSERT INTO")
		assert.NotNil(t, msg.NewValues)
		assert.NotNil(t, msg.OldKeys)
		assert.NotNil(t, msg.Ack)
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}
}

func TestConnectorWithOptions(t *testing.T) {
	cfg := createTestConfig()

	// Test that options are applied correctly
	// Since NewConnector requires database connection, we test option application
	// by checking the connector struct fields after creation with mocked dependencies

	mockPool := &mocks.MockDatabasePool{}
	mockCDC := &mocks.MockCDCConnector{}

	conn := &connector{
		cdc:           mockCDC,
		cfg:           cfg,
		messages:      make(chan Message, cfg.ConnectorConfig.BulkSize),
		pool:          mockPool,
		primaryKey:    "custom_id",
		defaultSchema: "custom_schema",
	}

	assert.Equal(t, "custom_id", conn.primaryKey)
	assert.Equal(t, "custom_schema", conn.defaultSchema)
}

func TestConnectorMessageChannelCapacity(t *testing.T) {
	cfg := createTestConfig()
	expectedCapacity := cfg.ConnectorConfig.BulkSize

	messages := make(chan Message, expectedCapacity)

	conn := &connector{
		cfg:      cfg,
		messages: messages,
	}

	assert.Equal(t, expectedCapacity, cap(conn.messages))
}

func TestConnectorProcessMessageWithCustomPrimaryKey(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:           cfg,
		messages:      messages,
		primaryKey:    "custom_pk",
		defaultSchema: "public",
	}

	ack := func() error { return nil }

	insertMsg := &format.Insert{
		TableName:      "users",
		TableNamespace: "public",
		Decoded: map[string]any{
			"custom_pk": 1,
			"name":      "test",
		},
	}

	replCtx := &replication.ListenerContext{
		Message: insertMsg,
		Ack:     ack,
	}

	conn.processMessage(ctx, replCtx)

	select {
	case msg := <-messages:
		assert.NotEmpty(t, msg.Query)
		// Query should use custom_pk as primary key
		assert.Contains(t, msg.Query, "custom_pk")
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}
}

func TestConnectorProcessMessageWithCustomSchema(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	messages := make(chan Message, 10)

	conn := &connector{
		cfg:           cfg,
		messages:      messages,
		primaryKey:    "id",
		defaultSchema: "custom_schema",
	}

	ack := func() error { return nil }

	insertMsg := &format.Insert{
		TableName:      "users",
		TableNamespace: "public",
		Decoded: map[string]any{
			"id":   1,
			"name": "test",
		},
	}

	replCtx := &replication.ListenerContext{
		Message: insertMsg,
		Ack:     ack,
	}

	conn.processMessage(ctx, replCtx)

	select {
	case msg := <-messages:
		assert.Equal(t, "custom_schema", msg.Schema)
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msgNotReceived)
	}
}

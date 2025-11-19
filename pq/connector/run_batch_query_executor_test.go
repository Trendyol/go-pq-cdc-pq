package connector

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc-pq/mocks"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test constants
const (
	testBulkSize   = 3
	testTimeout    = 100 * time.Millisecond
	testMaxRetries = 3
	testRetryDelay = 10 * time.Millisecond
	testTableName  = "test_table"
	testQuerySQL   = "INSERT INTO test_table (id, name) VALUES ($1, $2)"
	mockErrorMsg   = "mock database error"
)

// Test helper functions
func createTestBatchConfig() BatchConfig {
	return BatchConfig{
		BulkSize:   testBulkSize,
		Timeout:    testTimeout,
		MaxRetries: testMaxRetries,
		RetryDelay: testRetryDelay,
	}
}

func createTestMessage(id int, name string) Message {
	return Message{
		Query:     testQuerySQL,
		Args:      []any{id, name},
		Ack:       func() error { return nil },
		Schema:    DefaultSchema,
		Table:     testTableName,
		Action:    ActionInsert,
		NewValues: map[string]any{"id": id, "name": name},
	}
}

func TestBatchExecutor_AddMessage(t *testing.T) {
	config := createTestBatchConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	mockPool := &mocks.MockDatabasePool{}
	executor := NewBatchExecutor(mockPool, config, logger)

	t.Run("add single message", func(t *testing.T) {
		message := createTestMessage(1, "test1")
		executor.addMessage(message)

		assert.Equal(t, 1, len(executor.queue))
		assert.Equal(t, 1, len(executor.acks))
		assert.Equal(t, testQuerySQL, executor.queue[0].SQL)
		assert.Equal(t, []any{1, "test1"}, executor.queue[0].Arguments)
	})

	t.Run("add multiple messages", func(t *testing.T) {
		executor.resetBatch() // Reset for clean test

		message1 := createTestMessage(1, "test1")
		message2 := createTestMessage(2, "test2")

		executor.addMessage(message1)
		executor.addMessage(message2)

		assert.Equal(t, 2, len(executor.queue))
		assert.Equal(t, 2, len(executor.acks))
	})
}

func TestBatchExecutor_ResetBatch(t *testing.T) {
	config := createTestBatchConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	mockPool := &mocks.MockDatabasePool{}
	executor := NewBatchExecutor(mockPool, config, logger)

	// Add some messages first
	message1 := createTestMessage(1, "test1")
	message2 := createTestMessage(2, "test2")
	executor.addMessage(message1)
	executor.addMessage(message2)

	assert.Equal(t, 2, len(executor.queue))
	assert.Equal(t, 2, len(executor.acks))

	// Reset batch
	executor.resetBatch()

	assert.Equal(t, 0, len(executor.queue))
	assert.Equal(t, 0, len(executor.acks))
	assert.Equal(t, testBulkSize, cap(executor.queue))
	assert.Equal(t, testBulkSize, cap(executor.acks))
}

func TestBatchExecutor_ExecuteBatch_Success(t *testing.T) {
	config := createTestBatchConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	mockPool := &mocks.MockDatabasePool{}
	mockBatchResults := &mocks.MockBatchResults{}

	// Setup mock expectations
	mockPool.On("SendBatch", mock.Anything, mock.MatchedBy(func(batch *pgx.Batch) bool {
		return len(batch.QueuedQueries) == 2
	})).Return(mockBatchResults)

	mockBatchResults.On("Exec").Return(pgconn.CommandTag{}, nil).Times(2)
	mockBatchResults.On("Close").Return(nil)

	executor := NewBatchExecutor(mockPool, config, logger)

	// Create test queue
	queue := []*pgx.QueuedQuery{
		{SQL: testQuerySQL, Arguments: []any{1, "test1"}},
		{SQL: testQuerySQL, Arguments: []any{2, "test2"}},
	}

	ackCalled := false
	ackFunc := func() error {
		ackCalled = true
		return nil
	}

	// Execute batch
	err := executor.executeBatch(context.Background(), queue, ackFunc)

	assert.NoError(t, err)
	assert.True(t, ackCalled)
	mockPool.AssertExpectations(t)
	mockBatchResults.AssertExpectations(t)
}

func TestBatchExecutor_ExecuteBatch_DatabaseError(t *testing.T) {
	config := createTestBatchConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	mockPool := &mocks.MockDatabasePool{}
	mockBatchResults := &mocks.MockBatchResults{}

	// Setup mock expectations - database error
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(mockBatchResults)
	mockBatchResults.On("Exec").Return(pgconn.CommandTag{}, errors.New(mockErrorMsg))
	mockBatchResults.On("Close").Return(nil)

	executor := NewBatchExecutor(mockPool, config, logger)

	// Create test queue
	queue := []*pgx.QueuedQuery{
		{SQL: testQuerySQL, Arguments: []any{1, "test1"}},
	}

	ackCalled := false
	ackFunc := func() error {
		ackCalled = true
		return nil
	}

	// Execute batch
	err := executor.executeBatch(context.Background(), queue, ackFunc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), mockErrorMsg)
	assert.False(t, ackCalled)
	mockPool.AssertExpectations(t)
	mockBatchResults.AssertExpectations(t)
}

func TestBatchExecutor_ExecuteBatchWithRetry_Success(t *testing.T) {
	config := createTestBatchConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	mockPool := &mocks.MockDatabasePool{}
	mockBatchResults := &mocks.MockBatchResults{}

	// Setup mock expectations - fail first time, succeed second time
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(mockBatchResults).Times(2)
	mockBatchResults.On("Exec").Return(pgconn.CommandTag{}, errors.New(mockErrorMsg)).Once()
	mockBatchResults.On("Close").Return(nil).Once()
	mockBatchResults.On("Exec").Return(pgconn.CommandTag{}, nil).Once()
	mockBatchResults.On("Close").Return(nil).Once()

	executor := NewBatchExecutor(mockPool, config, logger)

	// Create test queue
	queue := []*pgx.QueuedQuery{
		{SQL: testQuerySQL, Arguments: []any{1, "test1"}},
	}

	ackCalled := false
	ackFunc := func() error {
		ackCalled = true
		return nil
	}

	// Execute batch with retry
	err := executor.executeBatchWithRetry(context.Background(), queue, ackFunc)

	assert.NoError(t, err)
	assert.True(t, ackCalled)
	mockPool.AssertExpectations(t)
	mockBatchResults.AssertExpectations(t)
}

func TestBatchExecutor_ExecuteBatchWithRetry_AllRetriesFail(t *testing.T) {
	config := createTestBatchConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	mockPool := &mocks.MockDatabasePool{}
	mockBatchResults := &mocks.MockBatchResults{}

	// Setup mock expectations - all attempts fail
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(mockBatchResults).Times(testMaxRetries)
	mockBatchResults.On("Exec").Return(pgconn.CommandTag{}, errors.New(mockErrorMsg)).Times(testMaxRetries)
	mockBatchResults.On("Close").Return(nil).Times(testMaxRetries)

	executor := NewBatchExecutor(mockPool, config, logger)

	// Create test queue
	queue := []*pgx.QueuedQuery{
		{SQL: testQuerySQL, Arguments: []any{1, "test1"}},
	}

	ackCalled := false
	ackFunc := func() error {
		ackCalled = true
		return nil
	}

	// Execute batch with retry
	err := executor.executeBatchWithRetry(context.Background(), queue, ackFunc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), mockErrorMsg)
	assert.False(t, ackCalled)
	mockPool.AssertExpectations(t)
	mockBatchResults.AssertExpectations(t)
}

func TestBatchExecutor_Start_BatchProcessing(t *testing.T) {
	config := BatchConfig{
		BulkSize:   2,
		Timeout:    50 * time.Millisecond,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	mockPool := &mocks.MockDatabasePool{}
	mockBatchResults := &mocks.MockBatchResults{}

	// Setup mock expectations
	mockPool.On("SendBatch", mock.Anything, mock.MatchedBy(func(batch *pgx.Batch) bool {
		return len(batch.QueuedQueries) == 2
	})).Return(mockBatchResults)
	mockBatchResults.On("Exec").Return(pgconn.CommandTag{}, nil).Times(2)
	mockBatchResults.On("Close").Return(nil)

	executor := NewBatchExecutor(mockPool, config, logger)
	messages := make(chan Message, 10)

	ctx, cancel := context.WithCancel(context.Background())

	// Start executor in goroutine
	go executor.Start(ctx, messages)

	// Send messages
	messages <- createTestMessage(1, "test1")
	messages <- createTestMessage(2, "test2")

	// Wait a bit for processing
	time.Sleep(20 * time.Millisecond)

	// Cancel context and wait
	cancel()
	time.Sleep(10 * time.Millisecond)

	mockPool.AssertExpectations(t)
	mockBatchResults.AssertExpectations(t)
}

func TestBatchExecutor_Start_TimeoutProcessing(t *testing.T) {
	config := BatchConfig{
		BulkSize:   5, // Higher than number of messages we'll send
		Timeout:    30 * time.Millisecond,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	mockPool := &mocks.MockDatabasePool{}
	mockBatchResults := &mocks.MockBatchResults{}

	// Setup mock expectations
	mockPool.On("SendBatch", mock.Anything, mock.MatchedBy(func(batch *pgx.Batch) bool {
		return len(batch.QueuedQueries) == 1
	})).Return(mockBatchResults)
	mockBatchResults.On("Exec").Return(pgconn.CommandTag{}, nil).Once()
	mockBatchResults.On("Close").Return(nil)

	executor := NewBatchExecutor(mockPool, config, logger)
	messages := make(chan Message, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Start executor in goroutine
	go executor.Start(ctx, messages)

	// Send one message (less than bulk size)
	messages <- createTestMessage(1, "test1")

	// Wait for timeout processing
	time.Sleep(60 * time.Millisecond)

	mockPool.AssertExpectations(t)
	mockBatchResults.AssertExpectations(t)
}

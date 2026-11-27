package gopqcdcpq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc-pq/config"
	"github.com/Trendyol/go-pq-cdc-pq/mocks"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// okCommand is a command tag returned on successful Exec calls.
const okCommand = "INSERT 0 1"

func TestSinkFailureRetainsQueueAndDoesNotAck(t *testing.T) {
	ctx := context.Background()
	mockPool := &mocks.MockDatabasePool{}
	mockResults := &mocks.MockBatchResults{}

	applyErr := errors.New("target unreachable")
	mockResults.On("Exec").Return(pgconn.CommandTag{}, applyErr).Maybe()
	mockResults.On("Close").Return(nil).Maybe()
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(mockResults).Maybe()

	cfg := config.BatchConfig{
		BulkSize:   100,
		Timeout:    time.Hour,
		MaxRetries: 1,
		RetryDelay: time.Millisecond,
	}
	sink := NewSink(mockPool, cfg, nil)

	ackCalled := false
	ch := make(chan Message, 1)
	ch <- Message{
		Query: "INSERT INTO users VALUES ($1)",
		Args:  []any{1},
		Ack: func() error {
			ackCalled = true
			return nil
		},
	}
	close(ch)

	err := sink.Start(ctx, ch)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "target apply failed")
	// A failed batch must not be acknowledged: this is the fail-closed fix.
	assert.False(t, ackCalled, "ack must not be called when the batch fails")
}

func TestSinkSuccessDrainsQueueAndAcks(t *testing.T) {
	ctx := context.Background()
	mockPool := &mocks.MockDatabasePool{}
	mockResults := &mocks.MockBatchResults{}

	mockResults.On("Exec").Return(pgconn.NewCommandTag(okCommand), nil).Maybe()
	mockResults.On("Close").Return(nil).Maybe()
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(mockResults).Maybe()

	cfg := createTestConfig().BatchConfig
	sink := NewSink(mockPool, cfg, nil)

	ackCalled := false
	ch := make(chan Message, 1)
	ch <- Message{
		Query: "INSERT INTO users VALUES ($1)",
		Args:  []any{1},
		Ack: func() error {
			ackCalled = true
			return nil
		},
	}
	close(ch)

	err := sink.Start(ctx, ch)
	assert.NoError(t, err)
	assert.True(t, ackCalled, "ack should be called after a successful flush")
}

func TestSinkOrderedBarrier(t *testing.T) {
	ctx := context.Background()
	mockPool := &mocks.MockDatabasePool{}
	mockResults := &mocks.MockBatchResults{}

	mockResults.On("Exec").Return(pgconn.NewCommandTag(okCommand), nil).Maybe()
	mockResults.On("Close").Return(nil).Maybe()
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(mockResults).Maybe()

	cfg := createTestConfig().BatchConfig
	sink := NewSink(mockPool, cfg, nil)

	dataAcked := false
	barrierAcked := false
	ch := make(chan Message, 2)
	ch <- Message{
		Query: "INSERT INTO users VALUES ($1)",
		Args:  []any{1},
		Ack: func() error {
			dataAcked = true
			return nil
		},
	}
	// Barrier (empty Query): flush prior writes, then acknowledge in order.
	ch <- Message{
		Ack: func() error {
			barrierAcked = true
			return nil
		},
	}
	close(ch)

	err := sink.Start(ctx, ch)
	assert.NoError(t, err)
	// Data ack fires during the barrier flush; barrier ack fires after.
	assert.True(t, dataAcked, "data ack should be called before the barrier ack")
	assert.True(t, barrierAcked, "barrier ack should be called after prior writes flush")
}

func TestSinkCloseFailureRetainsQueueAndDoesNotAck(t *testing.T) {
	mockPool := &mocks.MockDatabasePool{}
	mockResults := &mocks.MockBatchResults{}
	closeErr := errors.New("batch close failed")
	mockResults.On("Exec").Return(pgconn.NewCommandTag(okCommand), nil).Once()
	mockResults.On("Close").Return(closeErr).Once()
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(mockResults).Once()

	cfg := config.BatchConfig{BulkSize: 100, Timeout: time.Hour, MaxRetries: 1, RetryDelay: time.Millisecond}
	sink := NewSink(mockPool, cfg, nil)
	ackCalled := false
	message := Message{
		Query: "INSERT INTO users VALUES ($1)",
		Args:  []any{1},
		Ack: func() error {
			ackCalled = true
			return nil
		},
	}
	ch := make(chan Message, 1)
	ch <- message
	close(ch)

	err := sink.Start(context.Background(), ch)
	assert.ErrorIs(t, err, closeErr)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "target apply failed")
	}
	assert.False(t, ackCalled)
	if assert.Len(t, sink.queue, 1) {
		assert.Equal(t, message.Query, sink.queue[0].SQL)
		assert.Equal(t, message.Args, sink.queue[0].Arguments)
		assert.NotNil(t, sink.acks[0])
	}
	mockPool.AssertExpectations(t)
	mockResults.AssertExpectations(t)
}

func TestSinkExecFailureBlocksBarrierAckAndRetainsQueue(t *testing.T) {
	mockPool := &mocks.MockDatabasePool{}
	mockResults := &mocks.MockBatchResults{}
	applyErr := errors.New("query failed")
	mockResults.On("Exec").Return(pgconn.CommandTag{}, applyErr).Once()
	mockResults.On("Close").Return(nil).Once()
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(mockResults).Once()

	cfg := config.BatchConfig{BulkSize: 100, Timeout: time.Hour, MaxRetries: 1, RetryDelay: time.Millisecond}
	sink := NewSink(mockPool, cfg, nil)
	dataAcked, barrierAcked := false, false
	message := Message{
		Query: "INSERT INTO users VALUES ($1)",
		Args:  []any{1},
		Ack: func() error {
			dataAcked = true
			return nil
		},
	}
	ch := make(chan Message, 2)
	ch <- message
	ch <- Message{Ack: func() error {
		barrierAcked = true
		return nil
	}}
	close(ch)

	err := sink.Start(context.Background(), ch)
	assert.ErrorIs(t, err, applyErr)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "target apply failed")
	}
	assert.False(t, dataAcked)
	assert.False(t, barrierAcked)
	if assert.Len(t, sink.queue, 1) {
		assert.Equal(t, message.Query, sink.queue[0].SQL)
		assert.Equal(t, message.Args, sink.queue[0].Arguments)
		assert.NotNil(t, sink.acks[0])
	}
	mockPool.AssertExpectations(t)
	mockResults.AssertExpectations(t)
}

func TestNewSinkPreservesConfig(t *testing.T) {
	mockPool := &mocks.MockDatabasePool{}

	// The unused logger may be nil; the configuration is retained unchanged.
	cfg := config.BatchConfig{
		BulkSize:   10,
		Timeout:    time.Second,
		MaxRetries: 2,
		RetryDelay: time.Millisecond,
	}
	assert.NotPanics(t, func() {
		sink := NewSink(mockPool, cfg, nil)
		assert.Equal(t, cfg, sink.config)
		assert.Empty(t, sink.queue)
		assert.Empty(t, sink.acks)
	})
}

func TestSinkAcknowledgesEachActionWhenLastStatementHasNilAck(t *testing.T) {
	mockPool := &mocks.MockDatabasePool{}
	mockResults := &mocks.MockBatchResults{}
	mockResults.On("Exec").Return(pgconn.NewCommandTag(okCommand), nil).Times(3)
	committed := false
	mockResults.On("Close").Return(nil).Once().Run(func(mock.Arguments) {
		committed = true
	})
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(mockResults).Once()

	sink := NewSink(mockPool, config.BatchConfig{BulkSize: 100, Timeout: time.Hour, MaxRetries: 1}, nil)
	var acknowledged []int
	ch := make(chan Message, 3)
	for i := 1; i <= 2; i++ {
		ch <- Message{Query: "INSERT INTO users VALUES ($1)", Args: []any{i}, Ack: func() error {
			assert.True(t, committed, "ack must follow successful batch close")
			acknowledged = append(acknowledged, i)
			return nil
		}}
	}
	ch <- Message{Query: "INSERT INTO users VALUES ($1)", Args: []any{3}}
	close(ch)

	assert.NoError(t, sink.Start(context.Background(), ch))
	assert.Equal(t, []int{1, 2}, acknowledged)
	assert.Empty(t, sink.queue)
	assert.Empty(t, sink.acks)
	mockPool.AssertExpectations(t)
	mockResults.AssertExpectations(t)
}

func TestSinkNonpositiveMaxRetriesFailsWithoutAck(t *testing.T) {
	for _, retries := range []int{0, -1} {
		sink := NewSink(&mocks.MockDatabasePool{}, config.BatchConfig{BulkSize: 100, Timeout: time.Hour, MaxRetries: retries}, nil)
		acked := false
		sink.enqueue(Message{Query: "SELECT 1", Ack: func() error {
			acked = true
			return nil
		}})
		assert.Error(t, sink.flush(context.Background()))
		assert.False(t, acked)
		assert.Len(t, sink.queue, 1)
		assert.Len(t, sink.acks, 1)
	}
}

func TestSinkRetryRebuildsQueuedQueries(t *testing.T) {
	mockPool := &mocks.MockDatabasePool{}
	failedResults := &mocks.MockBatchResults{}
	failedResults.On("Exec").Return(pgconn.CommandTag{}, errors.New("retry me")).Once()
	failedResults.On("Close").Return(nil).Once()
	successResults := &mocks.MockBatchResults{}
	successResults.On("Exec").Return(pgconn.NewCommandTag(okCommand), nil).Once()
	successResults.On("Close").Return(nil).Once()
	var firstQuery *pgx.QueuedQuery
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(failedResults).Once().Run(func(args mock.Arguments) {
		firstQuery = args.Get(1).(*pgx.Batch).QueuedQueries[0]
		firstQuery.SQL = "mutated"
		firstQuery.Arguments = []any{"mutated"}
	})
	mockPool.On("SendBatch", mock.Anything, mock.AnythingOfType("*pgx.Batch")).Return(successResults).Once().Run(func(args mock.Arguments) {
		query := args.Get(1).(*pgx.Batch).QueuedQueries[0]
		assert.NotSame(t, firstQuery, query)
		assert.Equal(t, "SELECT $1", query.SQL)
		assert.Equal(t, []any{1}, query.Arguments)
	})
	sink := NewSink(mockPool, config.BatchConfig{BulkSize: 100, Timeout: time.Hour, MaxRetries: 2}, nil)
	sink.enqueue(Message{Query: "SELECT $1", Args: []any{1}})
	assert.NoError(t, sink.flush(context.Background()))
	mockPool.AssertExpectations(t)
	failedResults.AssertExpectations(t)
	successResults.AssertExpectations(t)
}

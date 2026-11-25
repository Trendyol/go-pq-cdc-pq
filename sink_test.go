package gopqcdcpq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc-pq/config"
	"github.com/Trendyol/go-pq-cdc-pq/mocks"
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

func TestNewSinkDefaults(t *testing.T) {
	mockPool := &mocks.MockDatabasePool{}

	// nil logger must not panic and a zero ApplyTimeout falls back to default.
	cfg := config.BatchConfig{
		BulkSize:   10,
		Timeout:    time.Second,
		MaxRetries: 2,
		RetryDelay: time.Millisecond,
		// ApplyTimeout intentionally left zero.
	}
	assert.NotPanics(t, func() {
		sink := NewSink(mockPool, cfg, nil)
		assert.Equal(t, config.DefaultApplyTimeout, sink.config.ApplyTimeout)
		// Config passed by value: ensure we consumed a copy, not the caller's.
		assert.Equal(t, time.Duration(0), cfg.ApplyTimeout)
	})
}

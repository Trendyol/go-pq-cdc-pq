package mocks

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/mock"
)

// MockDatabasePool is a mock implementation of Pool interface
type MockDatabasePool struct {
	mock.Mock
}

func (m *MockDatabasePool) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	args := m.Called(ctx, batch)
	return args.Get(0).(pgx.BatchResults)
}

func (m *MockDatabasePool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(pgx.Row)
}

func (m *MockDatabasePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(pgx.Rows), mockArgs.Error(1)
}

func (m *MockDatabasePool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	mockArgs := m.Called(ctx, sql, arguments)
	return mockArgs.Get(0).(pgconn.CommandTag), mockArgs.Error(1)
}

func (m *MockDatabasePool) Close() {
	m.Called()
}

// MockBatchResults is a mock implementation of pgx.BatchResults
type MockBatchResults struct {
	mock.Mock
}

func (m *MockBatchResults) Exec() (pgconn.CommandTag, error) {
	args := m.Called()
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *MockBatchResults) Query() (pgx.Rows, error) {
	args := m.Called()
	return args.Get(0).(pgx.Rows), args.Error(1)
}

func (m *MockBatchResults) QueryRow() pgx.Row {
	args := m.Called()
	return args.Get(0).(pgx.Row)
}

func (m *MockBatchResults) Close() error {
	args := m.Called()
	return args.Error(0)
}

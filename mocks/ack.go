package mocks

import "github.com/stretchr/testify/mock"

// MockAckFunc is a mock implementation of ack function
type MockAckFunc struct {
	mock.Mock
}

func (m *MockAckFunc) Ack() error {
	args := m.Called()
	return args.Error(0)
}

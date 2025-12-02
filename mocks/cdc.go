package mocks

import (
	"context"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
)

// MockCDCConnector is a mock implementation of cdc.Connector
type MockCDCConnector struct {
	mock.Mock
}

func (m *MockCDCConnector) Start(ctx context.Context) {
	m.Called(ctx)
}

func (m *MockCDCConnector) WaitUntilReady(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockCDCConnector) Close() {
	m.Called()
}

func (m *MockCDCConnector) GetConfig() *cdcconfig.Config {
	args := m.Called()
	return args.Get(0).(*cdcconfig.Config)
}

func (m *MockCDCConnector) SetMetricCollectors(collectors ...prometheus.Collector) {
	m.Called(collectors)
}

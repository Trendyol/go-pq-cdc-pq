package connector

import (
	"time"
)

// Constants for actions and defaults
const (
	DefaultSchema     = "public"
	DefaultPrimarykey = "id"

	ActionDelete = "DELETE"
	ActionInsert = "INSERT"
	ActionUpdate = "UPDATE"
)

// BatchConfig holds configuration for batch processing
type BatchConfig struct {
	BulkSize   int
	Timeout    time.Duration
	MaxRetries int
	RetryDelay time.Duration
}

// Message represents a CDC message with all necessary information
type Message struct {
	Query     string
	Args      []any
	Ack       func() error
	Schema    string
	Table     string
	Action    string // INSERT, UPDATE, DELETE
	OldKeys   map[string]any
	NewValues map[string]any
}

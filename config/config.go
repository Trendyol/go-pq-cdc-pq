package config

import (
	"fmt"
	"os"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"gopkg.in/yaml.v3"
)

// BatchConfig holds configuration for batch processing
type BatchConfig struct {
	BulkSize   int           `yaml:"bulk_size"`
	Timeout    time.Duration `yaml:"timeout"`
	MaxRetries int           `yaml:"max_retries"`
	RetryDelay time.Duration `yaml:"retry_delay"`
}

// Connector represents the complete connector configuration
type Connector struct {
	CDC              config.Config     `yaml:"cdc" mapstructure:"cdc"`
	App              AppConfig         `yaml:"app"`
	Server           ServerConfig      `yaml:"server"`
	Postgres         PostgresConfig    `yaml:"postgres"`
	ConnectorConfig  ConnectorConfig   `yaml:"connector"`
	BatchConfig      BatchConfig       `yaml:"batch"`
	TablePrimaryKeys map[string]string `yaml:"tablePrimaryKeys" mapstructure:"tablePrimaryKeys"`
}

// AppConfig holds basic application metadata.
type AppConfig struct {
	Name string `yaml:"name"`
}

// ServerConfig describes the embedded server configuration.
type ServerConfig struct {
	Port int `yaml:"port"`
}

// ConnectorConfig contains CDC connector options.
type ConnectorConfig struct {
	Publication                  string        `yaml:"publication"`
	Table                        string        `yaml:"table"`
	Schema                       string        `yaml:"schema"`
	Slot                         string        `yaml:"slot"`
	SlotActivityCheckerInterval  time.Duration `yaml:"slot_activity_checker_interval"`
	CreateIfNotExists            bool          `yaml:"create_if_not_exists"`
	BulkSize                     int           `yaml:"bulk_size"`
	Timeout                      time.Duration `yaml:"timeout"`
	MaxRetries                   int           `yaml:"max_retries"`
	RetryDelay                   time.Duration `yaml:"retry_delay"`
	DebugMode                    bool          `yaml:"debug_mode"`
	LogLevel                     int           `yaml:"log_level"`
	CreateTargetTableIfNotExists bool          `yaml:"create_target_table_if_not_exists"`
	ReplicaIdentity              string        `yaml:"replica_identity"` // DEFAULT or FULL
}

// PostgresConfig carries both source and target database configurations.
type PostgresConfig struct {
	Source DatabaseConfig `yaml:"source"`
	Target DatabaseConfig `yaml:"target"`
}

// DatabaseConfig defines a generic PostgreSQL connection definition.
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DB       string `yaml:"db"`
}

// Load reads configuration from the provided path.
func Load(path string) (*Connector, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Connector
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	cfg.SetDefault()
	return &cfg, nil
}

// SetDefault sets default values for configuration fields
func (c *Connector) SetDefault() {
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}

	if c.ConnectorConfig.BulkSize == 0 {
		c.ConnectorConfig.BulkSize = 10000
	}

	if c.ConnectorConfig.Timeout == 0 {
		c.ConnectorConfig.Timeout = 30 * time.Second
	}

	if c.ConnectorConfig.MaxRetries == 0 {
		c.ConnectorConfig.MaxRetries = 3
	}

	if c.ConnectorConfig.RetryDelay == 0 {
		c.ConnectorConfig.RetryDelay = time.Second
	}

	if c.ConnectorConfig.SlotActivityCheckerInterval == 0 {
		c.ConnectorConfig.SlotActivityCheckerInterval = 3 * time.Second
	}

	// Populate CDC config from Postgres source and Connector config
	if c.CDC.Host == "" {
		c.CDC.Host = c.Postgres.Source.Host
	}
	if c.CDC.Username == "" {
		c.CDC.Username = c.Postgres.Source.User
	}
	if c.CDC.Password == "" {
		c.CDC.Password = c.Postgres.Source.Password
	}
	if c.CDC.Database == "" {
		c.CDC.Database = c.Postgres.Source.DB
	}
	if c.CDC.Publication.Name == "" {
		c.CDC.Publication.Name = c.ConnectorConfig.Publication
	}
	if !c.CDC.Publication.CreateIfNotExists {
		c.CDC.Publication.CreateIfNotExists = c.ConnectorConfig.CreateIfNotExists
	}
	// Set default ReplicaIdentity if not specified
	if c.ConnectorConfig.ReplicaIdentity == "" {
		c.ConnectorConfig.ReplicaIdentity = "DEFAULT"
	}

	// Populate Publication Tables if empty
	if len(c.CDC.Publication.Tables) == 0 && c.ConnectorConfig.Table != "" {
		schema := c.ConnectorConfig.Schema
		if schema == "" {
			schema = "public"
		}
		c.CDC.Publication.Tables = publication.Tables{
			{
				Name:            c.ConnectorConfig.Table,
				Schema:          schema,
				ReplicaIdentity: c.ConnectorConfig.ReplicaIdentity,
			},
		}
	}

	// Ensure ReplicaIdentity is set for all tables
	for i := range c.CDC.Publication.Tables {
		if c.CDC.Publication.Tables[i].ReplicaIdentity == "" {
			c.CDC.Publication.Tables[i].ReplicaIdentity = c.ConnectorConfig.ReplicaIdentity
		}
	}
	// Populate Publication Operations if empty (default: INSERT, UPDATE, DELETE)
	if len(c.CDC.Publication.Operations) == 0 {
		c.CDC.Publication.Operations = publication.Operations{
			publication.OperationInsert,
			publication.OperationUpdate,
			publication.OperationDelete,
		}
	}
	if c.CDC.Slot.Name == "" {
		c.CDC.Slot.Name = c.ConnectorConfig.Slot
	}
	if c.CDC.Slot.SlotActivityCheckerInterval == 0 {
		c.CDC.Slot.SlotActivityCheckerInterval = c.ConnectorConfig.SlotActivityCheckerInterval
	}
	if !c.CDC.Slot.CreateIfNotExists {
		c.CDC.Slot.CreateIfNotExists = c.ConnectorConfig.CreateIfNotExists
	}

	// Set CDC config defaults if needed
	if c.CDC.Metric.Port == 0 {
		c.CDC.Metric.Port = c.Server.Port
	}

	// Ensure underlying CDC config applies its own defaults (snapshot, logger, etc.).
	c.CDC.SetDefault()
}

// Config is an alias for Connector to maintain backward compatibility
type Config = Connector

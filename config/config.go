package config

import (
	"fmt"
	"os"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"gopkg.in/yaml.v3"
)

// BatchConfig holds configuration for batch processing
type BatchConfig struct {
	BulkSize   int
	Timeout    time.Duration
	MaxRetries int
	RetryDelay time.Duration
}

// Connector represents the complete connector configuration
type Connector struct {
	CDC             config.Config   `yaml:"cdc" mapstructure:"cdc"`
	App             AppConfig       `yaml:"app"`
	Server          ServerConfig    `yaml:"server"`
	Postgres        PostgresConfig  `yaml:"postgres"`
	ConnectorConfig ConnectorConfig `yaml:"connector"`
	BatchConfig     BatchConfig     `yaml:"batch"`
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

	// Set CDC config defaults if needed
	if c.CDC.Metric.Port == 0 {
		c.CDC.Metric.Port = c.Server.Port
	}
}

// Config is an alias for Connector to maintain backward compatibility
type Config = Connector

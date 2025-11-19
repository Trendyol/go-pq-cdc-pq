package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the full application configuration.
type Config struct {
	App         AppConfig         `yaml:"app"`
	Server      ServerConfig      `yaml:"server"`
	Connector   ConnectorConfig   `yaml:"connector"`
	InitialLoad InitialLoadConfig `yaml:"initial_load"`
	Postgres    PostgresConfig    `yaml:"postgres"`
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

// InitialLoadConfig holds configuration for initial data load.
type InitialLoadConfig struct {
	Enabled    bool   `yaml:"enabled"`
	BatchSize  int    `yaml:"batch_size"`
	TableName  string `yaml:"table_name"`
	PrimaryKey string `yaml:"primary_key"`
	Condition  string `yaml:"condition"`
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
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	cfg.setDefaults()
	return &cfg, nil
}

func (c *Config) setDefaults() {
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}

	if c.Connector.BulkSize == 0 {
		c.Connector.BulkSize = 10000
	}

	if c.Connector.Timeout == 0 {
		c.Connector.Timeout = 30 * time.Second
	}

	if c.Connector.MaxRetries == 0 {
		c.Connector.MaxRetries = 3
	}

	if c.Connector.RetryDelay == 0 {
		c.Connector.RetryDelay = time.Second
	}

	if c.Connector.SlotActivityCheckerInterval == 0 {
		c.Connector.SlotActivityCheckerInterval = 3 * time.Second
	}

	if c.InitialLoad.BatchSize == 0 {
		c.InitialLoad.BatchSize = 1000
	}

	if c.InitialLoad.TableName == "" {
		c.InitialLoad.TableName = c.Connector.Table
	}

	if c.InitialLoad.PrimaryKey == "" {
		c.InitialLoad.PrimaryKey = "id"
	}

	if c.InitialLoad.Condition == "" {
		c.InitialLoad.Condition = "1=1"
	}
}

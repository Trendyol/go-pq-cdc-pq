package gopqcdcpq

import (
	"os"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
)

// Config captures user configurable settings for the connector.
type Config struct {
	ConfigPath        string
	PublicationTables []publication.Table
	PrimaryKey        string
	DefaultSchema     string
}

// Option represents an option for customizing the connector config.
type Option func(*Config)

func defaultConfig() Config {
	return Config{
		ConfigPath:    os.Getenv("CONFIG_FILE"),
		PrimaryKey:    "id",
		DefaultSchema: "public",
	}
}

// WithConfigPath overrides the location of the main configuration file.
func WithConfigPath(path string) Option {
	return func(cfg *Config) {
		cfg.ConfigPath = path
	}
}

// WithPublicationTables overrides the publication tables used while creating
// the CDC publication. When not provided, defaults are used.
func WithPublicationTables(tables ...publication.Table) Option {
	return func(cfg *Config) {
		cfg.PublicationTables = append([]publication.Table(nil), tables...)
	}
}

// WithPrimaryKey overrides the default primary key column name used for
// building upsert and delete queries. Defaults to "id".
func WithPrimaryKey(primaryKey string) Option {
	return func(cfg *Config) {
		cfg.PrimaryKey = primaryKey
	}
}

// WithDefaultSchema overrides the default schema name used for message processing.
// Defaults to "public".
func WithDefaultSchema(schema string) Option {
	return func(cfg *Config) {
		cfg.DefaultSchema = schema
	}
}

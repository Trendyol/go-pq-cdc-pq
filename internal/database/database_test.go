package database

import (
	"context"
	"testing"

	"github.com/Trendyol/go-pq-cdc-pq/config"
	"github.com/stretchr/testify/assert"
)

func TestNewTargetPoolNilConfig(t *testing.T) {
	pool, err := NewTargetPool(context.Background(), nil)
	assert.Nil(t, pool)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connector configuration is required")
}

func TestBuildPGXConfig(t *testing.T) {
	dbCfg := config.DatabaseConfig{
		Host:     "h",
		User:     "u",
		Password: "p", // simple password so the original fmt.Sprintf conn string parses
		DB:       "d",
	}

	cfg, err := buildPGXConfig(dbCfg)
	assert.NoError(t, err)
	assert.NotNil(t, cfg)
	conn := cfg.ConnConfig
	assert.Equal(t, "h", conn.Host)
	assert.Equal(t, "d", conn.Database)
	// default port applied when unset
	assert.Equal(t, uint16(5432), conn.Port)
}

func TestBuildPGXConfigExplicitPort(t *testing.T) {
	dbCfg := config.DatabaseConfig{
		Host:     "h",
		Port:     6432,
		User:     "u",
		Password: "p",
		DB:       "d",
	}

	cfg, err := buildPGXConfig(dbCfg)
	assert.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, uint16(6432), cfg.ConnConfig.Port)
}

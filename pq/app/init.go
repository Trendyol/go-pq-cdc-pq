package app

import (
	"context"
	"fmt"
	"os"

	"github.com/Trendyol/go-pq-cdc-pq/internal/config"
	"github.com/Trendyol/go-pq-cdc-pq/internal/otel"
	"github.com/Trendyol/go-pq-cdc-pq/internal/tracer"
)

// =============================================================================
// Application Initialization Functions
// =============================================================================

// InitConfig carries parameters required for initializing the application runtime.
type InitConfig struct {
	ConfigPath string
}

// InitializeApplication initializeApplication sets up logging, telemetry and configuration
// using environment variables for inputs.
func InitializeApplication() (*config.Config, func(context.Context) error, context.Context, error) {
	return InitializeApplicationWithConfig(InitConfig{
		ConfigPath: os.Getenv("CONFIG_FILE"),
	})
}

// InitializeApplicationWithConfig performs the initialization using the provided config paths.
func InitializeApplicationWithConfig(initCfg InitConfig) (*config.Config, func(context.Context) error, context.Context, error) {
	// Initialize tracer first (this sets up slog-context and OTEL)
	ctx := tracer.Initialize()

	// Initialize OpenTelemetry (this might be redundant now, but keeping for compatibility)
	otelCloseFn := otel.Initialize()

	// Create combined close function
	closeFn := func(ctx context.Context) error {
		// Shutdown tracer first
		if err := tracer.Shutdown(ctx); err != nil {
			return err
		}
		// Then OTEL
		return otelCloseFn(ctx)
	}

	configPath := initCfg.ConfigPath
	if configPath == "" {
		configPath = "config/config.yaml"
	}

	// Load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, closeFn, ctx, fmt.Errorf("failed to load config: %w", err)
	}

	return cfg, closeFn, ctx, nil
}

package gopqcdcpq

import (
	"context"
	"fmt"
	"sync"

	"github.com/Trendyol/go-pq-cdc/pq/publication"

	appconfig "github.com/Trendyol/go-pq-cdc-pq/internal/config"
	"github.com/Trendyol/go-pq-cdc-pq/pq/app"
)

// Connector orchestrates the CDC lifecycle (initial load + replication listener).
type Connector struct {
	cfg       *appconfig.Config
	closeFn   func(context.Context) error
	ctx       context.Context
	cancel    context.CancelFunc
	tables    []publication.Table
	stopOnce  sync.Once
	closeOnce sync.Once
}

// NewConnector constructs a ready-to-run Connector using the provided options.
func NewConnector(options ...Option) (*Connector, error) {
	opts := defaultConfig()
	for _, option := range options {
		option(&opts)
	}

	initCfg := app.InitConfig{
		ConfigPath: opts.ConfigPath,
	}

	cfg, closeFn, tracerCtx, err := app.InitializeApplicationWithConfig(initCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize application: %w", err)
	}

	ctx, cancel := context.WithCancel(tracerCtx)

	tables := opts.PublicationTables
	if len(tables) == 0 {
		tables = app.CreatePublicationTables(cfg)
	}

	return &Connector{
		cfg:     cfg,
		closeFn: closeFn,
		ctx:     ctx,
		cancel:  cancel,
		tables:  tables,
	}, nil
}

// Context returns the long-lived context used by the connector runtime.
func (c *Connector) Context() context.Context {
	if c == nil {
		return context.Background()
	}
	return c.ctx
}

// Start boots the CDC service. Typically executed inside a goroutine.
func (c *Connector) Start() error {
	if c == nil {
		return fmt.Errorf("connector is nil")
	}
	return app.StartService(c.ctx, c.cfg, c.tables)
}

// Stop requests a graceful shutdown by cancelling the connector context.
func (c *Connector) Stop() {
	if c == nil || c.cancel == nil {
		return
	}
	c.stopOnce.Do(func() {
		c.cancel()
	})
}

// Shutdown stops the connector and releases telemetry resources.
func (c *Connector) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	c.Stop()

	var err error
	c.closeOnce.Do(func() {
		if c.closeFn != nil {
			err = c.closeFn(ctx)
		}
	})

	return err
}

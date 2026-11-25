package gopqcdcpq

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	slogctx "github.com/veqryn/slog-context"
)

const (
	// shutdownTimeout bounds how long Close waits for the upstream connector.
	shutdownTimeout = 30 * time.Second
	// startupReadyGrace lets a healthy CDC connector reach readiness and stop.
	startupReadyGrace = 250 * time.Millisecond
)

// initLifecycle lazily creates the lifecycle channels if Start/Close are called
// before NewConnector's internal setup completes. Safe to call repeatedly.
func (c *connector) initLifecycle() {
	c.lifecycleOnce.Do(func() {
		c.lifecycleMu.Lock()
		defer c.lifecycleMu.Unlock()
		if c.runCtx == nil {
			c.runCtx, c.cancel = context.WithCancel(context.Background())
		}
		c.stopped = make(chan struct{})
		c.cdcReady = make(chan struct{})
	})
}

// Start launches the target sink and source. The connector cannot be restarted
// after shutdown; construct a new instance with the same slot to replay after
// the target recovers.
func (c *connector) Start(ctx context.Context) {
	c.initLifecycle()
	c.lifecycleMu.Lock()
	if c.started || c.closing || c.fatalErr != nil {
		c.lifecycleMu.Unlock()
		return
	}
	c.started = true
	if c.cancel != nil {
		c.cancel()
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.runCtx, c.cancel = runCtx, cancel
	c.sinkStopped = make(chan struct{})
	c.cdcStopped = make(chan struct{})
	c.cdcStartDone = make(chan struct{})
	sinkStopped, cdcStopped, cdcStartDone := c.sinkStopped, c.cdcStopped, c.cdcStartDone
	c.lifecycleMu.Unlock()

	// Target sink: any non-shutdown error is terminal. The queue is retained on
	// failure so the source position is not advanced past lost writes.
	go func() {
		defer close(sinkStopped)
		if c.sink == nil {
			c.fail(fmt.Errorf("target sink is not initialized"))
			return
		}
		if err := c.sink.Start(slogctx.Append(runCtx, "component", "Sink"), c.messages); err != nil && runCtx.Err() == nil {
			c.fail(fmt.Errorf("target sink stopped: %w", err))
		}
	}()

	// Source: propagate upstream startup/shutdown/panic failures.
	go func() {
		defer close(cdcStopped)
		defer func() {
			if recovered := recover(); recovered != nil {
				c.fail(fmt.Errorf("CDC connector panic: %v", recovered))
			}
		}()
		if runCtx.Err() != nil || c.cdc == nil {
			return
		}
		ready := make(chan error, 1)
		go func() {
			err := c.cdc.WaitUntilReady(runCtx)
			c.lifecycleMu.Lock()
			c.cdcReadyErr = err
			c.lifecycleMu.Unlock()
			c.cdcReadyOnce.Do(func() { close(c.cdcReady) })
			ready <- err
		}()
		defer close(cdcStartDone)
		c.cdc.Start(runCtx)
		if runCtx.Err() != nil {
			return
		}
		select {
		case err := <-ready:
			if err != nil {
				c.fail(fmt.Errorf("CDC connector stopped before becoming ready: %w", err))
				return
			}
			// Upstream Stop returns after a graceful shutdown signal.
			c.cancel()
		case <-time.After(startupReadyGrace):
			if c.cfg != nil && c.cfg.CDC.IsSnapshotOnlyMode() {
				c.cancel()
				return
			}
			c.fail(fmt.Errorf("CDC connector stopped before becoming ready"))
		}
	}()

	go func() {
		<-runCtx.Done()
		c.Close()
	}()
}

// WaitForShutdown reports the terminal failure to every waiter and responds to
// caller cancellation or process termination signals.
func (c *connector) WaitForShutdown(ctx context.Context) error {
	c.initLifecycle()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	select {
	case sig := <-sigChan:
		slogctx.FromCtx(ctx).Info("shutdown signal received", "signal", sig.String())
		c.Close()
		return c.shutdownError(nil)
	case <-c.stopped:
		return c.shutdownError(nil)
	case <-ctx.Done():
		return c.shutdownError(ctx.Err())
	}
}

func (c *connector) shutdownError(fallback error) error {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	if c.fatalErr != nil {
		return c.fatalErr
	}
	return fallback
}

// Close cancels ingestion before closing resources. It serializes channel
// closure with event submission so blocked producers exit without panics.
func (c *connector) Close() {
	c.initLifecycle()
	c.closeOnce.Do(func() {
		c.lifecycleMu.Lock()
		c.closing = true
		if c.cancel != nil {
			c.cancel()
		}
		sinkStopped, cdcStopped, cdcStartDone := c.sinkStopped, c.cdcStopped, c.cdcStartDone
		c.lifecycleMu.Unlock()

		if c.cdc != nil {
			c.waitForCDCStart(cdcStartDone)
			c.cdc.Close()
		}
		c.sendMu.Lock()
		if c.messages != nil {
			close(c.messages)
		}
		c.sendMu.Unlock()
		if sinkStopped != nil {
			select {
			case <-sinkStopped:
			case <-time.After(shutdownTimeout):
				slog.Default().Error("timed out waiting for sink to stop")
			}
		}
		if cdcStopped != nil {
			select {
			case <-cdcStopped:
			case <-time.After(shutdownTimeout):
				slog.Default().Error("timed out waiting for CDC connector to stop")
			}
		}
		if c.pool != nil {
			c.pool.Close()
		}
		close(c.stopped)
	})
}

// waitUntilReady blocks until the upstream connector reports readiness.
func (c *connector) waitUntilReady(ctx context.Context) error {
	c.initLifecycle()
	select {
	case <-c.cdcReady:
		c.lifecycleMu.RLock()
		defer c.lifecycleMu.RUnlock()
		return c.cdcReadyErr
	case <-c.stopped:
		if err := c.shutdownError(nil); err != nil {
			return err
		}
		return fmt.Errorf("connector stopped before becoming ready")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitForCDCStart avoids closing the upstream readiness channel concurrently
// with its Start send.
func (c *connector) waitForCDCStart(startDone <-chan struct{}) {
	if startDone == nil {
		return
	}
	timer := time.NewTimer(shutdownTimeout)
	defer timer.Stop()
	select {
	case <-c.cdcReady:
		c.lifecycleMu.RLock()
		readyErr := c.cdcReadyErr
		c.lifecycleMu.RUnlock()
		if readyErr == nil {
			return
		}
		select {
		case <-startDone:
		case <-timer.C:
			slog.Default().Error("timed out waiting for CDC startup to stop")
		}
	case <-startDone:
	case <-timer.C:
		slog.Default().Error("timed out waiting for CDC readiness or startup to stop")
	}
}

// fail records the terminal error, cancels ingestion, and triggers shutdown so
// WaitForShutdown surfaces it to the process supervisor.
func (c *connector) fail(err error) {
	if err == nil {
		return
	}
	c.initLifecycle()
	c.lifecycleMu.Lock()
	if c.fatalErr != nil || c.closing {
		c.lifecycleMu.Unlock()
		return
	}
	c.fatalErr = err
	if c.cancel != nil {
		c.cancel()
	}
	c.lifecycleMu.Unlock()

	slog.Default().Error("terminal replication failure; stopping without acknowledging pending work", "error", err)
	go c.Close()
}

func (c *connector) processingContext() context.Context {
	c.initLifecycle()
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	return c.runCtx
}

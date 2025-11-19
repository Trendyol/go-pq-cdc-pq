package signal

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// WaitForShutdownSignal waits for shutdown signals (SIGINT, SIGTERM) or context cancellation
// Returns true if a signal was received, false if context was cancelled
func WaitForShutdownSignal(ctx context.Context) bool {
	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		slog.Info("Received shutdown signal", "signal", sig)
		return true
	case <-ctx.Done():
		slog.Info("Context cancelled, shutting down")
		return false
	}
}

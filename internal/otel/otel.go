package otel

import (
	"context"
	observability "gitlab.trendyol.com/devx/observability/sdk/opentelemetry-go"
	"os"
)

func Initialize() func(context.Context) error {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName != "" {
		return observability.Initialize()
	}
	return func(ctx context.Context) error { return nil }
}

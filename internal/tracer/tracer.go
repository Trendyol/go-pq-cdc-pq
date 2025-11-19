package tracer

import (
	"context"
	"log/slog"
	"os"

	slogctx "github.com/veqryn/slog-context"
	slogotel "github.com/veqryn/slog-context/otel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	// Service information
	serviceName    = "go-pq-cdc-pq"
	serviceVersion = "1.0.0"
)

var (
	tracer        trace.Tracer
	traceProvider *sdktrace.TracerProvider
)

// Initialize sets up the slog-context handler and OpenTelemetry tracer
func Initialize() context.Context {
	// Setup OTEL first
	setupOTEL()

	// Create the *slogctx.Handler middleware
	h := slogctx.NewHandler(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}), // The next handler in the chain
		&slogctx.HandlerOptions{
			// Prependers will first add the OTEL Trace ID,
			// then anything else Prepended to the ctx
			Prependers: []slogctx.AttrExtractor{
				slogotel.ExtractTraceSpanID,
				slogctx.ExtractPrepended,
			},
			// Appenders stays as default (leaving as nil would accomplish the same)
			Appenders: []slogctx.AttrExtractor{
				slogctx.ExtractAppended,
			},
		},
	)

	// Set the default logger
	slog.SetDefault(slog.New(h))

	// Create context with logger
	ctx := slogctx.NewCtx(context.Background(), slog.New(h))

	// Start a root span for the entire application
	ctx, _ = tracer.Start(ctx, "application.startup",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("service.name", serviceName),
			attribute.String("service.version", serviceVersion),
		))

	return ctx
}

// Shutdown properly shuts down the tracer provider
func Shutdown(ctx context.Context) error {
	if traceProvider != nil {
		return traceProvider.Shutdown(ctx)
	}
	return nil
}

// GetTracer returns the configured tracer instance
func GetTracer() trace.Tracer {
	return tracer
}

// StartSpan starts a new span with the configured tracer
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, opts...)
}

// StartSpan starts a new span and returns context with the span
func StartSpanWithAttrs(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	spanOpts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindInternal),
	}
	return tracer.Start(ctx, name, spanOpts...)
}

// setupOTEL initializes OpenTelemetry
func setupOTEL() {
	exp, err := stdouttrace.New()
	if err != nil {
		panic(err)
	}

	// Create a new tracer provider with a batch span processor and the given exporter.
	traceProvider = newTraceProvider(exp)

	// Set as global trace provider
	otel.SetTracerProvider(traceProvider)

	// Finally, set the tracer that can be used for this package.
	tracer = traceProvider.Tracer(serviceName)
}

// newTraceProvider creates a new trace provider with proper resource configuration
func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			attribute.String("service.name", serviceName),
			attribute.String("service.version", serviceVersion),
		),
	)
	if err != nil {
		panic(err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}

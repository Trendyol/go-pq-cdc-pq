package gopqcdcpq

import "context"

// Handler reacts to CDC messages forwarded by the connector.
type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx context.Context, msg Message) error

// Handle delegates the call to the wrapped function.
func (f HandlerFunc) Handle(ctx context.Context, msg Message) error {
	return f(ctx, msg)
}

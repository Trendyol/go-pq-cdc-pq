package gopqcdcpq

// Mapper transforms CDC events into SQL query actions.
// It receives a Message containing the CDC event data and returns
// a slice of QueryAction to be executed on the target database.
// Returning nil or empty slice will skip the event (ack will still be called).
type Mapper func(event *Message) []QueryAction

// QueryAction represents a SQL query to be executed on the target database.
type QueryAction struct {
	Query string
	Args  []any
}

// DefaultMapper performs 1:1 replication using the same schema.
// It simply forwards the pre-built query from the Message.
func DefaultMapper(event *Message) []QueryAction {
	if event.Query == "" {
		return nil
	}
	return []QueryAction{{
		Query: event.Query,
		Args:  event.Args,
	}}
}

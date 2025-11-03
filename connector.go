package gopqcdcpq

// Connector represents a CDC connector
type Connector interface {
	Connect() error
	Disconnect() error
	Start() error
	Stop() error
}

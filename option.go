package gopqcdcpq

// Option represents a configuration option
type Option func(*Config)

// Config holds the configuration
type Config struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// WithHost sets the host option
func WithHost(host string) Option {
	return func(c *Config) {
		c.Host = host
	}
}

// WithPort sets the port option
func WithPort(port int) Option {
	return func(c *Config) {
		c.Port = port
	}
}

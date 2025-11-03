package gopqcdcpq

// Message represents a CDC message
type Message struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

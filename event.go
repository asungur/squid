package squid

import (
	"time"

	"github.com/oklog/ulid/v2"
)

// Event represents a single log event stored in the database.
type Event struct {
	// ID is the unique identifier for this event (auto-generated on append).
	ID ulid.ULID `json:"id"`

	// Timestamp is when the event occurred.
	Timestamp time.Time `json:"timestamp"`

	// Type categorizes the event (e.g., "request", "error", "metric").
	Type string `json:"type"`

	// Tags are key-value pairs for filtering (e.g., {"service": "api", "env": "prod"}).
	Tags map[string]string `json:"tags,omitempty"`

	// Data contains the event payload with arbitrary fields.
	Data map[string]any `json:"data,omitempty"`
}

// validate checks if the event has required fields.
func (e *Event) validate() error {
	if e.Type == "" {
		return ErrEmptyType
	}
	return nil
}

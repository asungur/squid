package squid

import "errors"

var (
	// ErrClosed is returned when operating on a closed database.
	ErrClosed = errors.New("squid: database is closed")

	// ErrNotFound is returned when an event is not found.
	ErrNotFound = errors.New("squid: event not found")

	// ErrEmptyType is returned when an event has an empty type.
	ErrEmptyType = errors.New("squid: event type cannot be empty")

	// ErrInvalidQuery is returned when a query has invalid parameters.
	ErrInvalidQuery = errors.New("squid: invalid query parameters")

	// ErrTooManyValues is returned when aggregating percentiles over too many values.
	ErrTooManyValues = errors.New("squid: too many values for percentile calculation")
)

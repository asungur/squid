package squid

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// ulidSource provides monotonic ULID generation.
// It ensures that ULIDs generated within the same millisecond are ordered.
type ulidSource struct {
	mu      sync.Mutex
	entropy *ulid.MonotonicEntropy
}

// newULIDSource creates a new monotonic ULID source.
func newULIDSource() *ulidSource {
	return &ulidSource{
		entropy: ulid.Monotonic(rand.Reader, 0),
	}
}

// New generates a new ULID with the given timestamp.
func (s *ulidSource) New(t time.Time) ulid.ULID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(t), s.entropy)
}

// Now generates a new ULID with the current timestamp.
func (s *ulidSource) Now() ulid.ULID {
	return s.New(time.Now())
}

// timeToULIDPrefix converts a time to the ULID prefix for range scanning.
// The first 10 characters of a ULID encode the timestamp.
func timeToULIDPrefix(t time.Time) string {
	id := ulid.MustNew(ulid.Timestamp(t), nil)
	return id.String()[:10]
}

// ulidTime extracts the timestamp from a ULID.
func ulidTime(id ulid.ULID) time.Time {
	return ulid.Time(id.Time())
}

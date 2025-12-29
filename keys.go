package squid

import (
	"github.com/oklog/ulid/v2"
)

// Key prefixes for different record types in BadgerDB.
const (
	prefixEvent = "e:" // Primary event storage
	prefixTag   = "t:" // Tag index: t:<key>=<value>:<ulid>
	prefixType  = "y:" // Type index: y:<type>:<ulid>
	eventKeyLen = len(prefixEvent) + 26
)

// encodeEventKey creates a primary event key from a ULID.
// Format: e:<ulid>
func encodeEventKey(id ulid.ULID) []byte {
	key := make([]byte, 0, eventKeyLen)
	key = append(key, prefixEvent...)
	key = append(key, id.String()...)
	return key
}

// decodeEventKey extracts the ULID from a primary event key.
func decodeEventKey(key []byte) (ulid.ULID, error) {
	if len(key) < eventKeyLen {
		return ulid.ULID{}, ErrNotFound
	}
	return ulid.ParseStrict(string(key[len(prefixEvent):]))
}

// encodeTagIndexKey creates a tag index key.
// Format: t:<key>=<value>:<ulid>
func encodeTagIndexKey(tagKey, tagValue string, id ulid.ULID) []byte {
	// t: + key + = + value + : + ulid(26)
	key := make([]byte, 0, len(prefixTag)+len(tagKey)+1+len(tagValue)+1+26)
	key = append(key, prefixTag...)
	key = append(key, tagKey...)
	key = append(key, '=')
	key = append(key, tagValue...)
	key = append(key, ':')
	key = append(key, id.String()...)
	return key
}

// encodeTagIndexPrefix creates a prefix for scanning all events with a specific tag.
// Format: t:<key>=<value>:
func encodeTagIndexPrefix(tagKey, tagValue string) []byte {
	prefix := make([]byte, 0, len(prefixTag)+len(tagKey)+1+len(tagValue)+1)
	prefix = append(prefix, prefixTag...)
	prefix = append(prefix, tagKey...)
	prefix = append(prefix, '=')
	prefix = append(prefix, tagValue...)
	prefix = append(prefix, ':')
	return prefix
}

// decodeTagIndexKey extracts the ULID from a tag index key.
func decodeTagIndexKey(key []byte) (ulid.ULID, error) {
	// ULID is always the last 26 characters
	if len(key) < 26 {
		return ulid.ULID{}, ErrNotFound
	}
	return ulid.ParseStrict(string(key[len(key)-26:]))
}

// encodeTypeIndexKey creates a type index key.
// Format: y:<type>:<ulid>
func encodeTypeIndexKey(eventType string, id ulid.ULID) []byte {
	key := make([]byte, 0, len(prefixType)+len(eventType)+1+26)
	key = append(key, prefixType...)
	key = append(key, eventType...)
	key = append(key, ':')
	key = append(key, id.String()...)
	return key
}

// encodeTypeIndexPrefix creates a prefix for scanning all events of a specific type.
// Format: y:<type>:
func encodeTypeIndexPrefix(eventType string) []byte {
	prefix := make([]byte, 0, len(prefixType)+len(eventType)+1)
	prefix = append(prefix, prefixType...)
	prefix = append(prefix, eventType...)
	prefix = append(prefix, ':')
	return prefix
}

// decodeTypeIndexKey extracts the ULID from a type index key.
func decodeTypeIndexKey(key []byte) (ulid.ULID, error) {
	// ULID is always the last 26 characters
	if len(key) < 26 {
		return ulid.ULID{}, ErrNotFound
	}
	return ulid.ParseStrict(string(key[len(key)-26:]))
}

// eventKeyPrefix returns the prefix for all event keys.
func eventKeyPrefix() []byte {
	return []byte(prefixEvent)
}

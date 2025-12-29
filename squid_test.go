package squid

import (
	"os"
	"testing"
	"time"
)

func TestOpenClose(t *testing.T) {
	dir, err := os.MkdirTemp("", "squid-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Double close should return error
	if err := db.Close(); err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestAppend(t *testing.T) {
	dir, err := os.MkdirTemp("", "squid-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	event, err := db.Append(Event{
		Type: "test.event",
		Tags: map[string]string{
			"service": "api",
			"env":     "test",
		},
		Data: map[string]any{
			"value":   123,
			"message": "hello",
		},
	})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify ID was generated
	if event.ID.String() == "" {
		t.Error("expected ID to be set")
	}

	// Verify timestamp was set
	if event.Timestamp.IsZero() {
		t.Error("expected Timestamp to be set")
	}

	// Verify we can retrieve the event
	retrieved, err := db.Get(event.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Type != event.Type {
		t.Errorf("Type mismatch: got %s, want %s", retrieved.Type, event.Type)
	}
	if retrieved.Tags["service"] != "api" {
		t.Errorf("Tag mismatch: got %s, want api", retrieved.Tags["service"])
	}
}

func TestAppendWithTimestamp(t *testing.T) {
	dir, err := os.MkdirTemp("", "squid-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	customTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	event, err := db.Append(Event{
		Timestamp: customTime,
		Type:      "test.event",
	})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if !event.Timestamp.Equal(customTime) {
		t.Errorf("Timestamp mismatch: got %v, want %v", event.Timestamp, customTime)
	}
}

func TestAppendEmptyType(t *testing.T) {
	dir, err := os.MkdirTemp("", "squid-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	_, err = db.Append(Event{})
	if err != ErrEmptyType {
		t.Errorf("expected ErrEmptyType, got %v", err)
	}
}

func TestAppendBatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "squid-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	events := []Event{
		{Type: "event.1", Tags: map[string]string{"idx": "1"}},
		{Type: "event.2", Tags: map[string]string{"idx": "2"}},
		{Type: "event.3", Tags: map[string]string{"idx": "3"}},
	}

	results, err := db.AppendBatch(events)
	if err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Verify all events have unique IDs
	ids := make(map[string]bool)
	for _, e := range results {
		if ids[e.ID.String()] {
			t.Error("duplicate ID found")
		}
		ids[e.ID.String()] = true
	}

	// Verify we can retrieve each event
	for _, e := range results {
		retrieved, err := db.Get(e.ID)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", e.ID, err)
		}
		if retrieved.Type != e.Type {
			t.Errorf("Type mismatch: got %s, want %s", retrieved.Type, e.Type)
		}
	}
}

func TestGetNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "squid-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Try to get a non-existent event
	_, err = db.Get(db.ulids.Now())
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestKeyEncoding(t *testing.T) {
	source := newULIDSource()
	id := source.Now()

	// Test event key roundtrip
	eventKey := encodeEventKey(id)
	decoded, err := decodeEventKey(eventKey)
	if err != nil {
		t.Fatalf("decodeEventKey failed: %v", err)
	}
	if decoded != id {
		t.Errorf("event key roundtrip failed: got %s, want %s", decoded, id)
	}

	// Test tag index key
	tagKey := encodeTagIndexKey("service", "api", id)
	decodedTag, err := decodeIndexKey(tagKey)
	if err != nil {
		t.Fatalf("decodeIndexKey (tag) failed: %v", err)
	}
	if decodedTag != id {
		t.Errorf("tag key roundtrip failed: got %s, want %s", decodedTag, id)
	}

	// Test type index key
	typeKey := encodeTypeIndexKey("request", id)
	decodedType, err := decodeIndexKey(typeKey)
	if err != nil {
		t.Fatalf("decodeIndexKey (type) failed: %v", err)
	}
	if decodedType != id {
		t.Errorf("type key roundtrip failed: got %s, want %s", decodedType, id)
	}
}

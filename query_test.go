package squid

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestQueryAll(t *testing.T) {
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

	// Insert test events
	for i := 0; i < 5; i++ {
		_, err := db.Append(Event{
			Type: "test.event",
			Data: map[string]any{"index": i},
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Query all events
	ctx := context.Background()
	events, err := db.Query(ctx, Query{})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) != 5 {
		t.Errorf("expected 5 events, got %d", len(events))
	}
}

func TestQueryByType(t *testing.T) {
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

	// Insert events with different types. Test covers the error scenario.
	_, _ = db.Append(Event{Type: "request"})
	_, _ = db.Append(Event{Type: "request"})
	_, _ = db.Append(Event{Type: "error"})
	_, _ = db.Append(Event{Type: "metric"})

	// Query by type
	ctx := context.Background()
	events, err := db.Query(ctx, Query{Types: []string{"request"}})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}

	for _, e := range events {
		if e.Type != "request" {
			t.Errorf("expected type 'request', got %s", e.Type)
		}
	}
}

func TestQueryByTags(t *testing.T) {
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

	// Insert events with different tags
	db.Append(Event{Type: "request", Tags: map[string]string{"service": "api", "env": "prod"}})
	db.Append(Event{Type: "request", Tags: map[string]string{"service": "api", "env": "dev"}})
	db.Append(Event{Type: "request", Tags: map[string]string{"service": "web", "env": "prod"}})

	// Query by single tag
	ctx := context.Background()
	events, err := db.Query(ctx, Query{Tags: map[string]string{"service": "api"}})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}

	// Query by multiple tags
	events, err = db.Query(ctx, Query{Tags: map[string]string{"service": "api", "env": "prod"}})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

func TestQueryByTimeRange(t *testing.T) {
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

	// Insert events with specific timestamps
	t1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)
	t3 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	db.Append(Event{Timestamp: t1, Type: "event"})
	db.Append(Event{Timestamp: t2, Type: "event"})
	db.Append(Event{Timestamp: t3, Type: "event"})

	// Query with start time
	ctx := context.Background()
	start := time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)
	events, err := db.Query(ctx, Query{Start: &start})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}

	// Query with end time
	end := time.Date(2024, 1, 1, 11, 30, 0, 0, time.UTC)
	events, err = db.Query(ctx, Query{End: &end})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}

	// Query with both start and end
	events, err = db.Query(ctx, Query{Start: &start, End: &end})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

func TestQueryLimit(t *testing.T) {
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

	// Insert 10 events
	for i := 0; i < 10; i++ {
		db.Append(Event{Type: "event"})
	}

	// Query with limit
	ctx := context.Background()
	events, err := db.Query(ctx, Query{Limit: 3})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}
}

func TestQueryDescending(t *testing.T) {
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

	// Insert events with specific timestamps
	t1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)
	t3 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	db.Append(Event{Timestamp: t1, Type: "event", Data: map[string]any{"order": 1}})
	db.Append(Event{Timestamp: t2, Type: "event", Data: map[string]any{"order": 2}})
	db.Append(Event{Timestamp: t3, Type: "event", Data: map[string]any{"order": 3}})

	// Query ascending (default)
	ctx := context.Background()
	events, err := db.Query(ctx, Query{})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if events[0].Data["order"].(float64) != 1 {
		t.Errorf("expected first event order=1, got %v", events[0].Data["order"])
	}

	// Query descending
	events, err = db.Query(ctx, Query{Descending: true})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if events[0].Data["order"].(float64) != 3 {
		t.Errorf("expected first event order=3, got %v", events[0].Data["order"])
	}
}

func TestCount(t *testing.T) {
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

	// Empty database
	count, err := db.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	// Insert events
	for i := 0; i < 5; i++ {
		db.Append(Event{Type: "event"})
	}

	count, err = db.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5, got %d", count)
	}
}

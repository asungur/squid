package squid

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestDeleteBefore(t *testing.T) {
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

	_, _ = db.Append(Event{Timestamp: t1, Type: "event", Tags: map[string]string{"env": "prod"}})
	_, _ = db.Append(Event{Timestamp: t2, Type: "event", Tags: map[string]string{"env": "prod"}})
	_, _ = db.Append(Event{Timestamp: t3, Type: "event", Tags: map[string]string{"env": "prod"}})

	// Verify 3 events exist
	count, err := db.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 events, got %d", count)
	}

	// Delete events before 11:30
	cutoff := time.Date(2024, 1, 1, 11, 30, 0, 0, time.UTC)
	deleted, err := db.DeleteBefore(cutoff)
	if err != nil {
		t.Fatalf("DeleteBefore failed: %v", err)
	}

	if deleted != 2 {
		t.Errorf("expected 2 deleted, got %d", deleted)
	}

	// Verify only 1 event remains
	count, err = db.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 event, got %d", count)
	}

	// Verify the remaining event is the latest one
	ctx := context.Background()
	events, err := db.Query(ctx, Query{})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if !events[0].Timestamp.Equal(t3) {
		t.Errorf("expected timestamp %v, got %v", t3, events[0].Timestamp)
	}
}

func TestDeleteBeforeRemovesIndices(t *testing.T) {
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

	// Insert events with tags
	t1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	_, _ = db.Append(Event{Timestamp: t1, Type: "request", Tags: map[string]string{"service": "api"}})
	_, _ = db.Append(Event{Timestamp: t2, Type: "request", Tags: map[string]string{"service": "api"}})

	// Query by tag - should find 2
	ctx := context.Background()
	events, err := db.Query(ctx, Query{Tags: map[string]string{"service": "api"}})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}

	// Delete the older event
	cutoff := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)
	_, err = db.DeleteBefore(cutoff)
	if err != nil {
		t.Fatalf("DeleteBefore failed: %v", err)
	}

	// Query by tag - should find 1 (index was cleaned up)
	events, err = db.Query(ctx, Query{Tags: map[string]string{"service": "api"}})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 event after deletion, got %d", len(events))
	}

	// Query by type - should find 1
	events, err = db.Query(ctx, Query{Types: []string{"request"}})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

func TestSetRetentionStartsCleanup(t *testing.T) {
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

	// Insert an old event (simulated by using a past timestamp)
	oldTime := time.Now().Add(-2 * time.Hour)
	_, _ = db.Append(Event{Timestamp: oldTime, Type: "old-event"})

	// Insert a recent event
	_, _ = db.Append(Event{Type: "recent-event"})

	// Verify 2 events exist
	count, err := db.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 events, got %d", count)
	}

	// Set retention to 1 hour with immediate cleanup
	db.SetRetention(RetentionPolicy{
		MaxAge:          time.Hour,
		CleanupInterval: 10 * time.Millisecond,
	})

	// Wait for cleanup to run
	time.Sleep(50 * time.Millisecond)

	// Verify only 1 event remains
	count, err = db.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 event after retention cleanup, got %d", count)
	}
}

func TestSetRetentionDisable(t *testing.T) {
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

	// Enable retention
	db.SetRetention(RetentionPolicy{
		MaxAge:          time.Hour,
		CleanupInterval: 100 * time.Millisecond,
	})

	// Verify retention is running
	db.mu.RLock()
	running := db.retention != nil && db.retention.running
	db.mu.RUnlock()
	if !running {
		t.Error("expected retention to be running")
	}

	// Disable retention
	db.SetRetention(RetentionPolicy{MaxAge: 0})

	// Verify retention is stopped
	db.mu.RLock()
	retention := db.retention
	db.mu.RUnlock()
	if retention != nil {
		t.Error("expected retention to be nil after disabling")
	}
}

func TestSetRetentionRestart(t *testing.T) {
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

	// Enable retention with 2 hour max age
	db.SetRetention(RetentionPolicy{
		MaxAge:          2 * time.Hour,
		CleanupInterval: 100 * time.Millisecond,
	})

	// Insert an event 90 minutes old (should be kept with 2 hour retention)
	oldTime := time.Now().Add(-90 * time.Minute)
	_, _ = db.Append(Event{Timestamp: oldTime, Type: "event"})

	// Wait for cleanup
	time.Sleep(50 * time.Millisecond)

	// Event should still exist
	count, err := db.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 event with 2h retention, got %d", count)
	}

	// Change retention to 1 hour (event should now be deleted)
	db.SetRetention(RetentionPolicy{
		MaxAge:          time.Hour,
		CleanupInterval: 10 * time.Millisecond,
	})

	// Wait for cleanup
	time.Sleep(50 * time.Millisecond)

	// Event should be deleted
	count, err = db.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 events after retention change, got %d", count)
	}
}

func TestCloseStopsRetention(t *testing.T) {
	dir, err := os.MkdirTemp("", "squid-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Enable retention
	db.SetRetention(RetentionPolicy{
		MaxAge:          time.Hour,
		CleanupInterval: 100 * time.Millisecond,
	})

	// Close should stop retention gracefully
	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// This test passes if Close() doesn't hang
}

func TestRetentionDefaultInterval(t *testing.T) {
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

	// Set retention without specifying CleanupInterval
	db.SetRetention(RetentionPolicy{
		MaxAge: 10 * time.Hour,
	})

	// Default should be MaxAge/10 = 1 hour
	db.mu.RLock()
	interval := db.retention.policy.CleanupInterval
	db.mu.RUnlock()

	expected := time.Hour
	if interval != expected {
		t.Errorf("expected default interval %v, got %v", expected, interval)
	}
}

func TestRetentionMinimumInterval(t *testing.T) {
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

	// Set retention with very short MaxAge
	db.SetRetention(RetentionPolicy{
		MaxAge: 5 * time.Minute,
	})

	// Default should be minimum of 1 minute (not MaxAge/10 = 30s)
	db.mu.RLock()
	interval := db.retention.policy.CleanupInterval
	db.mu.RUnlock()

	expected := time.Minute
	if interval != expected {
		t.Errorf("expected minimum interval %v, got %v", expected, interval)
	}
}

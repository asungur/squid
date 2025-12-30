package squid

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/oklog/ulid/v2"
)

// RetentionPolicy defines how long events are kept before automatic deletion.
type RetentionPolicy struct {
	// MaxAge is the maximum age of events. Events older than this will be deleted.
	MaxAge time.Duration

	// CleanupInterval is how often the cleanup goroutine runs.
	// Defaults to MaxAge/10 if not set (minimum 1 minute).
	CleanupInterval time.Duration
}

// retentionState holds the state for the retention cleanup goroutine.
type retentionState struct {
	policy  RetentionPolicy
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	running bool
}

// isRunning safely checks if the retention goroutine is running.
func (s *retentionState) isRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// SetRetention configures the retention policy and starts background cleanup.
// Calling this multiple times will update the policy and restart the cleanup goroutine.
// Pass a zero MaxAge to disable retention (stop cleanup).
func (db *DB) SetRetention(policy RetentionPolicy) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Stop existing retention goroutine if running
	if db.retention != nil && db.retention.isRunning() {
		db.retention.cancel()
		<-db.retention.done
	}

	// Disable retention if MaxAge is zero
	if policy.MaxAge == 0 {
		db.retention = nil
		return
	}

	// Set default cleanup interval
	if policy.CleanupInterval == 0 {
		policy.CleanupInterval = policy.MaxAge / 10
		if policy.CleanupInterval < time.Minute {
			policy.CleanupInterval = time.Minute
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	state := &retentionState{
		policy:  policy,
		cancel:  cancel,
		done:    make(chan struct{}),
		running: true,
	}
	db.retention = state

	go db.runRetentionCleanup(ctx, state)
}

// runRetentionCleanup periodically deletes expired events.
func (db *DB) runRetentionCleanup(ctx context.Context, state *retentionState) {
	defer close(state.done)
	defer func() {
		state.mu.Lock()
		state.running = false
		state.mu.Unlock()
	}()

	ticker := time.NewTicker(state.policy.CleanupInterval)
	defer ticker.Stop()

	// Run cleanup immediately on start
	cutoff := time.Now().Add(-state.policy.MaxAge)
	db.deleteBefore(cutoff)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-state.policy.MaxAge)
			db.deleteBefore(cutoff)
		}
	}
}

// DeleteBefore manually deletes all events before the given time.
// This can be used for manual cleanup or testing.
func (db *DB) DeleteBefore(before time.Time) (int64, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return 0, ErrClosed
	}
	db.mu.RUnlock()

	return db.deleteBefore(before)
}

// deleteBefore is the internal implementation that deletes events before a cutoff time.
func (db *DB) deleteBefore(before time.Time) (int64, error) {
	var deleted int64

	err := db.badger.Update(func(txn *badger.Txn) error {
		toDelete, err := db.findExpiredEvents(txn, before)
		if err != nil {
			return err
		}

		for _, entry := range toDelete {
			if err := db.deleteEventAndIndices(txn, entry); err != nil {
				continue
			}
			deleted++
		}

		return nil
	})

	return deleted, err
}

// deleteEntry holds information needed to delete an event and its indices.
type deleteEntry struct {
	id    ulid.ULID
	event Event
}

// findExpiredEvents scans for events before the cutoff time.
func (db *DB) findExpiredEvents(txn *badger.Txn, before time.Time) ([]deleteEntry, error) {
	var toDelete []deleteEntry

	opts := badger.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	defer it.Close()

	prefix := eventKeyPrefix()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		id, err := decodeEventKey(key)
		if err != nil {
			continue
		}

		eventTime := ulidTime(id)
		if eventTime.Before(before) {
			var event Event
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &event)
			})
			if err != nil {
				continue
			}

			toDelete = append(toDelete, deleteEntry{
				id:    id,
				event: event,
			})
		} else {
			// Events are sorted by time, so we can stop early
			break
		}
	}

	return toDelete, nil
}

// deleteEventAndIndices removes an event and all its associated indices.
func (db *DB) deleteEventAndIndices(txn *badger.Txn, entry deleteEntry) error {
	// Delete primary event
	if err := txn.Delete(encodeEventKey(entry.id)); err != nil {
		return err
	}

	// Delete type index
	if err := txn.Delete(encodeTypeIndexKey(entry.event.Type, entry.id)); err != nil {
		return err
	}

	// Delete tag indices
	for k, v := range entry.event.Tags {
		if err := txn.Delete(encodeTagIndexKey(k, v, entry.id)); err != nil {
			return err
		}
	}

	return nil
}

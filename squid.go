package squid

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/oklog/ulid/v2"
)

// DB is the main database handle for Squid.
type DB struct {
	badger    *badger.DB
	ulids     *ulidSource
	retention *retentionState
	closed    bool
	mu        sync.RWMutex
}

// Open creates or opens a Squid database at the given path.
func Open(path string) (*DB, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // Disable BadgerDB's default logging

	bdb, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &DB{
		badger: bdb,
		ulids:  newULIDSource(),
	}, nil
}

// Close closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	// Stop retention goroutine if running
	if db.retention != nil && db.retention.isRunning() {
		db.retention.cancel()
		<-db.retention.done
	}

	db.closed = true

	return db.badger.Close()
}

// Append adds a new event to the database.
// The event's ID and Timestamp are set automatically if not provided.
func (db *DB) Append(event Event) (*Event, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	db.mu.RUnlock()

	if err := event.validate(); err != nil {
		return nil, err
	}

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Generate ULID based on timestamp
	event.ID = db.ulids.New(event.Timestamp)

	// Serialize event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	// Write event and indices in a single transaction
	err = db.badger.Update(func(txn *badger.Txn) error {
		// Write primary event
		if err := txn.Set(encodeEventKey(event.ID), data); err != nil {
			return fmt.Errorf("failed to write event %s: %w", event.ID, err)
		}

		// Write type index
		if err := txn.Set(encodeTypeIndexKey(event.Type, event.ID), nil); err != nil {
			return fmt.Errorf("failed to write type index %s: %w", event.Type, err)
		}

		// Write tag indices
		for k, v := range event.Tags {
			if err := txn.Set(encodeTagIndexKey(k, v, event.ID), nil); err != nil {
				return fmt.Errorf("failed to write tag index key=%s: %w", k, err)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &event, nil
}

// AppendBatch adds multiple events to the database atomically.
func (db *DB) AppendBatch(events []Event) ([]*Event, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	db.mu.RUnlock()

	if len(events) == 0 {
		return nil, nil
	}

	results := make([]*Event, len(events))
	now := time.Now()

	// Validate all events first
	for i := range events {
		if err := events[i].validate(); err != nil {
			return nil, err
		}
	}

	err := db.badger.Update(func(txn *badger.Txn) error {
		for i := range events {
			event := &events[i]

			// Set timestamp if not provided
			if event.Timestamp.IsZero() {
				event.Timestamp = now
			}

			// Generate ULID
			event.ID = db.ulids.New(event.Timestamp)

			// Serialize
			data, err := json.Marshal(event)
			if err != nil {
				return err
			}

			// Write primary event
			if err := txn.Set(encodeEventKey(event.ID), data); err != nil {
				return fmt.Errorf("failed to write event %s: %w", event.ID, err)
			}

			// Write type index
			if err := txn.Set(encodeTypeIndexKey(event.Type, event.ID), nil); err != nil {
				return fmt.Errorf("failed to write type index %s: %w", event.Type, err)
			}

			// Write tag indices
			for k, v := range event.Tags {
				if err := txn.Set(encodeTagIndexKey(k, v, event.ID), nil); err != nil {
					return fmt.Errorf("failed to write tag index key=%s: %w", k, err)
				}
			}

			results[i] = event
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

// Get retrieves a single event by its ID.
func (db *DB) Get(id ulid.ULID) (*Event, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	db.mu.RUnlock()

	var event Event

	err := db.badger.View(func(txn *badger.Txn) error {
		item, err := txn.Get(encodeEventKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &event)
		})
	})

	if err != nil {
		return nil, err
	}

	return &event, nil
}

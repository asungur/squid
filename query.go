package squid

import (
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/oklog/ulid/v2"
)

// Query defines search criteria for events.
type Query struct {
	// Start is the inclusive start time (nil means no lower bound).
	Start *time.Time

	// End is the inclusive end time (nil means no upper bound).
	End *time.Time

	// Types filters by event type (empty means all types).
	Types []string

	// Tags filters by tag key-value pairs (all must match).
	Tags map[string]string

	// Limit is the maximum number of events to return (0 means no limit).
	// TODO(asungur): Add input validation and avoid large numbers.
	Limit int

	// Descending returns events in reverse chronological order.
	Descending bool
}

// Query finds events matching the given criteria.
func (db *DB) Query(q Query) ([]*Event, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	db.mu.RUnlock()

	var events []*Event

	err := db.badger.View(func(txn *badger.Txn) error {
		// Determine which scan strategy to use
		candidateIDs, useIndex := db.planQuery(txn, q)

		if useIndex {
			// Fetch events by ID from index scan results
			events = db.fetchEventsByIDs(txn, candidateIDs, q)
		} else {
			// Full scan on primary event keys
			events = db.fullScan(txn, q)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return events, nil
}

// planQuery decides whether to use an index and returns candidate IDs if so.
// TODO(asungur): Query planning prioritises type index.
// This could be improved by approximating selectivity of each index type,
// and choosing the more performant index.
func (db *DB) planQuery(txn *badger.Txn, q Query) ([]ulid.ULID, bool) {
	// If we have a single type filter, use the type index
	// TODO(asungur): If we have multiple type filters, we should use the union of the indices.
	if len(q.Types) == 1 {
		ids := db.scanTypeIndex(txn, q.Types[0], q)
		return ids, true
	}

	// If we have tag filters, use the first tag's index
	// (smallest result set heuristic would require counting, skip for MVP)
	for k, v := range q.Tags {
		ids := db.scanTagIndex(txn, k, v, q)
		return ids, true
	}

	// No suitable index, use full scan
	return nil, false
}

// scanTypeIndex scans the type index for matching event IDs.
func (db *DB) scanTypeIndex(txn *badger.Txn, eventType string, q Query) []ulid.ULID {
	prefix := encodeTypeIndexPrefix(eventType)
	return db.scanIndex(txn, prefix, q)
}

// scanTagIndex scans the tag index for matching event IDs.
func (db *DB) scanTagIndex(txn *badger.Txn, tagKey, tagValue string, q Query) []ulid.ULID {
	prefix := encodeTagIndexPrefix(tagKey, tagValue)
	return db.scanIndex(txn, prefix, q)
}

// scanIndex scans an index prefix and returns matching event IDs.
func (db *DB) scanIndex(txn *badger.Txn, prefix []byte, q Query) []ulid.ULID {
	var ids []ulid.ULID

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // Index keys have no values
	opts.Reverse = q.Descending

	it := txn.NewIterator(opts)
	defer it.Close()

	// Determine seek position
	seekKey := prefix
	if q.Descending {
		// Seek to end of prefix range
		seekKey = prefixEnd(prefix)
	}

	for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()

		id, err := decodeIndexKey(key)
		if err != nil {
			continue
		}

		// Apply time filter
		if !db.matchesTimeRange(id, q) {
			continue
		}

		ids = append(ids, id)

		if q.Limit > 0 && len(ids) >= q.Limit {
			break
		}
	}

	return ids
}

// fetchEventsByIDs retrieves events by their IDs and applies remaining filters.
func (db *DB) fetchEventsByIDs(txn *badger.Txn, ids []ulid.ULID, q Query) []*Event {
	var events []*Event

	for _, id := range ids {
		item, err := txn.Get(encodeEventKey(id))
		if err != nil {
			continue
		}

		var event Event
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &event)
		})
		if err != nil {
			continue
		}

		// Apply remaining filters
		if !db.matchesFilters(&event, q) {
			continue
		}

		events = append(events, &event)

		if q.Limit > 0 && len(events) >= q.Limit {
			break
		}
	}

	return events
}

// fullScan iterates over all events and applies filters.
func (db *DB) fullScan(txn *badger.Txn, q Query) []*Event {
	var events []*Event

	opts := badger.DefaultIteratorOptions
	opts.Reverse = q.Descending

	it := txn.NewIterator(opts)
	defer it.Close()

	prefix := eventKeyPrefix()
	seekKey := prefix
	if q.Descending {
		seekKey = prefixEnd(prefix)
	}

	for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		// Extract ULID from key for time filtering before deserializing
		id, err := decodeEventKey(key)
		if err != nil {
			continue
		}

		// Apply time filter early
		if !db.matchesTimeRange(id, q) {
			// For ascending order, if we're past the end time, stop
			if !q.Descending && q.End != nil && ulidTime(id).After(*q.End) {
				break
			}
			// For descending order, if we're before the start time, stop
			if q.Descending && q.Start != nil && ulidTime(id).Before(*q.Start) {
				break
			}
			continue
		}

		var event Event
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &event)
		})
		if err != nil {
			continue
		}

		// Apply remaining filters
		if !db.matchesFilters(&event, q) {
			continue
		}

		events = append(events, &event)

		if q.Limit > 0 && len(events) >= q.Limit {
			break
		}
	}

	return events
}

// matchesTimeRange checks if an event ID falls within the query time range.
func (db *DB) matchesTimeRange(id ulid.ULID, q Query) bool {
	t := ulidTime(id)

	if q.Start != nil && t.Before(*q.Start) {
		return false
	}
	if q.End != nil && t.After(*q.End) {
		return false
	}

	return true
}

// matchesFilters checks if an event matches all query filters.
func (db *DB) matchesFilters(event *Event, q Query) bool {
	// Check type filter
	if len(q.Types) > 0 {
		matched := false
		for _, t := range q.Types {
			if event.Type == t {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check tag filters (all must match)
	for k, v := range q.Tags {
		if event.Tags[k] != v {
			return false
		}
	}

	return true
}

// prefixEnd returns the key that is just past all keys with the given prefix.
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)

	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}

	// All bytes were 0xFF, return nil to scan to the end
	return nil
}

// Count returns the total number of events in the database.
func (db *DB) Count() (int64, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return 0, ErrClosed
	}
	db.mu.RUnlock()

	var count int64

	err := db.badger.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := eventKeyPrefix()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return count, nil
}

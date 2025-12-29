package squid

import (
	"context"
	"encoding/json"
	"math"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/oklog/ulid/v2"
)

// AggregationType defines the type of aggregation to perform.
type AggregationType int

const (
	// Count counts the number of events.
	Count AggregationType = iota
	// Sum adds up all values of a field.
	Sum
	// Avg calculates the arithmetic mean.
	Avg
	// Min finds the minimum value.
	Min
	// Max finds the maximum value.
	Max
	// P50 calculates the 50th percentile (median).
	P50
	// P95 calculates the 95th percentile.
	P95
	// P99 calculates the 99th percentile.
	P99
)

// maxPercentileValues is the maximum number of values to collect for percentile calculations.
// This prevents memory exhaustion on large datasets.
const maxPercentileValues = 1_000_000

// AggregateResult holds the results of an aggregation operation.
type AggregateResult struct {
	Count int64
	Sum   float64
	Avg   float64
	Min   float64
	Max   float64
	P50   float64
	P95   float64
	P99   float64
}

// aggregator accumulates values during aggregation.
type aggregator struct {
	field            string
	needsPercentiles bool
	count            int64
	sum              float64
	min              float64
	max              float64
	values           []float64
}

func newAggregator(field string, needsPercentiles bool) *aggregator {
	return &aggregator{
		field:            field,
		needsPercentiles: needsPercentiles,
		min:              math.MaxFloat64,
		max:              -math.MaxFloat64,
	}
}

// add processes an event and updates the aggregation state.
// Returns an error if too many values are collected for percentile calculation.
func (a *aggregator) add(event *Event) error {
	val, ok := extractNumericValue(event, a.field)
	if !ok && a.field != "" {
		return nil // Skip events without the field
	}

	a.count++
	if a.field != "" {
		a.sum += val
		if val < a.min {
			a.min = val
		}
		if val > a.max {
			a.max = val
		}
		if a.needsPercentiles {
			if len(a.values) >= maxPercentileValues {
				return ErrTooManyValues
			}
			a.values = append(a.values, val)
		}
	}
	return nil
}

// result builds the final AggregateResult.
func (a *aggregator) result() *AggregateResult {
	result := &AggregateResult{
		Count: a.count,
	}

	if a.count > 0 && a.field != "" {
		result.Sum = a.sum
		result.Avg = a.sum / float64(a.count)
		result.Min = a.min
		result.Max = a.max

		if a.needsPercentiles && len(a.values) > 0 {
			sort.Float64s(a.values)
			result.P50 = percentile(a.values, 0.50)
			result.P95 = percentile(a.values, 0.95)
			result.P99 = percentile(a.values, 0.99)
		}
	}

	return result
}

// Aggregate computes aggregations over events matching the query.
// The field parameter specifies which field in Event.Data to aggregate.
// For Count aggregation, field can be empty.
func (db *DB) Aggregate(ctx context.Context, q Query, field string, aggs []AggregationType) (*AggregateResult, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	db.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Check if we need percentiles
	needsPercentiles := false
	for _, agg := range aggs {
		if agg == P50 || agg == P95 || agg == P99 {
			needsPercentiles = true
			break
		}
	}

	agg := newAggregator(field, needsPercentiles)

	err := db.badger.View(func(txn *badger.Txn) error {
		candidateIDs, useIndex := db.planQuery(ctx, txn, q)

		if useIndex {
			return db.aggregateByIDs(ctx, txn, candidateIDs, q, agg)
		}
		return db.aggregateFullScan(ctx, txn, q, agg)
	})

	if err != nil {
		return nil, err
	}

	return agg.result(), nil
}

// aggregateByIDs aggregates events by fetching them from candidate IDs.
func (db *DB) aggregateByIDs(ctx context.Context, txn *badger.Txn, ids []ulid.ULID, q Query, agg *aggregator) error {
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}

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

		if !db.matchesFilters(&event, q) {
			continue
		}

		if err := agg.add(&event); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// aggregateFullScan aggregates events by scanning all events.
func (db *DB) aggregateFullScan(ctx context.Context, txn *badger.Txn, q Query, agg *aggregator) error {
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
		if ctx.Err() != nil {
			return ctx.Err()
		}

		item := it.Item()
		key := item.Key()

		id, err := decodeEventKey(key)
		if err != nil {
			continue
		}

		if !db.matchesTimeRange(id, q) {
			if !q.Descending && q.End != nil && ulidTime(id).After(*q.End) {
				break
			}
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

		if !db.matchesFilters(&event, q) {
			continue
		}

		if err := agg.add(&event); err != nil {
			return err
		}
	}

	return ctx.Err()
}

// extractNumericValue extracts a numeric value from an event's Data field.
func extractNumericValue(event *Event, field string) (float64, bool) {
	if field == "" {
		return 0, true // Count-only mode
	}

	val, ok := event.Data[field]
	if !ok {
		return 0, false
	}

	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case int16:
		return float64(v), true
	case int8:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint8:
		return float64(v), true
	default:
		return 0, false
	}
}

// percentile calculates the p-th percentile of a sorted slice.
// Uses linear interpolation between closest ranks.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}

	// Calculate rank
	rank := p * float64(len(sorted)-1)
	lower := int(rank)
	upper := lower + 1

	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}

	// Linear interpolation
	weight := rank - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

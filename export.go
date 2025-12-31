package squid

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"
)

// ExportFormat defines the output format for exported events.
type ExportFormat int

const (
	// JSON exports events as JSON array.
	JSON ExportFormat = iota
	// CSV exports events as CSV with flattened tags and data.
	CSV
)

// Export writes events matching the query to the given writer in the specified format.
// The context can be used to cancel long-running exports.
func (db *DB) Export(ctx context.Context, w io.Writer, q Query, format ExportFormat) error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	db.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	events, err := db.Query(ctx, q)
	if err != nil {
		return err
	}

	switch format {
	case JSON:
		return exportJSON(ctx, w, events)
	case CSV:
		return exportCSV(ctx, w, events)
	default:
		return exportJSON(ctx, w, events)
	}
}

// exportJSON writes events as a JSON array.
// For JSON, we write all events at once, so we just check context before starting.
func exportJSON(ctx context.Context, w io.Writer, events []*Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(events)
}

// exportCSV writes events as CSV with flattened tags and data fields.
// Column order: id, timestamp, type, tag_*, data_*
// Checks context cancellation periodically during row writing.
func exportCSV(ctx context.Context, w io.Writer, events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// Collect all unique tag and data keys
	tagKeys := collectKeys(events, func(e *Event) map[string]string { return e.Tags })
	dataKeys := collectDataKeys(events)

	// Build header
	header := buildCSVHeader(tagKeys, dataKeys)

	writer := csv.NewWriter(w)
	defer writer.Flush()

	if err := writer.Write(header); err != nil {
		return err
	}

	// Write rows with periodic context checks
	for i, event := range events {
		// Check context every 1000 rows to avoid overhead on small exports
		if i%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		row := buildCSVRow(event, tagKeys, dataKeys)
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return writer.Error()
}

// collectKeys collects all unique keys from a map field across events.
func collectKeys(events []*Event, getter func(*Event) map[string]string) []string {
	seen := make(map[string]struct{})
	for _, e := range events {
		for k := range getter(e) {
			seen[k] = struct{}{}
		}
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// collectDataKeys collects all unique keys from Data fields across events.
func collectDataKeys(events []*Event) []string {
	seen := make(map[string]struct{})
	for _, e := range events {
		for k := range e.Data {
			seen[k] = struct{}{}
		}
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// buildCSVHeader creates the CSV header row.
func buildCSVHeader(tagKeys, dataKeys []string) []string {
	header := []string{"id", "timestamp", "type"}

	for _, k := range tagKeys {
		header = append(header, "tag_"+k)
	}
	for _, k := range dataKeys {
		header = append(header, "data_"+k)
	}

	return header
}

// buildCSVRow creates a CSV row for an event.
func buildCSVRow(event *Event, tagKeys, dataKeys []string) []string {
	row := []string{
		event.ID.String(),
		event.Timestamp.Format(time.RFC3339Nano),
		event.Type,
	}

	// Add tag values
	for _, k := range tagKeys {
		row = append(row, event.Tags[k])
	}

	// Add data values
	for _, k := range dataKeys {
		row = append(row, formatDataValue(event.Data[k]))
	}

	return row
}

// formatDataValue converts a data value to a string for CSV export.
func formatDataValue(v any) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
		return val
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", val)
	default:
		// Use JSON encoding for arrays, objects, etc.
		b, err := json.Marshal(val)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

package squid

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"sort"
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
		return exportJSON(w, events)
	case CSV:
		return exportCSV(w, events)
	default:
		return exportJSON(w, events)
	}
}

// exportJSON writes events as a JSON array.
func exportJSON(w io.Writer, events []*Event) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(events)
}

// exportCSV writes events as CSV with flattened tags and data fields.
// Column order: id, timestamp, type, tag_*, data_*
func exportCSV(w io.Writer, events []*Event) error {
	if len(events) == 0 {
		return nil
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

	// Write rows
	for _, event := range events {
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
		event.Timestamp.Format("2006-01-02T15:04:05.000Z07:00"),
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
	default:
		// Use JSON encoding for numbers, arrays, objects, etc.
		b, err := json.Marshal(val)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

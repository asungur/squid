package squid

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestExportJSON(t *testing.T) {
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
	ts := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	_, _ = db.Append(Event{
		Timestamp: ts,
		Type:      "request",
		Tags:      map[string]string{"service": "api"},
		Data:      map[string]any{"status": float64(200)},
	})

	var buf bytes.Buffer
	ctx := context.Background()
	err = db.Export(ctx, &buf, Query{}, JSON)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	// Parse JSON output
	var events []Event
	if err := json.Unmarshal(buf.Bytes(), &events); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}

	if events[0].Type != "request" {
		t.Errorf("expected type 'request', got %s", events[0].Type)
	}

	if events[0].Tags["service"] != "api" {
		t.Errorf("expected tag service=api, got %s", events[0].Tags["service"])
	}
}

func TestExportCSV(t *testing.T) {
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
	ts := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	_, _ = db.Append(Event{
		Timestamp: ts,
		Type:      "request",
		Tags:      map[string]string{"service": "api", "env": "prod"},
		Data:      map[string]any{"status": float64(200), "latency": float64(42.5)},
	})

	var buf bytes.Buffer
	ctx := context.Background()
	err = db.Export(ctx, &buf, Query{}, CSV)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	// Parse CSV output
	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("CSV read failed: %v", err)
	}

	if len(records) != 2 { // header + 1 data row
		t.Errorf("expected 2 rows, got %d", len(records))
	}

	// Check header
	header := records[0]
	expectedHeader := []string{"id", "timestamp", "type", "tag_env", "tag_service", "data_latency", "data_status"}
	if len(header) != len(expectedHeader) {
		t.Errorf("expected %d columns, got %d", len(expectedHeader), len(header))
	}

	for i, col := range expectedHeader {
		if header[i] != col {
			t.Errorf("header[%d]: expected %s, got %s", i, col, header[i])
		}
	}

	// Check data row
	row := records[1]
	if row[2] != "request" {
		t.Errorf("expected type 'request', got %s", row[2])
	}
	if row[3] != "prod" { // tag_env
		t.Errorf("expected tag_env 'prod', got %s", row[3])
	}
	if row[4] != "api" { // tag_service
		t.Errorf("expected tag_service 'api', got %s", row[4])
	}
}

func TestExportCSVMultipleEvents(t *testing.T) {
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

	// Insert events with different tags/data fields
	_, _ = db.Append(Event{Type: "request", Tags: map[string]string{"service": "api"}})
	_, _ = db.Append(Event{Type: "error", Tags: map[string]string{"env": "prod"}})
	_, _ = db.Append(Event{Type: "metric", Data: map[string]any{"value": float64(100)}})

	var buf bytes.Buffer
	ctx := context.Background()
	err = db.Export(ctx, &buf, Query{}, CSV)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("CSV read failed: %v", err)
	}

	// header + 3 data rows
	if len(records) != 4 {
		t.Errorf("expected 4 rows, got %d", len(records))
	}

	// Header should include all tags and data fields
	header := records[0]
	headerStr := strings.Join(header, ",")
	if !strings.Contains(headerStr, "tag_service") {
		t.Error("header missing tag_service")
	}
	if !strings.Contains(headerStr, "tag_env") {
		t.Error("header missing tag_env")
	}
	if !strings.Contains(headerStr, "data_value") {
		t.Error("header missing data_value")
	}
}

func TestExportEmpty(t *testing.T) {
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

	ctx := context.Background()

	// JSON export of empty db
	var jsonBuf bytes.Buffer
	err = db.Export(ctx, &jsonBuf, Query{}, JSON)
	if err != nil {
		t.Fatalf("JSON export failed: %v", err)
	}

	var events []Event
	if err := json.Unmarshal(jsonBuf.Bytes(), &events); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}

	// CSV export of empty db
	var csvBuf bytes.Buffer
	err = db.Export(ctx, &csvBuf, Query{}, CSV)
	if err != nil {
		t.Fatalf("CSV export failed: %v", err)
	}

	if csvBuf.Len() != 0 {
		t.Errorf("expected empty CSV output, got %d bytes", csvBuf.Len())
	}
}

func TestExportWithQuery(t *testing.T) {
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

	// Insert events
	_, _ = db.Append(Event{Type: "request"})
	_, _ = db.Append(Event{Type: "request"})
	_, _ = db.Append(Event{Type: "error"})

	var buf bytes.Buffer
	ctx := context.Background()

	// Export only requests
	err = db.Export(ctx, &buf, Query{Types: []string{"request"}}, JSON)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	var events []Event
	if err := json.Unmarshal(buf.Bytes(), &events); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}

	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}
}

func TestExportCSVDataTypes(t *testing.T) {
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

	// Insert event with various data types
	_, _ = db.Append(Event{
		Type: "test",
		Data: map[string]any{
			"string":  "hello",
			"number":  float64(42),
			"boolean": true,
			"array":   []any{1, 2, 3},
			"object":  map[string]any{"nested": "value"},
		},
	})

	var buf bytes.Buffer
	ctx := context.Background()
	err = db.Export(ctx, &buf, Query{}, CSV)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("CSV read failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(records))
	}

	// Find column indices
	header := records[0]
	colIndex := make(map[string]int)
	for i, col := range header {
		colIndex[col] = i
	}

	row := records[1]

	// String should be plain text
	if row[colIndex["data_string"]] != "hello" {
		t.Errorf("expected 'hello', got %s", row[colIndex["data_string"]])
	}

	// Boolean should be "true"
	if row[colIndex["data_boolean"]] != "true" {
		t.Errorf("expected 'true', got %s", row[colIndex["data_boolean"]])
	}

	// Array should be JSON encoded
	if row[colIndex["data_array"]] != "[1,2,3]" {
		t.Errorf("expected '[1,2,3]', got %s", row[colIndex["data_array"]])
	}
}

func TestFormatDataValue(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"int", 42, "42"},
		{"float", 3.14, "3.14"},
		{"array", []int{1, 2, 3}, "[1,2,3]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatDataValue(tt.input)
			if result != tt.expected {
				t.Errorf("formatDataValue(%v): expected %s, got %s", tt.input, tt.expected, result)
			}
		})
	}
}

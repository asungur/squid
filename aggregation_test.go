package squid

import (
	"context"
	"math"
	"os"
	"testing"
)

func TestAggregateCount(t *testing.T) {
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
	for i := 0; i < 10; i++ {
		_, err := db.Append(Event{
			Type: "metric",
			Data: map[string]any{"value": float64(i + 1)},
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	ctx := context.Background()
	result, err := db.Aggregate(ctx, Query{}, "", []AggregationType{Count})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if result.Count != 10 {
		t.Errorf("expected count 10, got %d", result.Count)
	}
}

func TestAggregateSum(t *testing.T) {
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

	// Insert test events: 1 + 2 + 3 + 4 + 5 = 15
	for i := 1; i <= 5; i++ {
		_, err := db.Append(Event{
			Type: "metric",
			Data: map[string]any{"value": float64(i)},
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	ctx := context.Background()
	result, err := db.Aggregate(ctx, Query{}, "value", []AggregationType{Sum})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if result.Sum != 15 {
		t.Errorf("expected sum 15, got %f", result.Sum)
	}
}

func TestAggregateAvg(t *testing.T) {
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

	// Insert test events: avg of 10, 20, 30 = 20
	for _, v := range []float64{10, 20, 30} {
		_, err := db.Append(Event{
			Type: "metric",
			Data: map[string]any{"value": v},
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	ctx := context.Background()
	result, err := db.Aggregate(ctx, Query{}, "value", []AggregationType{Avg})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if result.Avg != 20 {
		t.Errorf("expected avg 20, got %f", result.Avg)
	}
}

func TestAggregateMinMax(t *testing.T) {
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
	for _, v := range []float64{5, 2, 8, 1, 9, 3} {
		_, err := db.Append(Event{
			Type: "metric",
			Data: map[string]any{"value": v},
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	ctx := context.Background()
	result, err := db.Aggregate(ctx, Query{}, "value", []AggregationType{Min, Max})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if result.Min != 1 {
		t.Errorf("expected min 1, got %f", result.Min)
	}
	if result.Max != 9 {
		t.Errorf("expected max 9, got %f", result.Max)
	}
}

func TestAggregatePercentiles(t *testing.T) {
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

	// Insert 100 events with values 1-100
	for i := 1; i <= 100; i++ {
		_, err := db.Append(Event{
			Type: "metric",
			Data: map[string]any{"value": float64(i)},
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	ctx := context.Background()
	result, err := db.Aggregate(ctx, Query{}, "value", []AggregationType{P50, P95, P99})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	// P50 of 1-100 should be around 50.5
	if math.Abs(result.P50-50.5) > 0.5 {
		t.Errorf("expected P50 around 50.5, got %f", result.P50)
	}

	// P95 of 1-100 should be around 95.05
	if math.Abs(result.P95-95.05) > 0.5 {
		t.Errorf("expected P95 around 95.05, got %f", result.P95)
	}

	// P99 of 1-100 should be around 99.01
	if math.Abs(result.P99-99.01) > 0.5 {
		t.Errorf("expected P99 around 99.01, got %f", result.P99)
	}
}

func TestAggregateWithTypeFilter(t *testing.T) {
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

	// Insert metrics
	for i := 1; i <= 5; i++ {
		_, _ = db.Append(Event{Type: "metric", Data: map[string]any{"value": float64(i)}})
	}
	// Insert other events
	for i := 1; i <= 3; i++ {
		_, _ = db.Append(Event{Type: "error", Data: map[string]any{"value": float64(100)}})
	}

	ctx := context.Background()
	result, err := db.Aggregate(ctx, Query{Types: []string{"metric"}}, "value", []AggregationType{Count, Sum})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if result.Count != 5 {
		t.Errorf("expected count 5, got %d", result.Count)
	}
	if result.Sum != 15 {
		t.Errorf("expected sum 15, got %f", result.Sum)
	}
}

func TestAggregateWithTagFilter(t *testing.T) {
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
	_, _ = db.Append(Event{Type: "metric", Tags: map[string]string{"service": "api"}, Data: map[string]any{"value": 10.0}})
	_, _ = db.Append(Event{Type: "metric", Tags: map[string]string{"service": "api"}, Data: map[string]any{"value": 20.0}})
	_, _ = db.Append(Event{Type: "metric", Tags: map[string]string{"service": "web"}, Data: map[string]any{"value": 100.0}})

	ctx := context.Background()
	result, err := db.Aggregate(ctx, Query{Tags: map[string]string{"service": "api"}}, "value", []AggregationType{Count, Sum})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if result.Count != 2 {
		t.Errorf("expected count 2, got %d", result.Count)
	}
	if result.Sum != 30 {
		t.Errorf("expected sum 30, got %f", result.Sum)
	}
}

func TestAggregateEmptyResult(t *testing.T) {
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
	result, err := db.Aggregate(ctx, Query{}, "value", []AggregationType{Count, Sum, Avg})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if result.Count != 0 {
		t.Errorf("expected count 0, got %d", result.Count)
	}
	if result.Sum != 0 {
		t.Errorf("expected sum 0, got %f", result.Sum)
	}
}

func TestAggregateMissingField(t *testing.T) {
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

	// Insert events without the aggregation field
	_, _ = db.Append(Event{Type: "metric", Data: map[string]any{"other": "data"}})
	_, _ = db.Append(Event{Type: "metric", Data: map[string]any{"value": 10.0}})
	_, _ = db.Append(Event{Type: "metric"}) // No data at all

	ctx := context.Background()
	result, err := db.Aggregate(ctx, Query{}, "value", []AggregationType{Count, Sum})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	// Only the event with "value" field should be counted
	if result.Count != 1 {
		t.Errorf("expected count 1, got %d", result.Count)
	}
	if result.Sum != 10 {
		t.Errorf("expected sum 10, got %f", result.Sum)
	}
}

func TestAggregateAllTypes(t *testing.T) {
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

	// Insert test events: 1, 2, 3, 4, 5
	for i := 1; i <= 5; i++ {
		_, err := db.Append(Event{
			Type: "metric",
			Data: map[string]any{"value": float64(i)},
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	ctx := context.Background()
	result, err := db.Aggregate(ctx, Query{}, "value", []AggregationType{Count, Sum, Avg, Min, Max, P50, P95, P99})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if result.Count != 5 {
		t.Errorf("expected count 5, got %d", result.Count)
	}
	if result.Sum != 15 {
		t.Errorf("expected sum 15, got %f", result.Sum)
	}
	if result.Avg != 3 {
		t.Errorf("expected avg 3, got %f", result.Avg)
	}
	if result.Min != 1 {
		t.Errorf("expected min 1, got %f", result.Min)
	}
	if result.Max != 5 {
		t.Errorf("expected max 5, got %f", result.Max)
	}
	if result.P50 != 3 {
		t.Errorf("expected P50 3, got %f", result.P50)
	}
}

func TestPercentileEdgeCases(t *testing.T) {
	// Test with empty slice
	result := percentile([]float64{}, 0.5)
	if result != 0 {
		t.Errorf("expected 0 for empty slice, got %f", result)
	}

	// Test with single value
	result = percentile([]float64{42}, 0.5)
	if result != 42 {
		t.Errorf("expected 42 for single value, got %f", result)
	}

	// Test with two values
	result = percentile([]float64{10, 20}, 0.5)
	if result != 15 {
		t.Errorf("expected 15 for [10,20] at P50, got %f", result)
	}
}

func TestExtractNumericValue(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]any
		field    string
		expected float64
		ok       bool
	}{
		{"float64", map[string]any{"v": float64(1.5)}, "v", 1.5, true},
		{"int", map[string]any{"v": int(42)}, "v", 42, true},
		{"int64", map[string]any{"v": int64(100)}, "v", 100, true},
		{"uint", map[string]any{"v": uint(200)}, "v", 200, true},
		{"string", map[string]any{"v": "not a number"}, "v", 0, false},
		{"missing", map[string]any{"other": 1}, "v", 0, false},
		{"empty field", map[string]any{"v": 1}, "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &Event{Data: tt.data}
			val, ok := extractNumericValue(event, tt.field)
			if ok != tt.ok {
				t.Errorf("extractNumericValue ok: got %v, want %v", ok, tt.ok)
			}
			if ok && val != tt.expected {
				t.Errorf("extractNumericValue value: got %f, want %f", val, tt.expected)
			}
		})
	}
}

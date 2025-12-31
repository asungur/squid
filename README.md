<p align="center">
  <img src="logo.png" width="600" height="300" alt="Squid mascot"/>
</p>

# Squid
Squid is an event-log database for Go, built on BadgerDB. It provides append-only event storage with querying, aggregations, retention, and export capabilities.

## Getting Started

```bash
go get github.com/asungur/squid
```

### Open a Database

```go
import "github.com/asungur/squid"

sq, err := squid.Open("/path/to/data")
if err != nil {
    log.Fatal(err)
}
defer sq.Close()
```

### Append Events

```go
// Append a single event
event, err := sq.Append(squid.Event{
    Type: "request",
    Tags: map[string]string{
        "service": "api",
        "env":     "prod",
    },
    Data: map[string]any{
        "status":  200,
        "latency": 42.5,
    },
})

// Append multiple events in a batch
events, err := sq.AppendBatch([]squid.Event{
    {Type: "request", Tags: map[string]string{"service": "api"}},
    {Type: "error", Tags: map[string]string{"service": "web"}},
})
```

### Querying

```go
ctx := context.Background()

// Query all events
events, err := sq.Query(ctx, squid.Query{})

// Query by type
events, err := sq.Query(ctx, squid.Query{
    Types: []string{"request", "error"},
})

// Query by tags
events, err := sq.Query(ctx, squid.Query{
    Tags: map[string]string{"service": "api"},
})

// Query by time range
start := time.Now().Add(-1 * time.Hour)
end := time.Now()
events, err := sq.Query(ctx, squid.Query{
    Start: &start,
    End:   &end,
})

// Pagination and ordering
events, err := sq.Query(ctx, squid.Query{
    Limit:      100,
    Descending: true,  // newest first
})
```

### Aggregations

```go
ctx := context.Background()

// Aggregate a numeric field
result, err := sq.Aggregate(ctx, squid.Query{
    Types: []string{"request"},
}, "latency", []squid.AggregationType{
    squid.Count,
    squid.Sum,
    squid.Avg,
    squid.Min,
    squid.Max,
    squid.P50,
    squid.P95,
    squid.P99,
})

fmt.Printf("Count: %d\n", result.Count)
fmt.Printf("Average: %.2f\n", result.Avg)
fmt.Printf("P99: %.2f\n", result.P99)
```

### Retention Policies

```go
// Set retention policy to delete events older than 7 days
sq.SetRetention(squid.RetentionPolicy{
    MaxAge: 7 * 24 * time.Hour,
})

// Disable retention
sq.SetRetention(squid.RetentionPolicy{})

// Manual cleanup
deleted, err := sq.DeleteBefore(time.Now().Add(-24 * time.Hour))
```

### Exporting JSON and CSV

```go
ctx := context.Background()
var buf bytes.Buffer

// Export as JSON
err := sq.Export(ctx, &buf, squid.Query{}, squid.JSON)

// Export as CSV (tags and data fields are flattened with prefixes)
err := sq.Export(ctx, &buf, squid.Query{
    Types: []string{"request"},
}, squid.CSV)

// Write to file
file, _ := os.Create("events.json")
sq.Export(ctx, file, squid.Query{}, squid.JSON)
```

***Note:*** CSV export flattens tags and data fields with `tag_` and `data_` prefixes:

```bash
id,timestamp,type,tag_env,tag_service,data_latency,data_status
01HXYZ...,2024-01-01T10:00:00.000Z,request,prod,api,42.5,200
```

---

## Design

### Keys and Encoding

Keys are chronologically sorted using ULID which allows efficient time-range queries.

ULID provides monotonic generation which could handle same-millisecond events in terms of uniqueness. For this project `26 char` string was used.

| **Purpose** | **Key Pattern** | **Example** |
| --- | --- | --- |
| ***Primary event storage*** | `e:<ULID>` | `e:01HXYZ123ABC0000000000001` |
| ***Tag index*** | `t:<key>=<value>:<ULID>` | `t:service=api:01HXYZ123ABC...` |
| ***Type index*** | `y:<type>:<ULID>` | `y:request:01HXYZ123ABC...` |

For data serialisation, `JSON` was used to keep things simple and easy to debug.

Tag and Type fields are indexed for efficient querying.

## Further Development

- [ ]  [Add input validation](https://github.com/asungur/squid/blob/main/query.go#L27) to avoid large queries. (current architecture has limits)
- [ ]  [Use statistics to choose the more performant index type](https://github.com/asungur/squid/blob/main/query.go#L73-L83). (current implementation prioritises Type Index).
- [ ]  [Use a union index](https://github.com/asungur/squid/blob/main/query.go#L79) for multiple `type` filters.

---

### Contributing

- Please use [Github issues](https://github.com/asungur/squid/issues) to contribute.
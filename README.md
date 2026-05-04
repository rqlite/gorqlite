# gorqlite — a Go client for rqlite
[![Circle CI](https://circleci.com/gh/rqlite/gorqlite/tree/master.svg?style=svg)](https://circleci.com/gh/rqlite/gorqlite/tree/master)

gorqlite is a Go client for [rqlite](https://github.com/rqlite/rqlite). It hides the HTTP/JSON details and exposes a small API specialized for rqlite, plus a thin `database/sql` driver.

The main API mirrors a subset of `database/sql` semantics — `Open()`, `Query()`/`QueryOne()`, `Next()`/`Scan()`/`Map()`, `Write()`/`WriteOne()` — without pretending to be a full `database/sql` driver.

> Looking for a lightweight Go client? See [rqlite/rqlite-go-http](https://github.com/rqlite/rqlite-go-http).

## Status

Used in production by various groups, including [Replicated](https://www.replicated.com/blog/app-manager-with-rqlite).

## Features

- Speaks rqlite's HTTP API for you: POSTs, JSON, parameterized statements, the unified `/db/request` endpoint, and queued writes.
- Iterator with `database/sql`-style ergonomics (`Next()`, `Scan()`, `Map()`), plus typed `Null*` wrappers (`NullString`, `NullInt64`, …).
- Per-result metadata: timing, rows affected, last insert id.
- **Cluster discovery**: gorqlite probes `/status` (and `/nodes` on rqlite 6+) so you only need to point at one node. Failed requests are retried against the rest of the cluster.
- Disable discovery with `?disableClusterDiscovery=true` on the connection URL — useful when a load balancer (e.g. a Kubernetes Service) already handles peer selection.
- URL-style connection strings with optional credentials, consistency level, and timeout.
- `Context` variants of every API method.
- `Leader()` / `Peers()` for cluster introspection; `SetConsistencyLevel()` / `SetExecutionWithTransaction()` for tuning.
- `TraceOn(io.Writer)` / `TraceOff()` for verbose request/response tracing.
- No external dependencies — standard library only.

## Install

```sh
go get github.com/rqlite/gorqlite
```

## Quick start

```go
conn, err := gorqlite.Open("http://")              // localhost:4001, no auth
conn, err  = gorqlite.Open("https://")             // same, https
conn, err  = gorqlite.Open("https://localhost:4001/")

// With credentials and options.
conn, err  = gorqlite.Open("https://mary:secret2@localhost:4001/")
conn, err  = gorqlite.Open("https://server.example.com:4001/?level=weak")
conn, err  = gorqlite.Open("https://localhost:2265/?level=strong&timeout=30")
conn, err  = gorqlite.Open("https://localhost:2265/?disableClusterDiscovery=true")

// Provide your own *http.Client when you need control over TLS, proxies, etc.
client := &http.Client{}
conn, err = gorqlite.OpenWithClient("https://mary:secret2@localhost:4001/", client)

// Adjust the consistency level after opening.
conn.SetConsistencyLevel(gorqlite.ConsistencyLevelNone)
conn.SetConsistencyLevel(gorqlite.ConsistencyLevelWeak)
conn.SetConsistencyLevel(gorqlite.ConsistencyLevelStrong)
```

### Writes

```go
statements := []string{
    "INSERT INTO secret_agents(id, hero_name, abbrev) VALUES (125718, 'Speed Gibson', 'Speed')",
    "INSERT INTO secret_agents(id, hero_name, abbrev) VALUES (209166, 'Clint Barlow', 'Clint')",
    "INSERT INTO secret_agents(id, hero_name, abbrev) VALUES (44107,  'Barney Dunlap', 'Barney')",
}
results, err := conn.Write(statements)
for n, r := range results {
    fmt.Printf("result %d: %d rows affected\n", n, r.RowsAffected)
    if r.Err != nil {
        fmt.Printf("  error: %s\n", r.Err)
    }
}

// Single statement, with auto-increment.
res, err := conn.WriteOne("INSERT INTO foo (name) VALUES ('bar')")
fmt.Printf("last insert id: %d\n", res.LastInsertID)
```

### Queries

rqlite returns JSON, so numbers come back as `float64`; gorqlite will convert to `int64` for you when you `Scan()` into one. Other types you handle yourself.

```go
rows, err := conn.QueryOne("SELECT id, name FROM secret_agents WHERE id > 500")
fmt.Printf("query returned %d rows\n", rows.NumRows())

var (
    id   int64
    name string
)
for rows.Next() {
    if err := rows.Scan(&id, &name); err != nil {
        // ...
    }
    fmt.Printf("row %d: id=%d name=%q\n", rows.RowNumber(), id, name)
}
```

`QueryOne` takes a single statement; `Query([]string)` runs many in one transaction and returns `[]QueryResult`.

`Map()` is an alternative to `Scan()`:

```go
for rows.Next() {
    m, err := rows.Map() // map[columnName]interface{}
    id   := int64(m["id"].(float64)) // JSON numbers decode as float64
    name := m["name"].(string)
    _, _ = id, name
}
```

### Parameterized statements

```go
wr, err := conn.WriteOneParameterized(gorqlite.ParameterizedStatement{
    Query:     "INSERT INTO secret_agents(id, name, secret) VALUES (?, ?, ?)",
    Arguments: []interface{}{7, "James Bond", "not-a-secret"},
})

qr, err := conn.QueryOneParameterized(gorqlite.ParameterizedStatement{
    Query:     "SELECT id, name FROM secret_agents WHERE id > ?",
    Arguments: []interface{}{3},
})
```

Batch variants: `WriteParameterized`, `QueryParameterized`, `QueueParameterized`. Every method also has a `…Context` variant that takes a `context.Context`.

### Unified endpoint

`Request` / `RequestParameterized` send a mixed batch of reads and writes to rqlite's `/db/request`. The result for each statement carries either a `*QueryResult` or a `*WriteResult`.

```go
results, err := conn.Request([]string{
    "INSERT INTO secret_agents(id, name) VALUES (8, 'Felix')",
    "SELECT COUNT(*) FROM secret_agents",
})
```

### Queued writes

[Queued writes](https://rqlite.io/docs/api/queued-writes/) are fire-and-forget batched writes. They return a sequence number, not per-statement results.

```go
seq, err := conn.QueueOne("INSERT INTO secret_agents(id, name) VALUES (1, 'Q')")
seq, err  = conn.Queue([]string{ /* ... */ })
```

### Nullable types

```go
var name gorqlite.NullString
rows, err := conn.QueryOne("SELECT name FROM secret_agents WHERE id = 7")
for rows.Next() {
    _ = rows.Scan(&name)
}
if name.Valid {
    // use name.String
}
```

Also: `NullInt64`, `NullInt32`, `NullInt16`, `NullFloat64`, `NullBool`, `NullTime`.

### Cluster info

```go
leader, err := conn.Leader()
fmt.Println("current leader:", leader)

peers, err := conn.Peers()
for n, p := range peers {
    fmt.Printf("peer %d: %s\n", n, p)
}
```

### Tracing

```go
f, err := os.OpenFile("/tmp/gorqlite.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
gorqlite.TraceOn(f)
// ... or to stderr:
gorqlite.TraceOn(os.Stderr)
gorqlite.TraceOff()
```

Tracing is a package-level switch, so it covers `Open()` too. Credentials in URLs are redacted in trace output.

### Custom HTTP client

Pass your own `*http.Client` when you need to control TLS, certificate verification, or any other transport detail:

```go
tn := &http.Transport{
    TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}
client := &http.Client{Transport: tn}
conn, err := gorqlite.OpenWithClient("https://localhost:4001/", client)
```

## Notes

- If your rqlite cluster has access control enabled, the connecting user needs the **`status`** permission in addition to the usual `query`/`execute` permissions, so gorqlite can query cluster topology and fail over.
- rqlite does not stream results — a query returns the entire result set at once. Plan for large datasets accordingly.
- `Leader()` and `Peers()` each refresh cluster info before returning. Calling them in sequence can produce slightly inconsistent answers if the cluster changes between calls.
- `Open()` connects immediately (unlike `database/sql`'s `Open`) so that cluster discovery can run before you submit any work.

## `database/sql` driver

Prefer the native gorqlite API when you can. If you need `database/sql`, import the side-effect package `github.com/rqlite/gorqlite/stdlib`:

```go
package main

import (
    "database/sql"

    _ "github.com/rqlite/gorqlite/stdlib"
)

func main() {
    db, err := sql.Open("rqlite", "http://")
    if err != nil {
        panic(err)
    }
    if _, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT)"); err != nil {
        panic(err)
    }
}
```

Driver limitations:

- rqlite supports transactions only as a single submitted batch — there is no open-then-extend-then-commit flow, and no rollback.
- The SQLite C-level prepare/bind API isn't exposed by rqlite, so true `Prepare()` doesn't exist.
- `Begin()`, `Commit()`, `Rollback()`, and `Prepare()` are therefore no-ops.

## Concurrency

`*Connection` is safe to use from multiple goroutines: all configuration changes (`SetConsistencyLevel`, `SetExecutionWithTransaction`, `Close`) and the request methods are guarded internally.

A `*QueryResult` is a stateful iterator — `Next()`, `Scan()`, `Map()` mutate the row cursor. Don't share a single `QueryResult` across goroutines without your own synchronization.

## Tests

`go test ./...`. A running rqlite cluster is required.

Defaults:

```
url:               http://localhost:4001
table (main):      gorqlite_test
table (stdlib):    gorqlite_test_stdlib
```

Override via environment:

```
GORQLITE_TEST_URL=https://user:password@somewhere.example.com:1234
GORQLITE_TEST_TABLE=some_other_table
GORQLITE_TEST_TABLE_STDLIB=some_other_table
```

## Possible future work

- Backup API support.
- expvars / debugvars.
- Node removal API.
- "none" consistency reads sent to followers (currently always routed to the leader, except during leader discovery).

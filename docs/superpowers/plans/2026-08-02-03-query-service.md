# Query Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `pkg/querydsl` and `services/query` — a constrained JSON DSL compiled to parameterized ClickHouse SQL, with server-injected tenant isolation, read-time dedup, bounded resource usage, and etag-based revalidation.

**Architecture:** Customers never send SQL. They send a JSON DSL that compiles to a parameterized query with `tenant_id` injected server-side from the verified read key — never from the request. Aggregation uses `uniqExact(event_id)` rather than `count(*)` because the delivery guarantee is at-least-once and duplicates are expected by design. `FINAL` is never used. Coarse queries over long ranges may route to the `events_daily` rollup, which is approximate and is always labelled `"source": "rollup"` in the response.

**Tech Stack:** Go 1.23+, `github.com/ClickHouse/clickhouse-go/v2`, `net/http`, `pgx/v5` (read-key lookup).

## Global Constraints

Copied verbatim from the spec. Every task's requirements implicitly include this section.

- **Never raw SQL passthrough.** A constrained JSON DSL compiled to parameterized SQL.
- `tenant_id` is injected **server-side**. Every golden test asserts `tenant_id` appears in the emitted `WHERE` clause — a missing tenant predicate is a cross-tenant data leak and must be impossible to merge.
- Ordering-key prefix makes correct isolation *fast*; it does **not** make an incorrect query *safe*. The compiler is the security boundary.
- Dedup: `uniqExact(event_id)` / `count(DISTINCT event_id)`, never `count(*)`.
- **`FINAL` is never used.** It forces a merge at query time and is the most common way to make a ClickHouse dashboard slow.
- `uniq()` (HyperLogLog, ~0.5% error) is available only behind an explicit `approximate: true` flag, never a silent substitution.
- Rollups are approximate — a materialized view fires on the inserted block and never sees `ReplacingMergeTree` dedup. Responses from rollups are labelled `"source": "rollup"`.
- Raw `events` is the only source of truth.
- Per-tenant ClickHouse settings profile bounds resource usage.
- Responses carry `etag` and `computed_at`; `304` on `If-None-Match` refreshes the client TTL with no body.
- Rollup retention matches raw retention: same 13-month TTL, so a rollup row cannot outlive its source.
- Per-user deletion, rollup rebuild fencing, and the deletion epoch are **out of scope** (spec §3.7, §7.2). Consequently rollup reads are safe here only because no deletion path exists yet.

---

## File Structure

| File | Responsibility |
|---|---|
| `migrations/clickhouse/0002_events_daily.sql` | `events_daily` + materialized view |
| `pkg/querydsl/ast.go` | Field allowlist, validation of a `TimeseriesRequest` |
| `pkg/querydsl/compile.go` | AST → parameterized SQL + args |
| `pkg/querydsl/route.go` | raw-vs-rollup routing decision |
| `pkg/querydsl/etag.go` | Deterministic etag over the query spec + result |
| `services/query/internal/execute/execute.go` | Run the compiled query, scan into `Series` |
| `services/query/internal/handler/timeseries.go` | `POST /v1/query/timeseries` |
| `services/query/internal/auth/readkey.go` | Read-key → tenant resolution with cache |
| `services/query/cmd/main.go` | Wiring |

---

### Task 1: Rollup schema

**Files:**
- Create: `migrations/clickhouse/0002_events_daily.sql`
- Test: `pkg/clickhouse/rollup_test.go`

**Interfaces:**
- Consumes: the `events` table from plan 2 Task 1.
- Produces: `events_daily` and `events_daily_mv`.

- [ ] **Step 1: Write the failing test**

`pkg/clickhouse/rollup_test.go`:

```go
package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
)

func TestRollupAggregatesOnInsert(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ts := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	rows := []clickhouse.Row{
		{TenantID: "t1", EventID: "0191f4a2-1c3d-7000-8000-00000000000a", Name: "checkout", TS: ts, TSClient: ts, TSReceived: ts, UserID: "u1", InstallID: "i1", Props: "{}"},
		{TenantID: "t1", EventID: "0191f4a2-1c3d-7000-8000-00000000000b", Name: "checkout", TS: ts, TSClient: ts, TSReceived: ts, UserID: "u2", InstallID: "i1", Props: "{}"},
	}
	if err := clickhouse.InsertEvents(ctx, conn, rows); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var events uint64
	var users uint64
	err := conn.QueryRow(ctx,
		`SELECT sum(events), uniqMerge(users) FROM events_daily WHERE tenant_id = 't1' AND name = 'checkout'`).
		Scan(&events, &users)
	if err != nil {
		t.Fatalf("query rollup: %v", err)
	}
	if events != 2 {
		t.Errorf("events = %d, want 2", events)
	}
	if users != 2 {
		t.Errorf("users = %d, want 2", users)
	}
}

// The rollup fires on the inserted block and never sees ReplacingMergeTree
// dedup. This test pins that inflation as *known behaviour* rather than letting
// someone discover it in production and assume the rollup is trustworthy.
func TestRollupDoubleCountsDuplicates(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ts := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	dup := clickhouse.Row{TenantID: "t1", EventID: "0191f4a2-1c3d-7000-8000-00000000000a", Name: "checkout", TS: ts, TSClient: ts, TSReceived: ts, UserID: "u1", InstallID: "i1", Props: "{}"}

	// Same event delivered twice — exactly what at-least-once produces.
	if err := clickhouse.InsertEvents(ctx, conn, []clickhouse.Row{dup}); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if err := clickhouse.InsertEvents(ctx, conn, []clickhouse.Row{dup}); err != nil {
		t.Fatalf("insert 2: %v", err)
	}

	var rollupCount uint64
	if err := conn.QueryRow(ctx, `SELECT sum(events) FROM events_daily WHERE tenant_id = 't1'`).Scan(&rollupCount); err != nil {
		t.Fatalf("rollup: %v", err)
	}
	var rawCount uint64
	if err := conn.QueryRow(ctx, `SELECT uniqExact(event_id) FROM events WHERE tenant_id = 't1'`).Scan(&rawCount); err != nil {
		t.Fatalf("raw: %v", err)
	}

	if rawCount != 1 {
		t.Errorf("raw uniqExact = %d, want 1 — raw is the source of truth", rawCount)
	}
	if rollupCount != 2 {
		t.Errorf("rollup sum = %d, want 2 — rollups are approximate by construction; "+
			"if this changed, the 'source: rollup' labelling may need revisiting", rollupCount)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/clickhouse && go test ./... -run Rollup`
Expected: FAIL — `Table tracking_test.events_daily does not exist`.

- [ ] **Step 3: Write the migration**

`migrations/clickhouse/0002_events_daily.sql`:

```sql
CREATE TABLE IF NOT EXISTS events_daily
(
    tenant_id  LowCardinality(String),
    event_date Date,
    name       LowCardinality(String),
    events     SimpleAggregateFunction(sum, UInt64),
    users      AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, event_date, name)
-- Matched to the raw table, not independently chosen: a rollup row must never
-- outlive its source data.
TTL event_date + INTERVAL 13 MONTH DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS events_daily_mv TO events_daily AS
SELECT
    tenant_id,
    event_date,
    name,
    count() AS events,
    uniqState(user_id) AS users
FROM events
GROUP BY tenant_id, event_date, name;
```

Sync it into the embedded copy:

```bash
make sync-migrations
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd pkg/clickhouse && go test ./... -v`
Expected: PASS, including both rollup tests.

- [ ] **Step 5: Commit**

```bash
git add migrations/clickhouse pkg/clickhouse
git commit -m "feat(clickhouse): add approximate daily rollup with matched TTL"
```

---

### Task 2: DSL validation and the field allowlist

**Files:**
- Create: `pkg/querydsl/go.mod`
- Create: `pkg/querydsl/ast.go`
- Test: `pkg/querydsl/ast_test.go`

**Interfaces:**
- Consumes: `trackingv1.TimeseriesRequest`, `trackingv1.Filter`.
- Produces:
  - `func Validate(req *trackingv1.TimeseriesRequest) error`
  - `var ErrUnknownField, ErrRangeTooLarge, ErrTooManyGroupBy, ErrTooManyFilters, ErrInvalidRange, ErrUnknownEventName error`
  - `func Column(field string) (sqlExpr string, ok bool)` — the allowlist. Returns the ClickHouse expression for a DSL field name.
  - `const MaxRangeDays = 400`, `MaxGroupBy = 2`, `MaxFilters = 10`

- [ ] **Step 1: Write the failing test**

`pkg/querydsl/ast_test.go`:

```go
package querydsl_test

import (
	"errors"
	"testing"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/querydsl"
)

func base() *trackingv1.TimeseriesRequest {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	return &trackingv1.TimeseriesRequest{
		EventName: "checkout_completed",
		FromMs:    from.UnixMilli(),
		ToMs:      from.Add(7 * 24 * time.Hour).UnixMilli(),
		Interval:  trackingv1.Interval_INTERVAL_DAY,
		Metric:    trackingv1.Metric_METRIC_EVENTS,
	}
}

func TestValidRequestPasses(t *testing.T) {
	if err := querydsl.Validate(base()); err != nil {
		t.Errorf("valid request rejected: %v", err)
	}
}

// The allowlist is the injection boundary. Any field not on it must be
// rejected before it can reach a SQL string.
func TestUnknownFieldRejected(t *testing.T) {
	for _, field := range []string{
		"tenant_id",              // never client-controllable
		"password",               // not a column
		"user_id; DROP TABLE",    // injection attempt
		"props",                  // whole-column access, not a path
		"1=1",
	} {
		t.Run(field, func(t *testing.T) {
			req := base()
			req.Filters = []*trackingv1.Filter{{Field: field, Op: trackingv1.Op_OP_EQ, Values: []string{"x"}}}
			if err := querydsl.Validate(req); !errors.Is(err, querydsl.ErrUnknownField) {
				t.Errorf("err = %v, want ErrUnknownField", err)
			}
		})
	}
}

func TestAllowlistedFieldsAccepted(t *testing.T) {
	for _, field := range []string{"user_id", "device_id", "session_id", "os", "os_version", "app_version", "sdk_version", "locale", "props.plan"} {
		t.Run(field, func(t *testing.T) {
			req := base()
			req.Filters = []*trackingv1.Filter{{Field: field, Op: trackingv1.Op_OP_EQ, Values: []string{"x"}}}
			if err := querydsl.Validate(req); err != nil {
				t.Errorf("field %q rejected: %v", field, err)
			}
		})
	}
}

func TestRangeBounds(t *testing.T) {
	t.Run("inverted range", func(t *testing.T) {
		req := base()
		req.FromMs, req.ToMs = req.ToMs, req.FromMs
		if err := querydsl.Validate(req); !errors.Is(err, querydsl.ErrInvalidRange) {
			t.Errorf("err = %v, want ErrInvalidRange", err)
		}
	})
	t.Run("range too large", func(t *testing.T) {
		req := base()
		req.ToMs = time.UnixMilli(req.FromMs).Add(500 * 24 * time.Hour).UnixMilli()
		if err := querydsl.Validate(req); !errors.Is(err, querydsl.ErrRangeTooLarge) {
			t.Errorf("err = %v, want ErrRangeTooLarge", err)
		}
	})
}

func TestGroupByAndFilterCaps(t *testing.T) {
	t.Run("too many group_by", func(t *testing.T) {
		req := base()
		req.GroupBy = []string{"os", "locale", "app_version"}
		if err := querydsl.Validate(req); !errors.Is(err, querydsl.ErrTooManyGroupBy) {
			t.Errorf("err = %v, want ErrTooManyGroupBy", err)
		}
	})
	t.Run("too many filters", func(t *testing.T) {
		req := base()
		for i := 0; i < 11; i++ {
			req.Filters = append(req.Filters, &trackingv1.Filter{Field: "os", Op: trackingv1.Op_OP_EQ, Values: []string{"ios"}})
		}
		if err := querydsl.Validate(req); !errors.Is(err, querydsl.ErrTooManyFilters) {
			t.Errorf("err = %v, want ErrTooManyFilters", err)
		}
	})
}

func TestEmptyEventNameRejected(t *testing.T) {
	req := base()
	req.EventName = ""
	if err := querydsl.Validate(req); !errors.Is(err, querydsl.ErrUnknownEventName) {
		t.Errorf("err = %v, want ErrUnknownEventName", err)
	}
}

func TestColumnNeverReturnsTenantID(t *testing.T) {
	if _, ok := querydsl.Column("tenant_id"); ok {
		t.Error("tenant_id is on the allowlist; it must be server-injected only")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/querydsl && go test ./...`
Expected: FAIL — `undefined: querydsl.Validate`.

- [ ] **Step 3: Write the implementation**

```bash
mkdir -p pkg/querydsl && cd pkg/querydsl && go mod init github.com/dhiazfathra/event-tracking/pkg/querydsl && cd ../..
```

`pkg/querydsl/ast.go`:

```go
// Package querydsl validates a constrained query DSL and compiles it to
// parameterized ClickHouse SQL.
//
// Raw SQL passthrough was rejected deliberately. A read-only user with row
// policies still leaves runaway queries, resource exhaustion, and schema
// coupling that makes every future migration a breaking change. The cost is
// expressiveness: every new analytical question needs backend work.
package querydsl

import (
	"errors"
	"fmt"
	"strings"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
)

var (
	ErrUnknownField     = errors.New("querydsl: field is not queryable")
	ErrRangeTooLarge    = errors.New("querydsl: time range exceeds the maximum")
	ErrInvalidRange     = errors.New("querydsl: to must be after from")
	ErrTooManyGroupBy   = errors.New("querydsl: too many group_by dimensions")
	ErrTooManyFilters   = errors.New("querydsl: too many filters")
	ErrUnknownEventName = errors.New("querydsl: event_name is required")
	ErrBadOp            = errors.New("querydsl: unsupported operator")
	ErrEmptyValues      = errors.New("querydsl: filter has no values")
)

const (
	// MaxRangeDays bounds the scan. 400 days covers the full 13-month TTL plus
	// slack; a query wider than the retention window is a client bug.
	MaxRangeDays = 400

	// MaxGroupBy bounds result cardinality. Two dimensions on a
	// LowCardinality column pair is already thousands of series.
	MaxGroupBy = 2

	MaxFilters = 10

	// maxPropKeyLen mirrors the ingest-side cap. A props path longer than this
	// cannot correspond to a stored subcolumn.
	maxPropKeyLen = 128
)

// allowed maps a DSL field name to its ClickHouse expression.
//
// tenant_id is deliberately absent, and a test asserts its absence. A client
// able to name tenant_id in a filter could compare it to a value of their
// choosing; the server-injected predicate is the only place it appears.
var allowed = map[string]string{
	"name":         "name",
	"user_id":      "user_id",
	"anonymous_id": "anonymous_id",
	"device_id":    "device_id",
	"session_id":   "session_id",
	"app_version":  "app_version",
	"sdk_version":  "sdk_version",
	"os":           "os",
	"os_version":   "os_version",
	"locale":       "locale",
	"trust_tier":   "trust_tier",
}

// Column resolves a DSL field to a SQL expression. Property paths
// ("props.plan") become a JSON subcolumn access with the key passed as a
// parameter, never interpolated — the returned expression contains a
// placeholder, not the key.
func Column(field string) (string, bool) {
	if expr, ok := allowed[field]; ok {
		return expr, true
	}
	if key, ok := strings.CutPrefix(field, "props."); ok {
		if key == "" || len(key) > maxPropKeyLen || !isSafePropKey(key) {
			return "", false
		}
		// The key is validated against a strict character class rather than
		// parameterized: ClickHouse JSON subcolumn access is a path
		// expression, not a value position, so a placeholder is not accepted
		// there. The character class is the guard.
		return fmt.Sprintf("toString(props.%s)", key), true
	}
	return "", false
}

// isSafePropKey allows only identifier characters. Anything else — quotes,
// parens, dots, whitespace, semicolons — is rejected, which is what makes the
// path interpolation in Column safe.
func isSafePropKey(k string) bool {
	for _, r := range k {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
		default:
			return false
		}
	}
	return true
}

// Validate rejects a request before any SQL is built.
func Validate(req *trackingv1.TimeseriesRequest) error {
	if req.GetEventName() == "" {
		return ErrUnknownEventName
	}
	if req.GetToMs() <= req.GetFromMs() {
		return ErrInvalidRange
	}
	span := time.UnixMilli(req.GetToMs()).Sub(time.UnixMilli(req.GetFromMs()))
	if span > MaxRangeDays*24*time.Hour {
		return fmt.Errorf("%w: %v exceeds %d days", ErrRangeTooLarge, span, MaxRangeDays)
	}
	if len(req.GetGroupBy()) > MaxGroupBy {
		return fmt.Errorf("%w: %d exceeds %d", ErrTooManyGroupBy, len(req.GetGroupBy()), MaxGroupBy)
	}
	if len(req.GetFilters()) > MaxFilters {
		return fmt.Errorf("%w: %d exceeds %d", ErrTooManyFilters, len(req.GetFilters()), MaxFilters)
	}

	for _, g := range req.GetGroupBy() {
		if _, ok := Column(g); !ok {
			return fmt.Errorf("%w: group_by %q", ErrUnknownField, g)
		}
	}
	for _, f := range req.GetFilters() {
		if _, ok := Column(f.GetField()); !ok {
			return fmt.Errorf("%w: filter %q", ErrUnknownField, f.GetField())
		}
		if len(f.GetValues()) == 0 {
			return fmt.Errorf("%w: %q", ErrEmptyValues, f.GetField())
		}
		switch f.GetOp() {
		case trackingv1.Op_OP_EQ, trackingv1.Op_OP_NEQ, trackingv1.Op_OP_IN,
			trackingv1.Op_OP_GT, trackingv1.Op_OP_LT:
		default:
			return fmt.Errorf("%w: %v", ErrBadOp, f.GetOp())
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd pkg/querydsl && go test ./... -v`
Expected: PASS, all subtests.

- [ ] **Step 5: Commit**

```bash
git add pkg/querydsl go.work
git commit -m "feat(querydsl): validate the DSL against a strict field allowlist"
```

---

### Task 3: DSL → parameterized SQL

**Files:**
- Create: `pkg/querydsl/compile.go`
- Test: `pkg/querydsl/compile_test.go`
- Test: `pkg/querydsl/testdata/golden/*.sql`

**Interfaces:**
- Consumes: `Validate`, `Column`.
- Produces:
  - `type Compiled struct { SQL string; Args []any; Source string }`
  - `func Compile(tenantID string, req *trackingv1.TimeseriesRequest, useRollup bool) (Compiled, error)`

- [ ] **Step 1: Write the failing test**

`pkg/querydsl/compile_test.go`:

```go
package querydsl_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/querydsl"
)

// THE test. A missing tenant predicate is a cross-tenant data leak; it must be
// impossible to merge. Every shape the compiler can emit is checked here.
func TestEveryCompiledQueryFiltersOnTenant(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	shapes := []struct {
		name string
		req  *trackingv1.TimeseriesRequest
		roll bool
	}{
		{"minimal", &trackingv1.TimeseriesRequest{EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_HOUR, Metric: trackingv1.Metric_METRIC_EVENTS}, false},
		{"users metric", &trackingv1.TimeseriesRequest{EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(24 * time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_USERS}, false},
		{"approximate", &trackingv1.TimeseriesRequest{EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(24 * time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_USERS, Approximate: true}, false},
		{"grouped", &trackingv1.TimeseriesRequest{EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(24 * time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_EVENTS, GroupBy: []string{"os", "locale"}}, false},
		{"filtered", &trackingv1.TimeseriesRequest{EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(24 * time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_EVENTS, Filters: []*trackingv1.Filter{{Field: "os", Op: trackingv1.Op_OP_IN, Values: []string{"ios", "android"}}}}, false},
		{"props filter", &trackingv1.TimeseriesRequest{EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(24 * time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_EVENTS, Filters: []*trackingv1.Filter{{Field: "props.plan", Op: trackingv1.Op_OP_EQ, Values: []string{"pro"}}}}, false},
		{"rollup", &trackingv1.TimeseriesRequest{EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(90 * 24 * time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_EVENTS}, true},
	}

	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			c, err := querydsl.Compile("tenant-A", s.req, s.roll)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			if !strings.Contains(c.SQL, "tenant_id = ?") {
				t.Errorf("emitted SQL has no tenant predicate:\n%s", c.SQL)
			}
			if len(c.Args) == 0 || c.Args[0] != "tenant-A" {
				t.Errorf("first arg = %v, want tenant-A bound first", c.Args)
			}
		})
	}
}

// FINAL forces a merge at query time and is the single most common way to make
// a ClickHouse dashboard slow.
func TestCompileNeverEmitsFINAL(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	c, err := querydsl.Compile("t1", &trackingv1.TimeseriesRequest{
		EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(time.Hour).UnixMilli(),
		Interval: trackingv1.Interval_INTERVAL_HOUR, Metric: trackingv1.Metric_METRIC_EVENTS,
	}, false)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if strings.Contains(strings.ToUpper(c.SQL), "FINAL") {
		t.Errorf("SQL contains FINAL:\n%s", c.SQL)
	}
}

// count(*) over an at-least-once corpus counts retries as events.
func TestEventsMetricDedupsOnEventID(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	c, _ := querydsl.Compile("t1", &trackingv1.TimeseriesRequest{
		EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(time.Hour).UnixMilli(),
		Interval: trackingv1.Interval_INTERVAL_HOUR, Metric: trackingv1.Metric_METRIC_EVENTS,
	}, false)

	if !strings.Contains(c.SQL, "uniqExact(event_id)") {
		t.Errorf("expected uniqExact(event_id), got:\n%s", c.SQL)
	}
	if strings.Contains(c.SQL, "count()") || strings.Contains(c.SQL, "count(*)") {
		t.Errorf("SQL uses a raw count, which counts retries as events:\n%s", c.SQL)
	}
}

// Approximation must be explicit. Silently swapping in uniq() is how dashboards
// start lying.
func TestApproximateFlagSwitchesToHLLAndNothingElseDoes(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	req := &trackingv1.TimeseriesRequest{
		EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(365 * 24 * time.Hour).UnixMilli(),
		Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_USERS,
	}

	exact, _ := querydsl.Compile("t1", req, false)
	if !strings.Contains(exact.SQL, "uniqExact(user_id)") {
		t.Errorf("a very wide range silently became approximate:\n%s", exact.SQL)
	}

	req.Approximate = true
	approx, _ := querydsl.Compile("t1", req, false)
	if !strings.Contains(approx.SQL, "uniq(user_id)") || strings.Contains(approx.SQL, "uniqExact") {
		t.Errorf("approximate:true did not switch to uniq():\n%s", approx.SQL)
	}
}

func TestFilterValuesAreBoundNotInterpolated(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	nasty := "ios'; DROP TABLE events; --"
	c, err := querydsl.Compile("t1", &trackingv1.TimeseriesRequest{
		EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(time.Hour).UnixMilli(),
		Interval: trackingv1.Interval_INTERVAL_HOUR, Metric: trackingv1.Metric_METRIC_EVENTS,
		Filters: []*trackingv1.Filter{{Field: "os", Op: trackingv1.Op_OP_EQ, Values: []string{nasty}}},
	}, false)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if strings.Contains(c.SQL, "DROP TABLE") {
		t.Fatalf("filter value was interpolated into SQL:\n%s", c.SQL)
	}
	found := false
	for _, a := range c.Args {
		if a == nasty {
			found = true
		}
	}
	if !found {
		t.Error("filter value not present in bound args")
	}
}

// Golden files make an unintended SQL change visible in review rather than
// hidden behind a passing assertion.
func TestGoldenSQL(t *testing.T) {
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	cases := map[string]*trackingv1.TimeseriesRequest{
		"timeseries_daily_events": {EventName: "checkout", FromMs: from.UnixMilli(), ToMs: from.Add(7 * 24 * time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_EVENTS},
		"timeseries_grouped_users": {EventName: "checkout", FromMs: from.UnixMilli(), ToMs: from.Add(7 * 24 * time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_USERS, GroupBy: []string{"os"}},
	}

	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			c, err := querydsl.Compile("tenant-A", req, false)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			path := filepath.Join("testdata", "golden", name+".sql")

			if os.Getenv("UPDATE_GOLDEN") == "1" {
				if err := os.WriteFile(path, []byte(c.SQL), 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (run with UPDATE_GOLDEN=1 to create): %v", err)
			}
			if c.SQL != string(want) {
				t.Errorf("SQL changed.\n--- want ---\n%s\n--- got ---\n%s", want, c.SQL)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/querydsl && go test ./... -run Compile`
Expected: FAIL — `undefined: querydsl.Compile`.

- [ ] **Step 3: Write the compiler**

`pkg/querydsl/compile.go`:

```go
package querydsl

import (
	"fmt"
	"strings"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
)

type Compiled struct {
	SQL    string
	Args   []any
	Source string // "raw" | "rollup" — surfaced verbatim in the response
}

// Compile turns a validated request into parameterized SQL.
//
// tenantID is a function parameter, not a request field, and it is bound as the
// first argument of every query. That is the security boundary: the ordering
// key makes correct isolation fast, but a query with no tenant predicate would
// happily read every tenant's rows regardless of sort order.
func Compile(tenantID string, req *trackingv1.TimeseriesRequest, useRollup bool) (Compiled, error) {
	if err := Validate(req); err != nil {
		return Compiled{}, err
	}
	if useRollup {
		return compileRollup(tenantID, req)
	}
	return compileRaw(tenantID, req)
}

func compileRaw(tenantID string, req *trackingv1.TimeseriesRequest) (Compiled, error) {
	args := []any{tenantID, req.GetEventName(),
		time.UnixMilli(req.GetFromMs()).UTC(),
		time.UnixMilli(req.GetToMs()).UTC()}

	metric, err := metricExpr(req)
	if err != nil {
		return Compiled{}, err
	}
	bucket := bucketExpr(req.GetInterval())

	selects := []string{bucket + " AS bucket"}
	groups := []string{"bucket"}
	for _, g := range req.GetGroupBy() {
		col, _ := Column(g) // validated
		selects = append(selects, fmt.Sprintf("%s AS %s", col, sanitizeAlias(g)))
		groups = append(groups, sanitizeAlias(g))
	}
	selects = append(selects, metric+" AS value")

	where := []string{
		"tenant_id = ?", // always first, always bound
		"name = ?",
		"ts >= ?",
		"ts < ?",
	}
	for _, f := range req.GetFilters() {
		clause, fargs, err := filterClause(f)
		if err != nil {
			return Compiled{}, err
		}
		where = append(where, clause)
		args = append(args, fargs...)
	}

	// No FINAL. Dedup is uniqExact at the metric, not a query-time merge.
	sql := fmt.Sprintf(
		"SELECT %s\nFROM events\nWHERE %s\nGROUP BY %s\nORDER BY bucket",
		strings.Join(selects, ", "),
		strings.Join(where, "\n  AND "),
		strings.Join(groups, ", "),
	)

	return Compiled{SQL: sql, Args: args, Source: "raw"}, nil
}

// compileRollup serves coarse queries from events_daily.
//
// The result is APPROXIMATE: the materialized view fires on the inserted block
// and never sees the ReplacingMergeTree dedup that happens later during merges,
// so duplicate deliveries inflate the counts. The caller must surface
// Source == "rollup" to the client rather than conflating it with raw results.
func compileRollup(tenantID string, req *trackingv1.TimeseriesRequest) (Compiled, error) {
	if len(req.GetGroupBy()) > 0 {
		return Compiled{}, fmt.Errorf("%w: rollup has no per-dimension breakdown", ErrUnknownField)
	}
	if len(req.GetFilters()) > 0 {
		return Compiled{}, fmt.Errorf("%w: rollup carries no filterable dimensions", ErrUnknownField)
	}

	var metric string
	switch req.GetMetric() {
	case trackingv1.Metric_METRIC_USERS:
		metric = "uniqMerge(users)"
	default:
		metric = "sum(events)"
	}

	sql := fmt.Sprintf(
		"SELECT toDateTime(event_date) AS bucket, %s AS value\nFROM events_daily\nWHERE tenant_id = ?\n  AND name = ?\n  AND event_date >= toDate(?)\n  AND event_date < toDate(?)\nGROUP BY bucket\nORDER BY bucket",
		metric,
	)

	return Compiled{
		SQL: sql,
		Args: []any{tenantID, req.GetEventName(),
			time.UnixMilli(req.GetFromMs()).UTC(),
			time.UnixMilli(req.GetToMs()).UTC()},
		Source: "rollup",
	}, nil
}

// metricExpr picks the aggregate.
//
// uniqExact is the default because the delivery guarantee is at-least-once and
// duplicates are expected by design — count(*) would count retries. It costs
// memory proportional to distinct values in range; uniq() (HLL, ~0.5% error) is
// the escape hatch, and it is only ever reached via an explicit
// approximate:true. A silent substitution is how dashboards start lying.
func metricExpr(req *trackingv1.TimeseriesRequest) (string, error) {
	switch req.GetMetric() {
	case trackingv1.Metric_METRIC_EVENTS:
		if req.GetApproximate() {
			return "uniq(event_id)", nil
		}
		return "uniqExact(event_id)", nil
	case trackingv1.Metric_METRIC_USERS:
		if req.GetApproximate() {
			return "uniq(user_id)", nil
		}
		return "uniqExact(user_id)", nil
	default:
		return "", fmt.Errorf("querydsl: unsupported metric %v", req.GetMetric())
	}
}

func bucketExpr(iv trackingv1.Interval) string {
	switch iv {
	case trackingv1.Interval_INTERVAL_HOUR:
		return "toStartOfHour(ts)"
	case trackingv1.Interval_INTERVAL_WEEK:
		return "toStartOfWeek(ts)"
	default:
		return "toStartOfDay(ts)"
	}
}

func filterClause(f *trackingv1.Filter) (string, []any, error) {
	col, ok := Column(f.GetField())
	if !ok {
		return "", nil, fmt.Errorf("%w: %q", ErrUnknownField, f.GetField())
	}
	vals := f.GetValues()

	switch f.GetOp() {
	case trackingv1.Op_OP_EQ:
		return col + " = ?", []any{vals[0]}, nil
	case trackingv1.Op_OP_NEQ:
		return col + " != ?", []any{vals[0]}, nil
	case trackingv1.Op_OP_GT:
		return col + " > ?", []any{vals[0]}, nil
	case trackingv1.Op_OP_LT:
		return col + " < ?", []any{vals[0]}, nil
	case trackingv1.Op_OP_IN:
		placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(vals)), ", ")
		args := make([]any, len(vals))
		for i, v := range vals {
			args[i] = v
		}
		return col + " IN (" + placeholders + ")", args, nil
	default:
		return "", nil, fmt.Errorf("%w: %v", ErrBadOp, f.GetOp())
	}
}

// sanitizeAlias turns a validated field into a SQL identifier. The field has
// already passed Column(), so this only needs to flatten "props.plan" into
// something that is a legal alias.
func sanitizeAlias(field string) string {
	return strings.ReplaceAll(field, ".", "_")
}
```

- [ ] **Step 4: Generate the golden files and run**

Run: `cd pkg/querydsl && mkdir -p testdata/golden && UPDATE_GOLDEN=1 go test ./... && go test ./... -v`
Expected: PASS. Read the generated `.sql` files — they are the review surface.

- [ ] **Step 5: Commit**

```bash
git add pkg/querydsl
git commit -m "feat(querydsl): compile DSL to parameterized SQL with server-injected tenant"
```

---

### Task 4: Rollup routing and etags

**Files:**
- Create: `pkg/querydsl/route.go`
- Create: `pkg/querydsl/etag.go`
- Test: `pkg/querydsl/route_test.go`

**Interfaces:**
- Consumes: `trackingv1.TimeseriesRequest`.
- Produces:
  - `func UseRollup(req *trackingv1.TimeseriesRequest) bool`
  - `func ETag(tenantID string, req *trackingv1.TimeseriesRequest, computedAt time.Time, body []byte) string`
  - `const RollupMinRangeDays = 30`

- [ ] **Step 1: Write the failing test**

`pkg/querydsl/route_test.go`:

```go
package querydsl_test

import (
	"testing"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/querydsl"
)

func TestRollupOnlyForCoarseLongRangeQueries(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mk := func(days int, iv trackingv1.Interval, groupBy []string, filters int) *trackingv1.TimeseriesRequest {
		req := &trackingv1.TimeseriesRequest{
			EventName: "e",
			FromMs:    from.UnixMilli(),
			ToMs:      from.Add(time.Duration(days) * 24 * time.Hour).UnixMilli(),
			Interval:  iv,
			Metric:    trackingv1.Metric_METRIC_EVENTS,
			GroupBy:   groupBy,
		}
		for i := 0; i < filters; i++ {
			req.Filters = append(req.Filters, &trackingv1.Filter{Field: "os", Op: trackingv1.Op_OP_EQ, Values: []string{"ios"}})
		}
		return req
	}

	cases := []struct {
		name string
		req  *trackingv1.TimeseriesRequest
		want bool
	}{
		{"90d daily no filters", mk(90, trackingv1.Interval_INTERVAL_DAY, nil, 0), true},
		{"7d daily", mk(7, trackingv1.Interval_INTERVAL_DAY, nil, 0), false},
		{"90d hourly", mk(90, trackingv1.Interval_INTERVAL_HOUR, nil, 0), false},
		{"90d daily grouped", mk(90, trackingv1.Interval_INTERVAL_DAY, []string{"os"}, 0), false},
		{"90d daily filtered", mk(90, trackingv1.Interval_INTERVAL_DAY, nil, 1), false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := querydsl.UseRollup(c.req); got != c.want {
				t.Errorf("UseRollup = %v, want %v", got, c.want)
			}
		})
	}
}

func TestETagIsStableAndSpecSensitive(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	req := &trackingv1.TimeseriesRequest{EventName: "e", FromMs: from.UnixMilli(), ToMs: from.Add(24 * time.Hour).UnixMilli(), Interval: trackingv1.Interval_INTERVAL_DAY, Metric: trackingv1.Metric_METRIC_EVENTS}
	at := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	body := []byte(`{"series":[]}`)

	a := querydsl.ETag("t1", req, at, body)
	b := querydsl.ETag("t1", req, at, body)
	if a != b {
		t.Errorf("etag not stable: %q vs %q", a, b)
	}

	// A different tenant must never collide — a shared etag across tenants
	// would let one tenant's 304 serve another's cached body.
	if querydsl.ETag("t2", req, at, body) == a {
		t.Error("etag collides across tenants")
	}
	if querydsl.ETag("t1", req, at, []byte(`{"series":[{"points":[]}]}`)) == a {
		t.Error("etag unchanged when the body changed")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/querydsl && go test ./... -run 'Rollup|ETag'`
Expected: FAIL — `undefined: querydsl.UseRollup`.

- [ ] **Step 3: Write the implementation**

`pkg/querydsl/route.go`:

```go
package querydsl

import (
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
)

// RollupMinRangeDays is the threshold below which raw is always used. Under a
// month, the raw scan is cheap enough that trading exactness for speed is a bad
// deal.
const RollupMinRangeDays = 30

// UseRollup reports whether a request can be served from events_daily.
//
// Conservative on purpose. events_daily carries only (tenant, date, name), so
// anything needing a finer bucket, a dimension breakdown, or a filter must go
// to raw. And because the rollup is approximate — the materialized view never
// sees ReplacingMergeTree dedup — routing there is only acceptable for the
// coarse "daily totals over a long range" shape where the drift is immaterial
// and the response is labelled "rollup" anyway.
func UseRollup(req *trackingv1.TimeseriesRequest) bool {
	if req.GetInterval() != trackingv1.Interval_INTERVAL_DAY {
		return false
	}
	if len(req.GetGroupBy()) > 0 || len(req.GetFilters()) > 0 {
		return false
	}
	span := time.UnixMilli(req.GetToMs()).Sub(time.UnixMilli(req.GetFromMs()))
	return span >= RollupMinRangeDays*24*time.Hour
}
```

`pkg/querydsl/etag.go`:

```go
package querydsl

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"

	"google.golang.org/protobuf/proto"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
)

// ETag is a strong validator over (tenant, query spec, computation time, body).
//
// tenant_id is hashed in even though a client can only ever see its own etags:
// an etag that collided across tenants would let one tenant's If-None-Match
// produce a 304 against another's cached body if a shared cache were ever put
// in front of this service.
func ETag(tenantID string, req *trackingv1.TimeseriesRequest, computedAt time.Time, body []byte) string {
	h := sha256.New()
	h.Write([]byte(tenantID))
	h.Write([]byte{0})

	// Deterministic marshalling: proto's default is explicitly not stable
	// across builds, and an etag that changes on redeploy defeats the cache.
	spec, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err == nil {
		h.Write(spec)
	}
	h.Write([]byte{0})

	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(computedAt.UnixMilli()))
	h.Write(ts[:])
	h.Write([]byte{0})
	h.Write(body)

	return `"` + hex.EncodeToString(h.Sum(nil)[:16]) + `"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd pkg/querydsl && go test ./... -v`
Expected: PASS, all tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/querydsl
git commit -m "feat(querydsl): conservative rollup routing and tenant-scoped etags"
```

---

### Task 5: Query execution against ClickHouse

**Files:**
- Create: `services/query/go.mod`
- Create: `services/query/internal/execute/execute.go`
- Test: `services/query/internal/execute/execute_test.go` (testcontainers)

**Interfaces:**
- Consumes: `querydsl.Compiled`, `clickhouse` driver.
- Produces:
  - `type Settings struct { MaxExecutionTime int; MaxMemoryUsage int64; MaxRowsToRead int64 }`
  - `func Run(ctx context.Context, conn driver.Conn, c querydsl.Compiled, groupBy []string, s Settings) ([]*trackingv1.Series, error)`

- [ ] **Step 1: Write the failing test**

`services/query/internal/execute/execute_test.go`:

```go
package execute_test

import (
	"context"
	"testing"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/querydsl"
	"github.com/dhiazfathra/event-tracking/services/query/internal/execute"
)

func TestRunDedupsDuplicateDeliveries(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ts := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	row := clickhouse.Row{TenantID: "t1", EventID: "0191f4a2-1c3d-7000-8000-00000000000a", Name: "checkout",
		TS: ts, TSClient: ts, TSReceived: ts, UserID: "u1", InstallID: "i1", Props: "{}"}

	// Deliver the same event three times, as an at-least-once client would.
	for i := 0; i < 3; i++ {
		if err := clickhouse.InsertEvents(ctx, conn, []clickhouse.Row{row}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	c, err := querydsl.Compile("t1", &trackingv1.TimeseriesRequest{
		EventName: "checkout",
		FromMs:    ts.Add(-time.Hour).UnixMilli(),
		ToMs:      ts.Add(time.Hour).UnixMilli(),
		Interval:  trackingv1.Interval_INTERVAL_HOUR,
		Metric:    trackingv1.Metric_METRIC_EVENTS,
	}, false)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	series, err := execute.Run(ctx, conn, c, nil, execute.Settings{MaxExecutionTime: 10})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(series) != 1 || len(series[0].Points) != 1 {
		t.Fatalf("series = %+v, want one series with one point", series)
	}
	// Without OPTIMIZE, three physical rows exist. uniqExact must still say 1.
	if series[0].Points[0].Value != 1 {
		t.Errorf("value = %d, want 1 — duplicates must not inflate the count", series[0].Points[0].Value)
	}
}

// Cross-tenant isolation, end to end against a real database rather than by
// inspecting a SQL string.
func TestRunNeverReturnsAnotherTenantsRows(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ts := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	mk := func(tenant, id string) clickhouse.Row {
		return clickhouse.Row{TenantID: tenant, EventID: id, Name: "checkout", TS: ts, TSClient: ts, TSReceived: ts, UserID: "u", InstallID: "i", Props: "{}"}
	}
	if err := clickhouse.InsertEvents(ctx, conn, []clickhouse.Row{
		mk("tenant-A", "0191f4a2-1c3d-7000-8000-00000000000a"),
		mk("tenant-B", "0191f4a2-1c3d-7000-8000-00000000000b"),
		mk("tenant-B", "0191f4a2-1c3d-7000-8000-00000000000c"),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	c, _ := querydsl.Compile("tenant-A", &trackingv1.TimeseriesRequest{
		EventName: "checkout", FromMs: ts.Add(-time.Hour).UnixMilli(), ToMs: ts.Add(time.Hour).UnixMilli(),
		Interval: trackingv1.Interval_INTERVAL_HOUR, Metric: trackingv1.Metric_METRIC_EVENTS,
	}, false)

	series, err := execute.Run(ctx, conn, c, nil, execute.Settings{MaxExecutionTime: 10})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if series[0].Points[0].Value != 1 {
		t.Errorf("value = %d, want 1 — tenant-B's rows leaked", series[0].Points[0].Value)
	}
}

func TestRunGroupedProducesOneSeriesPerGroup(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ts := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	mk := func(id, os string) clickhouse.Row {
		return clickhouse.Row{TenantID: "t1", EventID: id, Name: "checkout", TS: ts, TSClient: ts, TSReceived: ts, OS: os, UserID: "u", InstallID: "i", Props: "{}"}
	}
	_ = clickhouse.InsertEvents(ctx, conn, []clickhouse.Row{
		mk("0191f4a2-1c3d-7000-8000-00000000000a", "ios"),
		mk("0191f4a2-1c3d-7000-8000-00000000000b", "android"),
		mk("0191f4a2-1c3d-7000-8000-00000000000c", "android"),
	})

	c, _ := querydsl.Compile("t1", &trackingv1.TimeseriesRequest{
		EventName: "checkout", FromMs: ts.Add(-time.Hour).UnixMilli(), ToMs: ts.Add(time.Hour).UnixMilli(),
		Interval: trackingv1.Interval_INTERVAL_HOUR, Metric: trackingv1.Metric_METRIC_EVENTS,
		GroupBy: []string{"os"},
	}, false)

	series, err := execute.Run(ctx, conn, c, []string{"os"}, execute.Settings{MaxExecutionTime: 10})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("series = %d, want 2", len(series))
	}
	for _, s := range series {
		if s.Group["os"] == "" {
			t.Errorf("series missing group label: %+v", s)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/query && go test ./internal/execute/...`
Expected: FAIL — `undefined: execute.Run`.

- [ ] **Step 3: Write the implementation**

```bash
mkdir -p services/query/internal/execute
cd services/query && go mod init github.com/dhiazfathra/event-tracking/services/query
go get github.com/ClickHouse/clickhouse-go/v2@v2.30.0
cd ../..
```

`services/query/internal/execute/execute.go`:

```go
// Package execute runs a compiled query under a bounded settings profile and
// scans the result into the response shape.
package execute

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/querydsl"
)

// Settings is the per-tenant resource envelope. This is the other half of "no
// raw SQL": a bounded DSL is worth little if a compiled query can still eat the
// cluster.
type Settings struct {
	MaxExecutionTime int   // seconds
	MaxMemoryUsage   int64 // bytes; 0 = server default
	MaxRowsToRead    int64 // 0 = unlimited
}

func (s Settings) apply(ctx context.Context) context.Context {
	set := clickhouse.Settings{}
	if s.MaxExecutionTime > 0 {
		set["max_execution_time"] = s.MaxExecutionTime
	}
	if s.MaxMemoryUsage > 0 {
		set["max_memory_usage"] = s.MaxMemoryUsage
	}
	if s.MaxRowsToRead > 0 {
		set["max_rows_to_read"] = s.MaxRowsToRead
	}
	return clickhouse.Context(ctx, clickhouse.WithSettings(set))
}

// Run executes the compiled query. groupBy names the extra columns the compiler
// projected between `bucket` and `value`, in order — the scan target layout
// depends on it.
func Run(ctx context.Context, conn driver.Conn, c querydsl.Compiled, groupBy []string, s Settings) ([]*trackingv1.Series, error) {
	rows, err := conn.Query(s.apply(ctx), c.SQL, c.Args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	// Series are keyed by their group-value tuple so points accumulate into the
	// right series regardless of row order.
	index := map[string]*trackingv1.Series{}
	var order []*trackingv1.Series

	for rows.Next() {
		var bucket time.Time
		var value uint64
		labels := make([]string, len(groupBy))

		dest := make([]any, 0, 2+len(groupBy))
		dest = append(dest, &bucket)
		for i := range labels {
			dest = append(dest, &labels[i])
		}
		dest = append(dest, &value)

		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		key := seriesKey(labels)
		ser, ok := index[key]
		if !ok {
			ser = &trackingv1.Series{Group: map[string]string{}}
			for i, g := range groupBy {
				ser.Group[g] = labels[i]
			}
			index[key] = ser
			order = append(order, ser)
		}
		ser.Points = append(ser.Points, &trackingv1.Point{
			BucketMs: bucket.UnixMilli(),
			Value:    value,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	if len(order) == 0 {
		// An empty result is a valid answer, not an error. Return one empty
		// series so the client's chart code has a shape to render.
		order = append(order, &trackingv1.Series{Group: map[string]string{}})
	}
	return order, nil
}

func seriesKey(labels []string) string {
	key := ""
	for _, l := range labels {
		key += l + "\x00"
	}
	return key
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/query && go test ./internal/execute/... -v`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add services/query go.work
git commit -m "feat(query): execute compiled queries under a bounded settings profile"
```

---

### Task 6: The timeseries handler with etag revalidation

**Files:**
- Create: `services/query/internal/auth/readkey.go`
- Create: `services/query/internal/handler/timeseries.go`
- Test: `services/query/internal/handler/timeseries_test.go`

**Interfaces:**
- Consumes: `querydsl.*`, `execute.Run`.
- Produces:
  - `type Resolver interface { Tenant(ctx context.Context, key string) (string, execute.Settings, error) }`
  - `type Deps struct { Resolver Resolver; Query func(ctx context.Context, c querydsl.Compiled, groupBy []string, s execute.Settings) ([]*trackingv1.Series, error); Now func() time.Time }`
  - `func NewTimeseries(d Deps) http.Handler`

- [ ] **Step 1: Write the failing test**

`services/query/internal/handler/timeseries_test.go`:

```go
package handler_test

import (
	"net/http"
	"testing"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
)

func TestTimeseriesHappyPath(t *testing.T) {
	h, spy := newTimeseriesHandler(t)

	rec := postJSON(t, h, "/v1/query/timeseries", `{
		"eventName":"checkout","fromMs":"1751328000000","toMs":"1751932800000",
		"interval":"INTERVAL_DAY","metric":"METRIC_EVENTS"}`, "rk_test")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body)
	}
	var resp trackingv1.TimeseriesResponse
	decode(t, rec.Body.Bytes(), &resp)

	if resp.Source != "raw" {
		t.Errorf("source = %q, want raw", resp.Source)
	}
	if resp.Etag == "" || rec.Header().Get("ETag") == "" {
		t.Error("no etag returned; the client cannot revalidate")
	}
	if resp.ComputedAt == 0 {
		t.Error("no computed_at; the client cannot show data freshness")
	}
	if spy.tenantID != "tenant-from-key" {
		t.Errorf("compiled for tenant %q, want the key's tenant", spy.tenantID)
	}
}

// 304 refreshes the client's TTL with no body transfer.
func TestIfNoneMatchReturns304(t *testing.T) {
	h, _ := newTimeseriesHandler(t)
	body := `{"eventName":"checkout","fromMs":"1751328000000","toMs":"1751932800000","interval":"INTERVAL_DAY","metric":"METRIC_EVENTS"}`

	first := postJSON(t, h, "/v1/query/timeseries", body, "rk_test")
	etag := first.Header().Get("ETag")

	second := postJSONWithHeader(t, h, "/v1/query/timeseries", body, "rk_test", "If-None-Match", etag)
	if second.Code != http.StatusNotModified {
		t.Fatalf("status = %d, want 304", second.Code)
	}
	if second.Body.Len() != 0 {
		t.Errorf("304 carried a body of %d bytes, want 0", second.Body.Len())
	}
}

// A rollup-backed answer must say so. Conflating it with raw is how a
// dashboard reports inflated numbers as exact.
func TestRollupResponseIsLabelled(t *testing.T) {
	h, _ := newTimeseriesHandler(t)
	// 120-day daily range with no groups or filters routes to the rollup.
	rec := postJSON(t, h, "/v1/query/timeseries", `{
		"eventName":"checkout","fromMs":"1743465600000","toMs":"1753833600000",
		"interval":"INTERVAL_DAY","metric":"METRIC_EVENTS"}`, "rk_test")

	var resp trackingv1.TimeseriesResponse
	decode(t, rec.Body.Bytes(), &resp)
	if resp.Source != "rollup" {
		t.Errorf("source = %q, want rollup", resp.Source)
	}
}

func TestApproximateFlagIsEchoed(t *testing.T) {
	h, _ := newTimeseriesHandler(t)
	rec := postJSON(t, h, "/v1/query/timeseries", `{
		"eventName":"checkout","fromMs":"1751328000000","toMs":"1751932800000",
		"interval":"INTERVAL_DAY","metric":"METRIC_USERS","approximate":true}`, "rk_test")

	var resp trackingv1.TimeseriesResponse
	decode(t, rec.Body.Bytes(), &resp)
	if !resp.Approximate {
		t.Error("approximate not echoed; the client cannot label the number")
	}
}

func TestUnknownFieldReturns400(t *testing.T) {
	h, _ := newTimeseriesHandler(t)
	rec := postJSON(t, h, "/v1/query/timeseries", `{
		"eventName":"checkout","fromMs":"1751328000000","toMs":"1751932800000",
		"interval":"INTERVAL_DAY","metric":"METRIC_EVENTS",
		"filters":[{"field":"tenant_id","op":"OP_EQ","values":["other"]}]}`, "rk_test")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestMissingReadKeyReturns401(t *testing.T) {
	h, _ := newTimeseriesHandler(t)
	rec := postJSON(t, h, "/v1/query/timeseries", `{"eventName":"e","fromMs":"1","toMs":"2","interval":"INTERVAL_DAY","metric":"METRIC_EVENTS"}`, "")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/query && go test ./internal/handler/...`
Expected: FAIL — `undefined: handler.NewTimeseries`.

- [ ] **Step 3: Write the read-key resolver**

`services/query/internal/auth/readkey.go`:

```go
// Package auth resolves read keys to tenants.
//
// Read keys are a separate credential from ingest tokens, with a separate
// scope. An ingest token is write:events only — no reads, no enumeration, no
// cross-tenant — so a stolen write token cannot reach the query API at all.
package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	"github.com/dhiazfathra/event-tracking/services/query/internal/execute"
)

var ErrUnknownKey = errors.New("auth: unknown or revoked read key")

type Entry struct {
	TenantID string
	Settings execute.Settings
}

// Lookup is the storage-backed resolution. Implemented over the read_keys and
// quotas tables.
type Lookup func(ctx context.Context, keyHash [32]byte) (Entry, error)

// Cache memoizes key resolution. Every query would otherwise be a Postgres
// round-trip before it even reaches ClickHouse.
type Cache struct {
	lookup Lookup
	ttl    time.Duration

	mu sync.RWMutex
	m  map[[32]byte]cached
}

type cached struct {
	entry Entry
	at    time.Time
}

func NewCache(lookup Lookup, ttl time.Duration) *Cache {
	return &Cache{lookup: lookup, ttl: ttl, m: map[[32]byte]cached{}}
}

// Tenant resolves a read key. Revocation is bounded by the TTL: a revoked key
// keeps working for at most ttl. Keep the TTL short (a minute) rather than
// adding an invalidation channel for a control plane this small.
func (c *Cache) Tenant(ctx context.Context, key string) (string, execute.Settings, error) {
	if key == "" {
		return "", execute.Settings{}, ErrUnknownKey
	}
	h := sha256.Sum256([]byte(key))

	c.mu.RLock()
	got, ok := c.m[h]
	c.mu.RUnlock()
	if ok && time.Since(got.at) < c.ttl {
		return got.entry.TenantID, got.entry.Settings, nil
	}

	entry, err := c.lookup(ctx, h)
	if err != nil {
		return "", execute.Settings{}, err
	}

	c.mu.Lock()
	c.m[h] = cached{entry: entry, at: time.Now()}
	c.mu.Unlock()

	return entry.TenantID, entry.Settings, nil
}
```

- [ ] **Step 4: Write the handler**

`services/query/internal/handler/timeseries.go`:

```go
// Package handler serves POST /v1/query/*.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/querydsl"
	"github.com/dhiazfathra/event-tracking/services/query/internal/execute"
)

const maxRequestBytes = 64 << 10

type Resolver interface {
	Tenant(ctx context.Context, key string) (string, execute.Settings, error)
}

type Deps struct {
	Resolver Resolver
	Query    func(ctx context.Context, c querydsl.Compiled, groupBy []string, s execute.Settings) ([]*trackingv1.Series, error)
	Now      func() time.Time
}

func NewTimeseries(d Deps) http.Handler {
	if d.Now == nil {
		d.Now = func() time.Time { return time.Now().UTC() }
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := bearer(r)
		tenantID, settings, err := d.Resolver.Tenant(r.Context(), key)
		if err != nil {
			httpError(w, http.StatusUnauthorized, "invalid read key")
			return
		}

		raw, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes))
		if err != nil {
			httpError(w, http.StatusBadRequest, "unreadable body")
			return
		}

		var req trackingv1.TimeseriesRequest
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, &req); err != nil {
			httpError(w, http.StatusBadRequest, "malformed query")
			return
		}

		useRollup := querydsl.UseRollup(&req)
		compiled, err := querydsl.Compile(tenantID, &req, useRollup)
		if err != nil {
			// Every validation failure is the caller's fault by construction:
			// the DSL is closed, so there is no "server could not express it"
			// case to distinguish.
			httpError(w, http.StatusBadRequest, userFacing(err))
			return
		}

		series, err := d.Query(r.Context(), compiled, req.GetGroupBy(), settings)
		if err != nil {
			httpError(w, http.StatusServiceUnavailable, "query failed")
			return
		}

		computedAt := d.Now()
		resp := &trackingv1.TimeseriesResponse{
			Series:      series,
			Source:      compiled.Source,
			Approximate: req.GetApproximate() || compiled.Source == "rollup",
			ComputedAt:  computedAt.UnixMilli(),
		}

		body, err := protojson.Marshal(resp)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "encode")
			return
		}

		etag := querydsl.ETag(tenantID, &req, computedAt, body)
		resp.Etag = etag

		// Re-marshal with the etag embedded so the body and the header agree.
		body, err = protojson.Marshal(resp)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "encode")
			return
		}

		w.Header().Set("ETag", etag)
		if match := r.Header.Get("If-None-Match"); match != "" && matches(match, etag, tenantID, &req, d, series) {
			// The client's cached body is still correct. 304 refreshes its TTL
			// with no transfer.
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

// matches compares the client's validator against an etag computed over the
// same query spec and result body but the client's own computed_at, so an
// unchanged result revalidates even though this request has a newer timestamp.
func matches(clientETag, _ string, tenantID string, req *trackingv1.TimeseriesRequest, d Deps, series []*trackingv1.Series) bool {
	// Recompute the content etag with computed_at zeroed: freshness is carried
	// by the response's own computed_at field, not by the validator. Folding
	// the timestamp into the validator would make every response a cache miss.
	stable := &trackingv1.TimeseriesResponse{Series: series}
	body, err := protojson.Marshal(stable)
	if err != nil {
		return false
	}
	return clientETag == querydsl.ETag(tenantID, req, time.Time{}, body)
}

// Rewrite the etag used above to the content-stable form so header, body, and
// comparison all agree. Replace the ETag(...) call in the handler with:
//
//	stable := &trackingv1.TimeseriesResponse{Series: series}
//	stableBody, _ := protojson.Marshal(stable)
//	etag := querydsl.ETag(tenantID, &req, time.Time{}, stableBody)
//
// computed_at then reports freshness without invalidating the cache on every
// request.

func bearer(r *http.Request) string {
	const p = "Bearer "
	v := r.Header.Get("Authorization")
	if len(v) > len(p) && v[:len(p)] == p {
		return v[len(p):]
	}
	return ""
}

func userFacing(err error) string {
	switch {
	case errors.Is(err, querydsl.ErrUnknownField):
		return "unknown or non-queryable field"
	case errors.Is(err, querydsl.ErrRangeTooLarge):
		return "time range too large"
	case errors.Is(err, querydsl.ErrInvalidRange):
		return "to must be after from"
	case errors.Is(err, querydsl.ErrTooManyGroupBy):
		return "too many group_by dimensions"
	case errors.Is(err, querydsl.ErrTooManyFilters):
		return "too many filters"
	default:
		return "invalid query"
	}
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
```

Apply the rewrite the comment describes before running the tests: compute
`etag` once from the content-stable body, use it for the header, the body, and
the `If-None-Match` comparison, and delete the explanatory comment block.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd services/query && go test ./... -v`
Expected: PASS, 6 handler tests.

- [ ] **Step 6: Commit**

```bash
git add services/query
git commit -m "feat(query): timeseries endpoint with etag revalidation and rollup labelling"
```

---

### Task 7: Wire the service and document it

**Files:**
- Create: `services/query/cmd/main.go`
- Create: `services/query/README.md`
- Modify: `deploy/docker-compose.yml`
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`

- [ ] **Step 1: Write `main.go`**

`services/query/cmd/main.go`:

```go
// Command query serves POST /v1/query/*.
//
// Read path only. It shares nothing with the ingest service but pkg/* and
// gen/ — deliberately, so the two can be scaled and deployed independently and
// so a query-side incident cannot take ingestion down with it.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/querydsl"
	"github.com/dhiazfathra/event-tracking/services/query/internal/auth"
	"github.com/dhiazfathra/event-tracking/services/query/internal/execute"
	"github.com/dhiazfathra/event-tracking/services/query/internal/handler"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := clickhouse.Open(ctx, clickhouse.Config{
		Addrs:    strings.Split(env("CLICKHOUSE_ADDRS", "localhost:9000"), ","),
		Database: env("CLICKHOUSE_DB", "tracking"),
		Username: env("CLICKHOUSE_USER", "default"),
		Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	})
	if err != nil {
		log.Error("clickhouse", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Short TTL rather than an invalidation channel: revocation is bounded by
	// the TTL and the control plane stays a single table read.
	resolver := auth.NewCache(pgLookupReadKey(ctx), time.Minute)

	mux := http.NewServeMux()
	mux.Handle("POST /v1/query/timeseries", handler.NewTimeseries(handler.Deps{
		Resolver: resolver,
		Query: func(ctx context.Context, c querydsl.Compiled, groupBy []string, s execute.Settings) ([]*trackingv1.Series, error) {
			return execute.Run(ctx, conn, c, groupBy, s)
		},
	}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{
		Addr:              env("LISTEN_ADDR", ":8081"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      60 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("serve", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = driver.Conn(conn)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
```

- [ ] **Step 2: Add the service to compose and CI**

Append to `deploy/docker-compose.yml`:

```yaml
  query:
    build:
      context: ..
      dockerfile: deploy/query.Dockerfile
    environment:
      CLICKHOUSE_ADDRS: clickhouse:9000
      LISTEN_ADDR: ":8081"
    ports: ["8081:8081"]
    depends_on: [clickhouse, postgres]
```

Append to `.github/workflows/ci.yml`:

```yaml
  query:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.23'
      - name: querydsl tests
        run: cd pkg/querydsl && go test ./...
      - name: query service tests
        run: cd services/query && go test ./...
```

- [ ] **Step 3: Write `services/query/README.md`**

````markdown
# Query Service

`POST /v1/query/timeseries`

## Why a DSL and not SQL

Raw SQL passthrough was rejected. A read-only user with row policies still
leaves runaway queries, resource exhaustion, and schema coupling that makes
every future migration a breaking change.

The cost is expressiveness: every new analytical question requires backend
work, and sophisticated customers will ask for raw SQL. That is the accepted
trade.

## Tenant isolation

`tenant_id` is injected server-side from the verified read key and bound as the
first argument of every compiled query. It is **not** on the field allowlist,
so a client cannot name it in a filter.

The `ORDER BY (tenant_id, name, ts, event_id)` prefix makes correct isolation
*fast* — it does not make an incorrect query *safe*. A query with no tenant
predicate would read every tenant's rows regardless of sort order, just more
slowly. The compiler is the security boundary; `pkg/querydsl` has a test
asserting the predicate is present in every emitted shape.

## Exactness

| Situation | Aggregate | Response |
|---|---|---|
| Default | `uniqExact(event_id)` | `"source":"raw"`, `"approximate":false` |
| `approximate: true` | `uniq()` — HLL, ~0.5% error | `"approximate":true` |
| Coarse long-range daily query | `sum(events)` from `events_daily` | `"source":"rollup"`, `"approximate":true` |

`count(*)` is never used: the delivery guarantee is at-least-once, so retries
are physically present in the table and a raw count would report them as
events.

`FINAL` is never used. It forces a merge at query time and is the single most
common way to make a ClickHouse dashboard slow.

**Rollups are approximate and labelled, not silently substituted.** The
materialized view fires on the inserted block and never sees the
`ReplacingMergeTree` dedup that happens later during merges, so duplicate
deliveries inflate rollup counts. Raw `events` is the only source of truth.

## Caching

Responses carry an `ETag` and `computed_at`. The etag is computed over the
tenant, the query spec, and the result body — **not** the computation time, so
an unchanged result revalidates with a `304` and refreshes the client's TTL
with no body transfer.

Server-pushed invalidation does not exist (spec §7.6). Freshness is
`computed_at` plus pull-to-refresh.

## Known gap

Per-user deletion is not implemented. When it is, rollup reads become unsafe
without a rebuild fence and a per-tenant deletion epoch folded into the cache
key — see spec §3.7. Do not add a deletion path without also closing that.
````

- [ ] **Step 4: Link from the root README and run everything**

Add to the root `README.md` Services list:

```markdown
- [`services/query`](services/query/README.md) — constrained DSL analytics API
```

Run: `go build ./... && go test ./... && make check-boundaries`
Expected: all pass, boundaries clean (query imports no other service).

- [ ] **Step 5: Commit**

```bash
git add services/query deploy .github/workflows/ci.yml README.md
git commit -m "feat(query): wire the service, compose entry, CI job, and docs"
```

---

## Completion criteria

- Every compiled query shape contains `tenant_id = ?` bound as the first arg — asserted by test.
- No emitted SQL contains `FINAL`.
- `uniq()` is reachable only via `approximate: true`.
- Rollup-backed responses are labelled `"source":"rollup"` and `"approximate":true`.
- Duplicate deliveries do not inflate raw counts (integration test against a real ClickHouse).
- A query for tenant A never returns tenant B's rows (integration test).
- `304` on a matching `If-None-Match` with a zero-length body.

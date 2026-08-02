# Ingest Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `services/ingest` — the token exchange, the `POST /v1/batch` endpoint, per-event validation, trust-tier enrichment, clock-skew correction, Redis quota, and the ClickHouse async insert path.

**Architecture:** Two endpoints. `POST /v1/auth/token` exchanges a public client ID plus a platform attestation for a short-lived EdDSA JWT carrying `tenant_id`, server-issued `install_id`, `scope=write:events`, and `trust_tier`. `POST /v1/batch` verifies that JWT against a cached JWKS (no shared secret on the hot path), validates each event independently, corrects clock skew against a per-session offset, checks a Redis quota, and inserts to ClickHouse with `async_insert=1, wait_for_async_insert=1` so a `200` means the data was flushed. Every response is partial-success shaped: one bad event must never poison a batch.

**Tech Stack:** Go 1.23+, `net/http` (stdlib routing — Go 1.22 patterns cover this), `github.com/ClickHouse/clickhouse-go/v2`, `github.com/redis/go-redis/v9`, `github.com/jackc/pgx/v5`, `github.com/lestrrat-go/jwx/v3` (JWT + JWKS), `github.com/testcontainers/testcontainers-go`.

## Global Constraints

Copied verbatim from the spec. Every task's requirements implicitly include this section.

- `tenant_id` comes **only** from the verified JWT's `tenant_id` claim — never from a request body, never from a hot-path database lookup. A tenant field appearing in a request body is rejected as unknown, never preferred over the claim.
- `install_id` is **server-issued at the exchange, never client-supplied.**
- JWT acceptance requires **all** of: `alg` is `EdDSA` (checked against an allowlist **before** verification; `none` and all HMAC algorithms rejected outright, never inferred from the token header), `typ` is `at+jwt`, `iss` matches the platform issuer exactly, `aud` matches the ingest audience exactly, `exp`/`nbf` valid within ≤60s skew, `scope` contains `write:events`, and `kid` resolves in the JWKS to a key marked `use: "sig"` with `kty: "OKP"` / `crv: "Ed25519"`.
- A `kid` miss triggers **at most one rate-limited JWKS refetch**.
- Attestation runs at the **exchange**, not at ingest. Failure assigns Tier 1, it does not block.
- Tier 1 gets **tighter rate limits, not sampling.** Sampling is forbidden: the client deletes the outbox row on `200`, so a silently discarded event has no retry and no counter.
- Rate limits key on `install_id` first, tenant second, IP last. IP-primary limiting is actively harmful (CGNAT).
- Clock offset is computed **once per `(tenant_id, device_id, session_id)`** and persisted for the session. Recomputing per-request would move rows under the sort key and defeat `ReplacingMergeTree`.
- Skew clamp: corrected timestamps >24h future or >30d past are clamped to `received_at` and flagged.
- ClickHouse settings: `async_insert=1`, `wait_for_async_insert=1`, `async_insert_busy_timeout_ms=1000`, `async_insert_max_data_size=10000000`.
- **No in-process buffering in the ingest service.** `wait_for_async_insert=1` is the point: the `200` means durable.
- `FINAL` is never used.
- Batch caps: ≤500 events, ≤1 MB decompressed, ≤50 props per event.
- `event_id` stays client-generated. The server stamps `ts`, `ts_received`, `trust_tier`, `install_id` — it never mints an event ID.
- Legacy `wk_live_...` write keys: dual-accept → rate-limited deprecation → per-tenant config-flag cutoff. Never a deploy-gated cutoff.

---

## File Structure

| File | Responsibility |
|---|---|
| `migrations/clickhouse/0001_events.sql` | `events` table |
| `migrations/postgres/0001_control_plane.sql` | tenants, client_ids, signing keys, quotas |
| `pkg/clickhouse/pool.go` | Connection pool + async-insert settings |
| `pkg/clickhouse/insert.go` | Typed batch insert for `events` |
| `pkg/tenant/token.go` | JWT minting (exchange side) |
| `pkg/tenant/verify.go` | JWT verification + JWKS cache (ingest side) |
| `pkg/tenant/legacy.go` | Legacy `wk_live_` dual-accept, per-tenant cutoff flag |
| `services/ingest/internal/validate/validate.go` | Per-event validation → `Reject` list |
| `services/ingest/internal/enrich/skew.go` | Per-session clock offset + clamp |
| `services/ingest/internal/enrich/enrich.go` | Envelope → storage row |
| `services/ingest/internal/quota/quota.go` | Redis counters: quota + rate limit |
| `services/ingest/internal/handler/batch.go` | `POST /v1/batch` |
| `services/ingest/internal/handler/token.go` | `POST /v1/auth/token` |
| `services/ingest/internal/attest/attest.go` | App Attest / Play Integrity verification |
| `services/ingest/cmd/main.go` | Wiring, config, listen |

---

### Task 1: ClickHouse schema and migrations

**Files:**
- Create: `migrations/clickhouse/0001_events.sql`
- Create: `pkg/clickhouse/go.mod`
- Create: `pkg/clickhouse/migrate.go`
- Test: `pkg/clickhouse/migrate_test.go` (testcontainers)

**Interfaces:**
- Consumes: nothing from earlier plans except the module layout.
- Produces:
  - `clickhouse.Migrate(ctx context.Context, conn driver.Conn, dir fs.FS) error`
  - the `events` table, exactly as specified.

A mocked driver cannot tell you whether `ReplacingMergeTree` actually collapses
your duplicates or whether your `ORDER BY` is unique. Use a container.

- [ ] **Step 1: Write the failing test**

`pkg/clickhouse/migrate_test.go`:

```go
package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
)

func TestMigrateCreatesEventsTable(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t) // helper added in Step 3

	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var engine, sortingKey string
	row := conn.QueryRow(ctx,
		`SELECT engine_full, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'events'`)
	if err := row.Scan(&engine, &sortingKey); err != nil {
		t.Fatalf("describe events: %v", err)
	}

	if want := "ReplacingMergeTree(ts_received)"; !contains(engine, want) {
		t.Errorf("engine = %q, want it to contain %q", engine, want)
	}
	if want := "tenant_id, name, ts, event_id"; sortingKey != want {
		t.Errorf("sorting_key = %q, want %q", sortingKey, want)
	}
}

// The sort key must be unique or ReplacingMergeTree collapses rows that are not
// actually duplicates. This test proves two distinct events with the same
// (tenant, name, ts) both survive a forced merge, and that a true duplicate
// (same event_id) collapses to one.
func TestReplacingMergeTreeCollapsesOnlyTrueDuplicates(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ts := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	insert := func(eventID string, received time.Time) {
		t.Helper()
		err := conn.Exec(ctx,
			`INSERT INTO events (tenant_id, event_id, name, ts, ts_client, ts_received, trust_tier, install_id, props)
			 VALUES (?, ?, ?, ?, ?, ?, 0, 'i-1', '{}')`,
			"t1", eventID, "checkout", ts, ts, received)
		if err != nil {
			t.Fatalf("insert %s: %v", eventID, err)
		}
	}

	a := "0191f4a2-1c3d-7000-8000-00000000000a"
	b := "0191f4a2-1c3d-7000-8000-00000000000b"
	insert(a, ts)
	insert(a, ts.Add(time.Second)) // true duplicate, later version wins
	insert(b, ts)                  // distinct event, same tenant/name/ts

	if err := conn.Exec(ctx, `OPTIMIZE TABLE events FINAL`); err != nil {
		t.Fatalf("optimize: %v", err)
	}

	var n uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM events`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("row count after merge = %d, want 2 (one collapsed duplicate, two distinct events)", n)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/clickhouse && go test ./...`
Expected: FAIL — `undefined: clickhouse.Migrate`, `undefined: startClickHouse`.

- [ ] **Step 3: Write the migration and the migrator**

```bash
mkdir -p pkg/clickhouse migrations/clickhouse
cd pkg/clickhouse && go mod init github.com/dhiazfathra/event-tracking/pkg/clickhouse
go get github.com/ClickHouse/clickhouse-go/v2@v2.30.0
go get github.com/testcontainers/testcontainers-go@v0.34.0
go get github.com/testcontainers/testcontainers-go/modules/clickhouse@v0.34.0
cd ../..
```

`migrations/clickhouse/0001_events.sql`:

```sql
CREATE TABLE IF NOT EXISTS events
(
    tenant_id     LowCardinality(String),
    event_id      UUID,
    name          LowCardinality(String),

    ts            DateTime64(3, 'UTC'),   -- skew-corrected. Queries use this.
    ts_client     DateTime64(3, 'UTC'),   -- raw device clock, kept for forensics
    ts_received   DateTime64(3, 'UTC'),   -- server receipt; ReplacingMergeTree version
    event_date    Date MATERIALIZED toDate(ts),

    user_id       String,
    anonymous_id  String,
    device_id     String,
    session_id    String,
    seq           UInt64,

    app_version   LowCardinality(String),
    sdk_version   LowCardinality(String),
    os            LowCardinality(String),
    os_version    LowCardinality(String),
    locale        LowCardinality(String),

    trust_tier    UInt8,
    install_id    String,

    props         JSON,

    INDEX idx_user  user_id    TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_sess  session_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ts_received)
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, name, ts, event_id)
TTL event_date + INTERVAL 13 MONTH DELETE
SETTINGS index_granularity = 8192;
```

`pkg/clickhouse/migrate.go`:

```go
// Package clickhouse holds the shared ClickHouse pool, migration runner, and
// insert path. Forward-only migrations: there is no down-migration story,
// because rolling a schema backwards on a 2.6B-row/month table is a fantasy.
package clickhouse

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed all:sql
var embedded embed.FS

// Migrations is the embedded migration set. It is a package-level var so tests
// can substitute a fixture FS.
var Migrations fs.FS = mustSub(embedded, "sql")

func mustSub(f embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

// Migrate applies every .sql file in dir in lexical order. Each file may hold
// multiple statements separated by ";". Statements are idempotent by
// convention (CREATE TABLE IF NOT EXISTS), so re-running is safe and there is
// no version table to drift out of sync with reality.
func Migrate(ctx context.Context, conn driver.Conn, dir fs.FS) error {
	entries, err := fs.ReadDir(dir, ".")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		body, err := fs.ReadFile(dir, name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		for _, stmt := range strings.Split(string(body), ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	return nil
}
```

Symlink or copy the migration directory so the embed works:

```bash
mkdir -p pkg/clickhouse/sql && cp migrations/clickhouse/*.sql pkg/clickhouse/sql/
```

Add a Makefile rule so the copy cannot go stale:

```makefile
sync-migrations:
	rm -rf pkg/clickhouse/sql && mkdir -p pkg/clickhouse/sql
	cp migrations/clickhouse/*.sql pkg/clickhouse/sql/
	git diff --exit-code -- pkg/clickhouse/sql
```

- [ ] **Step 4: Write the testcontainers helper**

`pkg/clickhouse/testmain_test.go`:

```go
package clickhouse_test

import (
	"context"
	"testing"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/testcontainers/testcontainers-go"
	tcch "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// startClickHouse boots a throwaway ClickHouse and returns a live connection.
// One container per test: these are fast, and shared state between schema tests
// is how you get a green suite that passes only in the order you happened to
// run it.
func startClickHouse(t *testing.T) driver.Conn {
	t.Helper()
	ctx := context.Background()

	container, err := tcch.Run(ctx, "clickhouse/clickhouse-server:24.8-alpine",
		tcch.WithUsername("default"),
		tcch.WithPassword(""),
		tcch.WithDatabase("tracking_test"),
	)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	host, err := container.ConnectionHost(ctx)
	if err != nil {
		t.Fatalf("connection host: %v", err)
	}

	conn, err := ch.Open(&ch.Options{
		Addr: []string{host},
		Auth: ch.Auth{Database: "tracking_test", Username: "default"},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return conn
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd pkg/clickhouse && go test ./... -v`
Expected: PASS — `TestMigrateCreatesEventsTable`, `TestReplacingMergeTreeCollapsesOnlyTrueDuplicates`.

- [ ] **Step 6: Commit**

```bash
git add migrations/clickhouse pkg/clickhouse Makefile go.work
git commit -m "feat(clickhouse): add events schema and forward-only migrator"
```

---

### Task 2: ClickHouse pool with async-insert settings

**Files:**
- Create: `pkg/clickhouse/pool.go`
- Create: `pkg/clickhouse/insert.go`
- Test: `pkg/clickhouse/insert_test.go`

**Interfaces:**
- Consumes: `Migrate` from Task 1.
- Produces:
  - `type Config struct { Addrs []string; Database, Username, Password string }`
  - `func Open(ctx context.Context, cfg Config) (driver.Conn, error)`
  - `type Row struct { TenantID, EventID, Name string; TS, TSClient, TSReceived time.Time; UserID, AnonymousID, DeviceID, SessionID string; Seq uint64; AppVersion, SDKVersion, OS, OSVersion, Locale string; TrustTier uint8; InstallID string; Props string }`
  - `func InsertEvents(ctx context.Context, conn driver.Conn, rows []Row) error`

- [ ] **Step 1: Write the failing test**

`pkg/clickhouse/insert_test.go`:

```go
package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
)

func TestInsertEventsIsDurableOnReturn(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	rows := []clickhouse.Row{{
		TenantID:   "t1",
		EventID:    "0191f4a2-1c3d-7000-8000-00000000000a",
		Name:       "checkout",
		TS:         now,
		TSClient:   now,
		TSReceived: now,
		UserID:     "u1",
		Seq:        1,
		TrustTier:  0,
		InstallID:  "i-1",
		Props:      `{"amount":19.99}`,
	}}

	if err := clickhouse.InsertEvents(ctx, conn, rows); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// wait_for_async_insert=1 means the data is queryable the moment
	// InsertEvents returns. No sleep, no retry loop. If this test ever needs a
	// sleep to pass, the settings are wrong and the 200 is a lie.
	var n uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM events WHERE tenant_id = 't1'`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("count = %d, want 1 immediately after insert returns", n)
	}
}

func TestInsertEventsEmptyIsNoop(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := clickhouse.InsertEvents(ctx, conn, nil); err != nil {
		t.Errorf("empty insert returned %v, want nil", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/clickhouse && go test ./... -run Insert`
Expected: FAIL — `undefined: clickhouse.InsertEvents`.

- [ ] **Step 3: Write the pool**

`pkg/clickhouse/pool.go`:

```go
package clickhouse

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Config struct {
	Addrs    []string
	Database string
	Username string
	Password string
}

// Open returns a pooled connection configured for the ingest write path.
//
// The async-insert settings are the load-bearing part. Many stateless ingest
// pods each writing small batches would otherwise produce thousands of tiny
// parts per minute and push the table into "too many parts". ClickHouse
// coalesces them server-side instead.
//
// wait_for_async_insert=1 costs up to ~1s of p99 latency and buys the right to
// say a 200 means the data is flushed. The client deletes its outbox row on
// 200, so acking before the flush would be silent data loss.
func Open(ctx context.Context, cfg Config) (driver.Conn, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: cfg.Addrs,
		Auth: ch.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: ch.Settings{
			"async_insert":                  1,
			"wait_for_async_insert":         1,
			"async_insert_busy_timeout_ms":  1000,
			"async_insert_max_data_size":    10000000,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    32,
		MaxIdleConns:    8,
		ConnMaxLifetime: 10 * time.Minute,
		Compression:     &ch.Compression{Method: ch.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return conn, nil
}
```

- [ ] **Step 4: Write the insert path**

`pkg/clickhouse/insert.go`:

```go
package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Row is the storage shape of an event. It is deliberately not the proto type:
// the wire envelope has no tenant_id, no trust_tier, and no install_id, and
// conflating the two is how a client-supplied tenant field sneaks into storage.
type Row struct {
	TenantID   string
	EventID    string
	Name       string
	TS         time.Time
	TSClient   time.Time
	TSReceived time.Time

	UserID      string
	AnonymousID string
	DeviceID    string
	SessionID   string
	Seq         uint64

	AppVersion string
	SDKVersion string
	OS         string
	OSVersion  string
	Locale     string

	TrustTier uint8
	InstallID string

	Props string // JSON object
}

// InsertEvents writes rows and does not return until ClickHouse has flushed
// them (wait_for_async_insert=1 on the connection). There is deliberately no
// in-process accumulator: a Go-side buffer would create a window where the
// service has acked events it has not written, losing them on a pod restart
// after the client already deleted its copy.
func InsertEvents(ctx context.Context, conn driver.Conn, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO events (
		tenant_id, event_id, name, ts, ts_client, ts_received,
		user_id, anonymous_id, device_id, session_id, seq,
		app_version, sdk_version, os, os_version, locale,
		trust_tier, install_id, props)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, r := range rows {
		if err := batch.Append(
			r.TenantID, r.EventID, r.Name, r.TS, r.TSClient, r.TSReceived,
			r.UserID, r.AnonymousID, r.DeviceID, r.SessionID, r.Seq,
			r.AppVersion, r.SDKVersion, r.OS, r.OSVersion, r.Locale,
			r.TrustTier, r.InstallID, r.Props,
		); err != nil {
			return fmt.Errorf("append %s: %w", r.EventID, err)
		}
	}
	return batch.Send()
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd pkg/clickhouse && go test ./... -v`
Expected: PASS, 4 tests.

- [ ] **Step 6: Commit**

```bash
git add pkg/clickhouse
git commit -m "feat(clickhouse): add pool with async-insert settings and typed insert"
```

---

### Task 3: Control plane schema and token minting

**Files:**
- Create: `migrations/postgres/0001_control_plane.sql`
- Create: `pkg/tenant/go.mod`
- Create: `pkg/tenant/token.go`
- Test: `pkg/tenant/token_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type Claims struct { TenantID, InstallID, Scope string; TrustTier uint8 }`
  - `type Minter struct{ ... }`
  - `func NewMinter(kid string, priv ed25519.PrivateKey, issuer, audience string, ttl time.Duration) *Minter`
  - `func (m *Minter) Mint(c Claims, now time.Time) (token string, expiresIn int64, err error)`

- [ ] **Step 1: Write the failing test**

`pkg/tenant/token_test.go`:

```go
package tenant_test

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

func TestMintProducesRequiredClaims(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	_ = pub

	m := tenant.NewMinter("kid-1", priv, "https://issuer.example", "https://ingest.example", 45*time.Minute)
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	raw, expiresIn, err := m.Mint(tenant.Claims{
		TenantID:  "t1",
		InstallID: "i-1",
		Scope:     "write:events",
		TrustTier: 0,
	}, now)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if expiresIn != 2700 {
		t.Errorf("expiresIn = %d, want 2700", expiresIn)
	}

	// Parse without verification to inspect claims; verification is Task 4.
	tok, err := jwt.Parse([]byte(raw), jwt.WithVerify(false), jwt.WithValidate(false))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	var tenantID, installID, scope string
	if err := tok.Get("tenant_id", &tenantID); err != nil || tenantID != "t1" {
		t.Errorf("tenant_id = %q (err %v), want t1", tenantID, err)
	}
	if err := tok.Get("install_id", &installID); err != nil || installID != "i-1" {
		t.Errorf("install_id = %q (err %v), want i-1", installID, err)
	}
	if err := tok.Get("scope", &scope); err != nil || scope != "write:events" {
		t.Errorf("scope = %q (err %v), want write:events", scope, err)
	}
	if exp, _ := tok.Expiration(); !exp.Equal(now.Add(45 * time.Minute)) {
		t.Errorf("exp = %v, want %v", exp, now.Add(45*time.Minute))
	}
}

// typ=at+jwt and alg=EdDSA are part of the acceptance contract on the verify
// side. If the minter stops setting them, every token stops being accepted.
func TestMintSetsHeaderTypAndAlg(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	m := tenant.NewMinter("kid-1", priv, "iss", "aud", time.Hour)

	raw, _, err := m.Mint(tenant.Claims{TenantID: "t1", InstallID: "i-1", Scope: "write:events"}, time.Now())
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	msg, err := jws.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("jws parse: %v", err)
	}
	hdr := msg.Signatures()[0].ProtectedHeaders()
	if got := hdr.Algorithm().String(); got != "EdDSA" {
		t.Errorf("alg = %q, want EdDSA", got)
	}
	var typ string
	if err := hdr.Get("typ", &typ); err != nil || typ != "at+jwt" {
		t.Errorf("typ = %q (err %v), want at+jwt", typ, err)
	}
	if hdr.KeyID() != "kid-1" {
		t.Errorf("kid = %q, want kid-1", hdr.KeyID())
	}
}
```

Add the `jws` import: `"github.com/lestrrat-go/jwx/v3/jws"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/tenant && go test ./...`
Expected: FAIL — `undefined: tenant.NewMinter`.

- [ ] **Step 3: Write the Postgres control-plane schema**

`migrations/postgres/0001_control_plane.sql`:

```sql
CREATE TABLE tenants (
    tenant_id    TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Per-tenant flag, not a deploy, so a stranded customer can be rolled back
    -- to dual-accept without a release.
    legacy_key_mode TEXT NOT NULL DEFAULT 'dual_accept'
        CHECK (legacy_key_mode IN ('dual_accept', 'deprecating', 'cutoff'))
);

-- The identifier embedded in the SDK. Public: anything shipped inside a third
-- party's mobile binary is extractable. It identifies a tenant and authorizes
-- nothing.
CREATE TABLE client_ids (
    client_id   TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    revoked_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX client_ids_tenant ON client_ids(tenant_id);

-- Read keys for the query API. Separate credential, separate scope.
CREATE TABLE read_keys (
    key_hash    BYTEA PRIMARY KEY,       -- sha256 of the key; never store the key
    tenant_id   TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    revoked_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ed25519 signing keys. Rotation overlaps old and new so in-flight tokens stay
-- verifiable.
CREATE TABLE signing_keys (
    kid          TEXT PRIMARY KEY,
    public_key   BYTEA NOT NULL,
    private_key  BYTEA NOT NULL,        -- encrypted at rest by the deployment
    active       BOOLEAN NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at   TIMESTAMPTZ
);

CREATE TABLE quotas (
    tenant_id        TEXT PRIMARY KEY REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    daily_events     BIGINT NOT NULL,
    rps_tier0        INTEGER NOT NULL DEFAULT 50,
    rps_tier1        INTEGER NOT NULL DEFAULT 10,   -- tighter, never sampled
    rps_legacy       INTEGER NOT NULL DEFAULT 5     -- below tier 1, deprecation pressure
);

-- Server-issued. Never client-supplied: a client able to choose or rotate its
-- install_id could reset its own rate limit at will.
CREATE TABLE installs (
    install_id     TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    platform       TEXT NOT NULL,
    trust_tier     SMALLINT NOT NULL,
    attest_subject TEXT,                -- derived from the attestation at tier 0
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, attest_subject)
);

-- Per-session clock offset. Persisted so a retry gets the same ts as the
-- original: recomputing per-request would move the row under the sort key and
-- stop ReplacingMergeTree from ever collapsing the duplicate.
CREATE TABLE session_offsets (
    tenant_id   TEXT NOT NULL,
    device_id   TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    offset_ms   BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, device_id, session_id)
);
```

- [ ] **Step 4: Write the minter**

```bash
mkdir -p pkg/tenant && cd pkg/tenant && go mod init github.com/dhiazfathra/event-tracking/pkg/tenant
go get github.com/lestrrat-go/jwx/v3@v3.0.0
cd ../..
```

`pkg/tenant/token.go`:

```go
// Package tenant owns tenant identity: minting short-lived ingest tokens,
// verifying them, and the legacy write-key cutover.
package tenant

import (
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

// TokenType is the required `typ` header. RFC 9068. Verification rejects
// anything else, which stops an ID token or a refresh token from being
// replayed at the ingest endpoint.
const TokenType = "at+jwt"

// ScopeWriteEvents means exactly that: no reads, no enumeration, no
// cross-tenant. The worst case of a fully abused pipeline is cost plus data
// pollution, never exfiltration.
const ScopeWriteEvents = "write:events"

type Claims struct {
	TenantID  string
	InstallID string
	Scope     string
	TrustTier uint8
}

type Minter struct {
	kid      string
	priv     ed25519.PrivateKey
	issuer   string
	audience string
	ttl      time.Duration
}

func NewMinter(kid string, priv ed25519.PrivateKey, issuer, audience string, ttl time.Duration) *Minter {
	return &Minter{kid: kid, priv: priv, issuer: issuer, audience: audience, ttl: ttl}
}

// Mint issues a short-lived ingest token. The token, not the embedded client
// ID, is the actual credential — which is what makes rotation an
// incident-response lever rather than an abuse control.
func (m *Minter) Mint(c Claims, now time.Time) (string, int64, error) {
	tok, err := jwt.NewBuilder().
		Issuer(m.issuer).
		Audience([]string{m.audience}).
		IssuedAt(now).
		NotBefore(now).
		Expiration(now.Add(m.ttl)).
		Claim("tenant_id", c.TenantID).
		Claim("install_id", c.InstallID).
		Claim("scope", c.Scope).
		Claim("trust_tier", int(c.TrustTier)).
		Build()
	if err != nil {
		return "", 0, fmt.Errorf("build token: %w", err)
	}

	hdrs := jws.NewHeaders()
	if err := hdrs.Set("typ", TokenType); err != nil {
		return "", 0, err
	}
	if err := hdrs.Set(jws.KeyIDKey, m.kid); err != nil {
		return "", 0, err
	}

	key, err := jwk.Import(m.priv)
	if err != nil {
		return "", 0, fmt.Errorf("import key: %w", err)
	}

	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.EdDSA(), key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		return "", 0, fmt.Errorf("sign: %w", err)
	}
	return string(signed), int64(m.ttl.Seconds()), nil
}
```

Add imports `"github.com/lestrrat-go/jwx/v3/jwk"` and `"github.com/lestrrat-go/jwx/v3/jws"`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd pkg/tenant && go test ./... -v`
Expected: PASS, 2 tests.

- [ ] **Step 6: Commit**

```bash
git add migrations/postgres pkg/tenant go.work
git commit -m "feat(tenant): add control-plane schema and EdDSA token minting"
```

---

### Task 4: JWT verification and JWKS cache

**Files:**
- Create: `pkg/tenant/verify.go`
- Test: `pkg/tenant/verify_test.go`

**Interfaces:**
- Consumes: `Claims`, `TokenType`, `ScopeWriteEvents` from Task 3.
- Produces:
  - `type Verifier struct{ ... }`
  - `func NewVerifier(jwksURL, issuer, audience string, httpClient *http.Client) *Verifier`
  - `func (v *Verifier) Verify(ctx context.Context, bearer string, now time.Time) (Claims, error)`
  - Sentinel errors: `ErrBadAlg`, `ErrBadType`, `ErrBadIssuer`, `ErrBadAudience`, `ErrExpired`, `ErrBadScope`, `ErrUnknownKID`.

This is the security-critical task in the plan. The tests below are not
optional coverage — each one corresponds to a specific bypass.

- [ ] **Step 1: Write the failing test**

`pkg/tenant/verify_test.go`:

```go
package tenant_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

const (
	testIssuer   = "https://issuer.example"
	testAudience = "https://ingest.example"
)

type jwksServer struct {
	*httptest.Server
	hits int
}

func newJWKS(t *testing.T, kid string, pub ed25519.PublicKey, use, kty string) *jwksServer {
	t.Helper()
	key, err := jwk.Import(pub)
	if err != nil {
		t.Fatalf("import pub: %v", err)
	}
	_ = key.Set(jwk.KeyIDKey, kid)
	_ = key.Set(jwk.KeyUsageKey, use)
	set := jwk.NewSet()
	_ = set.AddKey(key)

	s := &jwksServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(s.Close)
	return s
}

func mint(t *testing.T, priv ed25519.PrivateKey, kid string, mutate func(*jwt.Builder)) string {
	t.Helper()
	now := time.Now()
	b := jwt.NewBuilder().
		Issuer(testIssuer).
		Audience([]string{testAudience}).
		IssuedAt(now).NotBefore(now).Expiration(now.Add(time.Hour)).
		Claim("tenant_id", "t1").
		Claim("install_id", "i-1").
		Claim("scope", tenant.ScopeWriteEvents).
		Claim("trust_tier", 0)
	if mutate != nil {
		mutate(b)
	}
	tok, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	hdrs := jws.NewHeaders()
	_ = hdrs.Set("typ", tenant.TokenType)
	_ = hdrs.Set(jws.KeyIDKey, kid)
	key, _ := jwk.Import(priv)
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.EdDSA(), key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return string(signed)
}

func TestVerifyHappyPath(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	claims, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), time.Now())
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.TenantID != "t1" || claims.InstallID != "i-1" || claims.TrustTier != 0 {
		t.Errorf("claims = %+v, want tenant t1 / install i-1 / tier 0", claims)
	}
}

// alg=none is the oldest JWT bypass there is. The allowlist must be consulted
// before verification, never inferred from the token's own header.
func TestVerifyRejectsAlgNone(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	// Hand-built unsigned token: header {"alg":"none","typ":"at+jwt","kid":"kid-1"}
	unsigned := "eyJhbGciOiJub25lIiwidHlwIjoiYXQrand0Iiwia2lkIjoia2lkLTEifQ." +
		"eyJ0ZW5hbnRfaWQiOiJ0MSJ9."

	if _, err := v.Verify(context.Background(), unsigned, time.Now()); !errors.Is(err, tenant.ErrBadAlg) {
		t.Errorf("err = %v, want ErrBadAlg", err)
	}
}

// An HMAC token signed with the *public* key as the secret is the classic
// confusion attack against an asymmetric verifier.
func TestVerifyRejectsHMACAlgorithm(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	tok, _ := jwt.NewBuilder().Issuer(testIssuer).Claim("tenant_id", "t1").Build()
	hdrs := jws.NewHeaders()
	_ = hdrs.Set("typ", tenant.TokenType)
	_ = hdrs.Set(jws.KeyIDKey, "kid-1")
	hmacKey, _ := jwk.Import([]byte(pub))
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256(), hmacKey, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatalf("sign hmac: %v", err)
	}

	if _, err := v.Verify(context.Background(), string(signed), time.Now()); !errors.Is(err, tenant.ErrBadAlg) {
		t.Errorf("err = %v, want ErrBadAlg", err)
	}
}

// A valid signature alone does not bind a token to this endpoint. iss/aud are
// what stop a token minted for another audience being replayed here.
func TestVerifyRejectsWrongAudienceAndIssuer(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	wrongAud := mint(t, priv, "kid-1", func(b *jwt.Builder) { b.Audience([]string{"https://other.example"}) })
	if _, err := v.Verify(context.Background(), wrongAud, time.Now()); !errors.Is(err, tenant.ErrBadAudience) {
		t.Errorf("wrong aud: err = %v, want ErrBadAudience", err)
	}

	wrongIss := mint(t, priv, "kid-1", func(b *jwt.Builder) { b.Issuer("https://evil.example") })
	if _, err := v.Verify(context.Background(), wrongIss, time.Now()); !errors.Is(err, tenant.ErrBadIssuer) {
		t.Errorf("wrong iss: err = %v, want ErrBadIssuer", err)
	}
}

func TestVerifyRejectsMissingScope(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	tok := mint(t, priv, "kid-1", func(b *jwt.Builder) { b.Claim("scope", "read:events") })
	if _, err := v.Verify(context.Background(), tok, time.Now()); !errors.Is(err, tenant.ErrBadScope) {
		t.Errorf("err = %v, want ErrBadScope", err)
	}
}

func TestVerifyRejectsExpiredBeyondSkew(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	tok := mint(t, priv, "kid-1", nil)
	future := time.Now().Add(2 * time.Hour) // token exp was now+1h
	if _, err := v.Verify(context.Background(), tok, future); !errors.Is(err, tenant.ErrExpired) {
		t.Errorf("err = %v, want ErrExpired", err)
	}
}

// A key published for encryption, or of the wrong type, must be rejected even
// if the signature would verify.
func TestVerifyRejectsKeyNotMarkedForSigning(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "enc", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), time.Now()); !errors.Is(err, tenant.ErrUnknownKID) {
		t.Errorf("err = %v, want ErrUnknownKID", err)
	}
}

// A forged kid must not be able to drive unbounded JWKS traffic. One refetch,
// rate-limited, then the answer is cached-miss.
func TestVerifyForgedKIDRefetchesAtMostOnce(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	// Prime the cache.
	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), time.Now()); err != nil {
		t.Fatalf("prime: %v", err)
	}
	before := js.hits

	for i := 0; i < 20; i++ {
		_, _ = v.Verify(context.Background(), mint(t, priv, "forged-kid", nil), time.Now())
	}

	if extra := js.hits - before; extra > 1 {
		t.Errorf("JWKS refetches for forged kid = %d, want at most 1", extra)
	}
}
```

Add the `encoding/json` import.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/tenant && go test ./... -run Verify`
Expected: FAIL — `undefined: tenant.NewVerifier`.

- [ ] **Step 3: Write the verifier**

`pkg/tenant/verify.go`:

```go
package tenant

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

var (
	ErrBadAlg      = errors.New("tenant: unsupported token algorithm")
	ErrBadType     = errors.New("tenant: unsupported token type")
	ErrBadIssuer   = errors.New("tenant: issuer mismatch")
	ErrBadAudience = errors.New("tenant: audience mismatch")
	ErrExpired     = errors.New("tenant: token expired or not yet valid")
	ErrBadScope    = errors.New("tenant: token lacks write:events scope")
	ErrUnknownKID  = errors.New("tenant: kid not found in JWKS")
	ErrMalformed   = errors.New("tenant: malformed token")
)

// clockSkew is the tolerance on exp/nbf. Kept tight: this is server-to-server
// time drift, not the multi-year mobile clock skew handled in enrich.
const clockSkew = 60 * time.Second

// jwksMinRefetch rate-limits refetches so forged kid values cannot drive
// unbounded traffic to the JWKS endpoint.
const jwksMinRefetch = 30 * time.Second

// jwksTTL bounds how long a cached key set is trusted. Rotation overlaps old
// and new keys so in-flight tokens stay verifiable across a rotation.
const jwksTTL = 10 * time.Minute

type Verifier struct {
	url      string
	issuer   string
	audience string
	client   *http.Client

	mu          sync.Mutex
	set         jwk.Set
	fetchedAt   time.Time
	lastRefetch time.Time
}

func NewVerifier(jwksURL, issuer, audience string, client *http.Client) *Verifier {
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	return &Verifier{url: jwksURL, issuer: issuer, audience: audience, client: client}
}

// Verify checks every acceptance condition and returns the claims. Ingest is a
// hot path: this does signature verification and claim checks only — no
// database lookup, no attestation. Attestation happened at the exchange.
func (v *Verifier) Verify(ctx context.Context, bearer string, now time.Time) (Claims, error) {
	raw := []byte(strings.TrimPrefix(bearer, "Bearer "))

	msg, err := jws.Parse(raw)
	if err != nil || len(msg.Signatures()) != 1 {
		return Claims{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	hdr := msg.Signatures()[0].ProtectedHeaders()

	// Algorithm allowlist, checked BEFORE verification and never inferred from
	// the header. "none" and every HMAC family are rejected outright here, so
	// they never reach a code path that could be tricked into using the public
	// key as a shared secret.
	if hdr.Algorithm().String() != jwa.EdDSA().String() {
		return Claims{}, fmt.Errorf("%w: %s", ErrBadAlg, hdr.Algorithm())
	}

	var typ string
	if err := hdr.Get("typ", &typ); err != nil || typ != TokenType {
		return Claims{}, fmt.Errorf("%w: %q", ErrBadType, typ)
	}

	kid := hdr.KeyID()
	key, err := v.key(ctx, kid, now)
	if err != nil {
		return Claims{}, err
	}

	tok, err := jwt.Parse(raw,
		jwt.WithKey(jwa.EdDSA(), key),
		jwt.WithValidate(false), // claim checks are explicit below
	)
	if err != nil {
		return Claims{}, fmt.Errorf("%w: signature: %v", ErrMalformed, err)
	}

	if iss, ok := tok.Issuer(); !ok || iss != v.issuer {
		return Claims{}, fmt.Errorf("%w: %q", ErrBadIssuer, iss)
	}
	aud, _ := tok.Audience()
	if !containsString(aud, v.audience) {
		return Claims{}, fmt.Errorf("%w: %v", ErrBadAudience, aud)
	}
	if exp, ok := tok.Expiration(); !ok || now.After(exp.Add(clockSkew)) {
		return Claims{}, ErrExpired
	}
	if nbf, ok := tok.NotBefore(); ok && now.Add(clockSkew).Before(nbf) {
		return Claims{}, ErrExpired
	}

	var scope string
	_ = tok.Get("scope", &scope)
	if !hasScope(scope, ScopeWriteEvents) {
		return Claims{}, fmt.Errorf("%w: %q", ErrBadScope, scope)
	}

	var c Claims
	if err := tok.Get("tenant_id", &c.TenantID); err != nil || c.TenantID == "" {
		return Claims{}, fmt.Errorf("%w: missing tenant_id", ErrMalformed)
	}
	if err := tok.Get("install_id", &c.InstallID); err != nil || c.InstallID == "" {
		return Claims{}, fmt.Errorf("%w: missing install_id", ErrMalformed)
	}
	c.Scope = scope

	var tier int
	_ = tok.Get("trust_tier", &tier)
	c.TrustTier = uint8(tier)

	return c, nil
}

// key resolves a kid to a signing key, refetching the JWKS at most once per
// jwksMinRefetch window on a miss.
func (v *Verifier) key(ctx context.Context, kid string, now time.Time) (jwk.Key, error) {
	if kid == "" {
		return nil, ErrUnknownKID
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	stale := v.set == nil || now.Sub(v.fetchedAt) > jwksTTL
	if stale {
		if err := v.refetchLocked(ctx, now); err != nil && v.set == nil {
			return nil, err
		}
	}

	if k, ok := lookup(v.set, kid); ok {
		return k, nil
	}

	// Miss. A rotation may have just published a new kid — but a forged kid
	// must not be able to hammer the JWKS endpoint, so this is rate-limited.
	if now.Sub(v.lastRefetch) >= jwksMinRefetch {
		if err := v.refetchLocked(ctx, now); err != nil {
			return nil, fmt.Errorf("%w: refetch: %v", ErrUnknownKID, err)
		}
		if k, ok := lookup(v.set, kid); ok {
			return k, nil
		}
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownKID, kid)
}

func (v *Verifier) refetchLocked(ctx context.Context, now time.Time) error {
	v.lastRefetch = now
	set, err := jwk.Fetch(ctx, v.url, jwk.WithHTTPClient(v.client))
	if err != nil {
		return err
	}
	v.set = set
	v.fetchedAt = now
	return nil
}

// lookup enforces the key-shape requirements: use=sig, kty=OKP, crv=Ed25519.
// A key published for any other purpose or type is rejected even if the
// signature would verify against it.
func lookup(set jwk.Set, kid string) (jwk.Key, bool) {
	if set == nil {
		return nil, false
	}
	k, ok := set.LookupKeyID(kid)
	if !ok {
		return nil, false
	}
	if k.KeyUsage() != "" && k.KeyUsage() != "sig" {
		return nil, false
	}
	if k.KeyType().String() != "OKP" {
		return nil, false
	}
	var crv string
	if err := k.Get("crv", &crv); err != nil || crv != "Ed25519" {
		return nil, false
	}
	return k, true
}

func hasScope(scope, want string) bool {
	for _, s := range strings.Fields(scope) {
		if s == want {
			return true
		}
	}
	return false
}

func containsString(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd pkg/tenant && go test ./... -v`
Expected: PASS, 10 tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/tenant
git commit -m "feat(tenant): verify EdDSA ingest tokens with a rate-limited JWKS cache"
```

---

### Task 5: Per-event validation

**Files:**
- Create: `services/ingest/go.mod`
- Create: `services/ingest/internal/validate/validate.go`
- Test: `services/ingest/internal/validate/validate_test.go`

**Interfaces:**
- Consumes: `trackingv1.Event`, `limits.MaxPropsPerEvent`.
- Produces:
  - `func Event(e *trackingv1.Event) *trackingv1_Reject` — returns nil when valid.
    Exact signature: `func Event(e *trackingv1.Event) *trackingv1.Reject`
  - Reject codes (exported consts): `CodeMissingEventID`, `CodeBadEventID`, `CodeMissingName`, `CodeNameTooLong`, `CodeTooManyProps`, `CodePropKeyTooLong`, `CodeInvalidPropertyType`, `CodeMissingDeviceID`.

- [ ] **Step 1: Write the failing test**

`services/ingest/internal/validate/validate_test.go`:

```go
package validate_test

import (
	"strings"
	"testing"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/validate"
)

func valid() *trackingv1.Event {
	return &trackingv1.Event{
		EventId:  "0191f4a2-1c3d-7000-8000-000000000001",
		Name:     "checkout_completed",
		TsClient: 1754092800000,
		Seq:      1,
		DeviceId: "device-abc",
		Props: map[string]*trackingv1.Value{
			"amount": {Kind: &trackingv1.Value_NumberValue{NumberValue: 19.99}},
		},
	}
}

func TestValidEventPasses(t *testing.T) {
	if r := validate.Event(valid()); r != nil {
		t.Errorf("valid event rejected: %+v", r)
	}
}

func TestRejectCodes(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*trackingv1.Event)
		want string
	}{
		{"missing event_id", func(e *trackingv1.Event) { e.EventId = "" }, validate.CodeMissingEventID},
		{"non-uuid event_id", func(e *trackingv1.Event) { e.EventId = "not-a-uuid" }, validate.CodeBadEventID},
		{"missing name", func(e *trackingv1.Event) { e.Name = "" }, validate.CodeMissingName},
		{"name too long", func(e *trackingv1.Event) { e.Name = strings.Repeat("x", 256) }, validate.CodeNameTooLong},
		{"missing device_id", func(e *trackingv1.Event) { e.DeviceId = "" }, validate.CodeMissingDeviceID},
		{"too many props", func(e *trackingv1.Event) {
			e.Props = map[string]*trackingv1.Value{}
			for i := 0; i < 51; i++ {
				e.Props[string(rune('a'+i%26))+strings.Repeat("z", i)] =
					&trackingv1.Value{Kind: &trackingv1.Value_BoolValue{BoolValue: true}}
			}
		}, validate.CodeTooManyProps},
		{"prop key too long", func(e *trackingv1.Event) {
			e.Props = map[string]*trackingv1.Value{
				strings.Repeat("k", 129): {Kind: &trackingv1.Value_BoolValue{BoolValue: true}},
			}
		}, validate.CodePropKeyTooLong},
		{"prop with no value set", func(e *trackingv1.Event) {
			e.Props = map[string]*trackingv1.Value{"amount": {}}
		}, validate.CodeInvalidPropertyType},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := valid()
			c.mut(e)
			r := validate.Event(e)
			if r == nil {
				t.Fatalf("expected reject %s, got nil", c.want)
			}
			if r.Code != c.want {
				t.Errorf("code = %q, want %q", r.Code, c.want)
			}
			// The reject must name the event so the client can mark exactly
			// that row dead. A reject without an event_id is unactionable.
			if r.EventId != e.EventId {
				t.Errorf("reject.event_id = %q, want %q", r.EventId, e.EventId)
			}
		})
	}
}

// Exactly 50 props is the boundary and must pass.
func TestFiftyPropsIsAllowed(t *testing.T) {
	e := valid()
	e.Props = map[string]*trackingv1.Value{}
	for i := 0; i < 50; i++ {
		e.Props[strings.Repeat("k", i+1)] = &trackingv1.Value{Kind: &trackingv1.Value_BoolValue{BoolValue: true}}
	}
	if r := validate.Event(e); r != nil {
		t.Errorf("50 props rejected: %+v", r)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/ingest && go test ./internal/validate/...`
Expected: FAIL — `undefined: validate.Event`.

- [ ] **Step 3: Write the implementation**

```bash
mkdir -p services/ingest/internal/validate
cd services/ingest && go mod init github.com/dhiazfathra/event-tracking/services/ingest
go get github.com/google/uuid@v1.6.0
cd ../..
```

`services/ingest/internal/validate/validate.go`:

```go
// Package validate checks one event at a time and returns a Reject describing
// exactly what is wrong with it.
//
// The per-event granularity is the whole point. If one malformed event failed
// the batch with a 4xx, the client would retry that batch forever, the outbox
// would never drain, and every subsequent event would be stuck behind it. A
// single poison event would silently kill a user's entire telemetry stream.
package validate

import (
	"fmt"
	"math"

	"github.com/google/uuid"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/limits"
)

// Reject codes. These strings are in the public contract: the SDK logs them and
// support engineers grep for them. Do not reword an existing one — add a new
// code instead.
const (
	CodeMissingEventID      = "MISSING_EVENT_ID"
	CodeBadEventID          = "BAD_EVENT_ID"
	CodeMissingName         = "MISSING_NAME"
	CodeNameTooLong         = "NAME_TOO_LONG"
	CodeMissingDeviceID     = "MISSING_DEVICE_ID"
	CodeTooManyProps        = "TOO_MANY_PROPS"
	CodePropKeyTooLong      = "PROP_KEY_TOO_LONG"
	CodeInvalidPropertyType = "INVALID_PROPERTY_TYPE"
)

const (
	maxNameLen    = 255
	maxPropKeyLen = 128
)

// Event returns nil if the event is storable, or a Reject naming the first
// problem found. First-problem-only is deliberate: a client cannot act on a
// list of eight complaints any differently than on one.
func Event(e *trackingv1.Event) *trackingv1.Reject {
	reject := func(code, msg string) *trackingv1.Reject {
		return &trackingv1.Reject{EventId: e.GetEventId(), Code: code, Message: msg}
	}

	if e.GetEventId() == "" {
		return reject(CodeMissingEventID, "event_id is required")
	}
	if _, err := uuid.Parse(e.GetEventId()); err != nil {
		return reject(CodeBadEventID, "event_id must be a UUID")
	}
	if e.GetName() == "" {
		return reject(CodeMissingName, "name is required")
	}
	if len(e.GetName()) > maxNameLen {
		return reject(CodeNameTooLong, fmt.Sprintf("name exceeds %d bytes", maxNameLen))
	}
	if e.GetDeviceId() == "" {
		return reject(CodeMissingDeviceID, "device_id is required")
	}

	// Bounds the ClickHouse JSON subcolumn explosion. The limit is a stated
	// guess (spec §7.5), not a measured one — but an unbounded property space
	// is a per-tenant denial of service on the whole table.
	if len(e.GetProps()) > limits.MaxPropsPerEvent {
		return reject(CodeTooManyProps,
			fmt.Sprintf("props: %d exceeds limit of %d", len(e.GetProps()), limits.MaxPropsPerEvent))
	}

	for k, v := range e.GetProps() {
		if len(k) > maxPropKeyLen {
			return reject(CodePropKeyTooLong, fmt.Sprintf("props.%s: key exceeds %d bytes", k, maxPropKeyLen))
		}
		switch val := v.GetKind().(type) {
		case *trackingv1.Value_StringValue, *trackingv1.Value_BoolValue:
			// fine
		case *trackingv1.Value_NumberValue:
			// NaN and Inf round-trip through JSON as nulls or errors depending
			// on the decoder. Reject them here rather than storing garbage.
			if math.IsNaN(val.NumberValue) || math.IsInf(val.NumberValue, 0) {
				return reject(CodeInvalidPropertyType, fmt.Sprintf("props.%s: must be a finite number", k))
			}
		default:
			return reject(CodeInvalidPropertyType, fmt.Sprintf("props.%s: no value set", k))
		}
	}

	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/ingest && go test ./internal/validate/... -v`
Expected: PASS, 10 subtests.

- [ ] **Step 5: Commit**

```bash
git add services/ingest go.work
git commit -m "feat(ingest): per-event validation with stable reject codes"
```

---

### Task 6: Clock skew correction

**Files:**
- Create: `services/ingest/internal/enrich/skew.go`
- Test: `services/ingest/internal/enrich/skew_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `type SessionKey struct { TenantID, DeviceID, SessionID string }`
  - `type OffsetStore interface { GetOrSet(ctx context.Context, k SessionKey, candidate int64) (int64, error) }`
  - `func Correct(tsClient int64, offsetMS int64, receivedAt time.Time) (ts time.Time, clamped bool)`
  - Exported: `MaxFutureSkew = 24 * time.Hour`, `MaxPastSkew = 30 * 24 * time.Hour`

- [ ] **Step 1: Write the failing test**

`services/ingest/internal/enrich/skew_test.go`:

```go
package enrich_test

import (
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
)

func TestCorrectAppliesOffset(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	tsClient := received.Add(-5 * time.Minute).UnixMilli() // device 5 min slow

	got, clamped := enrich.Correct(tsClient, (5 * time.Minute).Milliseconds(), received)
	if clamped {
		t.Errorf("clamped = true, want false")
	}
	if !got.Equal(received) {
		t.Errorf("ts = %v, want %v", got, received)
	}
}

// A single device with a broken clock must not create partitions years out.
func TestCorrectClampsFarFuture(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	tsClient := received.Add(400 * 24 * time.Hour).UnixMilli()

	got, clamped := enrich.Correct(tsClient, 0, received)
	if !clamped {
		t.Fatalf("clamped = false, want true")
	}
	if !got.Equal(received) {
		t.Errorf("ts = %v, want clamped to received_at %v", got, received)
	}
}

func TestCorrectClampsFarPast(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	tsClient := received.Add(-90 * 24 * time.Hour).UnixMilli()

	got, clamped := enrich.Correct(tsClient, 0, received)
	if !clamped {
		t.Fatalf("clamped = false, want true")
	}
	if !got.Equal(received) {
		t.Errorf("ts = %v, want clamped to received_at %v", got, received)
	}
}

func TestCorrectBoundariesAreInclusive(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	for _, c := range []struct {
		name  string
		delta time.Duration
	}{
		{"exactly 24h future", enrich.MaxFutureSkew},
		{"exactly 30d past", -enrich.MaxPastSkew},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, clamped := enrich.Correct(received.Add(c.delta).UnixMilli(), 0, received)
			if clamped {
				t.Errorf("clamped at the boundary; boundary must be inclusive")
			}
		})
	}
}

// The offset must be stable per session. If a retry recomputed it, the row
// would move under the (tenant, name, ts, event_id) sort key — and near
// midnight UTC into a different partition — so ReplacingMergeTree would never
// collapse the duplicate.
func TestOffsetIsStableAcrossRetries(t *testing.T) {
	store := enrich.NewMemoryOffsetStore()
	key := enrich.SessionKey{TenantID: "t1", DeviceID: "d1", SessionID: "s1"}

	first, err := store.GetOrSet(t.Context(), key, 5000)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// A retry arriving later computes a different candidate offset.
	second, err := store.GetOrSet(t.Context(), key, 9999)
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if first != second {
		t.Errorf("offset changed across retries: %d then %d", first, second)
	}
	if first != 5000 {
		t.Errorf("offset = %d, want the first-contact value 5000", first)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/ingest && go test ./internal/enrich/...`
Expected: FAIL — `undefined: enrich.Correct`.

- [ ] **Step 3: Write the implementation**

`services/ingest/internal/enrich/skew.go`:

```go
// Package enrich turns a validated wire envelope into a storage row: server
// timestamps, trust tier, install id, and skew-corrected event time.
package enrich

import (
	"context"
	"sync"
	"time"
)

const (
	// MaxFutureSkew and MaxPastSkew bound the corrected timestamp. Beyond them
	// the timestamp is clamped to received_at and flagged. Without this, one
	// device with a broken clock creates partitions years out and wrecks the
	// partition count for everybody.
	MaxFutureSkew = 24 * time.Hour
	MaxPastSkew   = 30 * 24 * time.Hour
)

// SessionKey scopes a clock offset. Per-session, never per-request.
type SessionKey struct {
	TenantID  string
	DeviceID  string
	SessionID string
}

// OffsetStore persists the first-contact offset for a session.
//
// GetOrSet returns the stored offset if one exists and stores+returns the
// candidate otherwise. It must be atomic: two concurrent first requests for the
// same session must agree on one value, or the two events land at different
// ts and a retry of either stops deduplicating.
type OffsetStore interface {
	GetOrSet(ctx context.Context, k SessionKey, candidate int64) (int64, error)
}

// Correct applies the session offset and clamps the result.
//
// Returns the corrected timestamp and whether it was clamped. Clamped rows are
// still stored — ts_client keeps the raw device value for forensics — but the
// flag lets ingest count how much of the corpus has an untrustworthy clock.
func Correct(tsClientMS, offsetMS int64, receivedAt time.Time) (time.Time, bool) {
	ts := time.UnixMilli(tsClientMS + offsetMS).UTC()

	if ts.Sub(receivedAt) > MaxFutureSkew {
		return receivedAt, true
	}
	if receivedAt.Sub(ts) > MaxPastSkew {
		return receivedAt, true
	}
	return ts, false
}

// CandidateOffset is the first-contact estimate: how far behind the server the
// device's clock appeared to be at send time.
func CandidateOffset(sentAtMS int64, receivedAt time.Time) int64 {
	return receivedAt.UnixMilli() - sentAtMS
}

// MemoryOffsetStore is the in-process implementation. Production wires the
// Postgres-backed store (session_offsets table) instead; this one exists so the
// skew logic is testable without a database and so a single-pod dev setup runs.
type MemoryOffsetStore struct {
	mu sync.Mutex
	m  map[SessionKey]int64
}

func NewMemoryOffsetStore() *MemoryOffsetStore {
	return &MemoryOffsetStore{m: make(map[SessionKey]int64)}
}

func (s *MemoryOffsetStore) GetOrSet(_ context.Context, k SessionKey, candidate int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.m[k]; ok {
		return v, nil
	}
	s.m[k] = candidate
	return candidate, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/ingest && go test ./internal/enrich/... -v`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add services/ingest/internal/enrich
git commit -m "feat(ingest): per-session clock skew correction with clamping"
```

---

### Task 7: Envelope → storage row

**Files:**
- Create: `services/ingest/internal/enrich/enrich.go`
- Test: `services/ingest/internal/enrich/enrich_test.go`

**Interfaces:**
- Consumes: `trackingv1.Event`, `tenant.Claims`, `clickhouse.Row`, `Correct`, `OffsetStore`.
- Produces: `func Row(e *trackingv1.Event, c tenant.Claims, offsetMS int64, receivedAt time.Time) (clickhouse.Row, bool)` — the bool is `clamped`.

- [ ] **Step 1: Write the failing test**

`services/ingest/internal/enrich/enrich_test.go`:

```go
package enrich_test

import (
	"encoding/json"
	"testing"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
)

func TestRowTakesTenantFromClaimsOnly(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	e := &trackingv1.Event{
		EventId:  "0191f4a2-1c3d-7000-8000-000000000001",
		Name:     "checkout",
		TsClient: received.UnixMilli(),
		DeviceId: "d1",
	}
	claims := tenant.Claims{TenantID: "tenant-A", InstallID: "i-1", TrustTier: 1}

	row, _ := enrich.Row(e, claims, 0, received)

	if row.TenantID != "tenant-A" {
		t.Errorf("tenant_id = %q, want tenant-A (from the verified claim)", row.TenantID)
	}
	if row.InstallID != "i-1" {
		t.Errorf("install_id = %q, want i-1 (server-issued)", row.InstallID)
	}
	if row.TrustTier != 1 {
		t.Errorf("trust_tier = %d, want 1", row.TrustTier)
	}
	if !row.TSReceived.Equal(received) {
		t.Errorf("ts_received = %v, want %v", row.TSReceived, received)
	}
	// The server stamps ts/ts_received/trust_tier/install_id. It never mints an
	// event ID: a per-attempt server ID would make every retry a distinct row
	// and break read-time dedup.
	if row.EventID != e.EventId {
		t.Errorf("event_id = %q, want the client's %q", row.EventID, e.EventId)
	}
}

func TestRowSerialisesPropsAsJSONObject(t *testing.T) {
	received := time.Now().UTC()
	e := &trackingv1.Event{
		EventId:  "0191f4a2-1c3d-7000-8000-000000000001",
		Name:     "checkout",
		TsClient: received.UnixMilli(),
		DeviceId: "d1",
		Props: map[string]*trackingv1.Value{
			"amount":   {Kind: &trackingv1.Value_NumberValue{NumberValue: 19.99}},
			"currency": {Kind: &trackingv1.Value_StringValue{StringValue: "IDR"}},
			"first":    {Kind: &trackingv1.Value_BoolValue{BoolValue: true}},
		},
	}

	row, _ := enrich.Row(e, tenant.Claims{TenantID: "t1", InstallID: "i-1"}, 0, received)

	var got map[string]any
	if err := json.Unmarshal([]byte(row.Props), &got); err != nil {
		t.Fatalf("props is not valid JSON: %v (%q)", err, row.Props)
	}
	// The union wrapper must be flattened — ClickHouse should see
	// {"amount":19.99}, not {"amount":{"numberValue":19.99}}.
	if got["amount"] != 19.99 {
		t.Errorf("props.amount = %v, want 19.99 (flattened, not wrapped)", got["amount"])
	}
	if got["currency"] != "IDR" {
		t.Errorf("props.currency = %v, want IDR", got["currency"])
	}
	if got["first"] != true {
		t.Errorf("props.first = %v, want true", got["first"])
	}
}

func TestRowEmptyPropsIsEmptyObject(t *testing.T) {
	received := time.Now().UTC()
	e := &trackingv1.Event{EventId: "0191f4a2-1c3d-7000-8000-000000000001", Name: "n", DeviceId: "d1", TsClient: received.UnixMilli()}
	row, _ := enrich.Row(e, tenant.Claims{TenantID: "t1", InstallID: "i-1"}, 0, received)
	if row.Props != "{}" {
		t.Errorf("props = %q, want {}", row.Props)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/ingest && go test ./internal/enrich/... -run Row`
Expected: FAIL — `undefined: enrich.Row`.

- [ ] **Step 3: Write the implementation**

`services/ingest/internal/enrich/enrich.go`:

```go
package enrich

import (
	"encoding/json"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

// Row builds the storage row from a validated envelope plus verified claims.
//
// Every trust-bearing field comes from claims, never from the envelope. The
// envelope has no tenant field at all (proto §3.1); if one ever appears in a
// request body it is ignored here, not preferred over the claim — otherwise a
// valid token for tenant A could write rows labelled tenant B.
func Row(e *trackingv1.Event, c tenant.Claims, offsetMS int64, receivedAt time.Time) (clickhouse.Row, bool) {
	ts, clamped := Correct(e.GetTsClient(), offsetMS, receivedAt)
	ctx := e.GetContext()

	return clickhouse.Row{
		TenantID:   c.TenantID,
		EventID:    e.GetEventId(),
		Name:       e.GetName(),
		TS:         ts,
		TSClient:   time.UnixMilli(e.GetTsClient()).UTC(),
		TSReceived: receivedAt,

		UserID:      e.GetUserId(),
		AnonymousID: e.GetAnonymousId(),
		DeviceID:    e.GetDeviceId(),
		SessionID:   e.GetSessionId(),
		Seq:         e.GetSeq(),

		AppVersion: ctx.GetAppVersion(),
		SDKVersion: ctx.GetSdkVersion(),
		OS:         ctx.GetOs(),
		OSVersion:  ctx.GetOsVersion(),
		Locale:     ctx.GetLocale(),

		TrustTier: c.TrustTier,
		InstallID: c.InstallID,

		Props: propsJSON(e.GetProps()),
	}, clamped
}

// propsJSON flattens the Value union into a plain JSON object so ClickHouse's
// JSON type produces subcolumns named after the property, not after the union
// arm. {"amount":19.99}, never {"amount":{"numberValue":19.99}}.
func propsJSON(props map[string]*trackingv1.Value) string {
	if len(props) == 0 {
		return "{}"
	}
	flat := make(map[string]any, len(props))
	for k, v := range props {
		switch val := v.GetKind().(type) {
		case *trackingv1.Value_StringValue:
			flat[k] = val.StringValue
		case *trackingv1.Value_NumberValue:
			flat[k] = val.NumberValue
		case *trackingv1.Value_BoolValue:
			flat[k] = val.BoolValue
		}
	}
	b, err := json.Marshal(flat)
	if err != nil {
		// Unreachable: every value is a string, float64, or bool. Falling back
		// to an empty object is better than dropping the event.
		return "{}"
	}
	return string(b)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/ingest && go test ./internal/enrich/... -v`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add services/ingest/internal/enrich
git commit -m "feat(ingest): build storage rows from verified claims, never the body"
```

---

### Task 8: Redis quota and rate limiting

**Files:**
- Create: `services/ingest/internal/quota/quota.go`
- Test: `services/ingest/internal/quota/quota_test.go` (testcontainers Redis)

**Interfaces:**
- Consumes: `tenant.Claims`.
- Produces:
  - `type Limits struct { DailyEvents int64; RPS int }`
  - `type Checker struct{ ... }`
  - `func NewChecker(rdb *redis.Client) *Checker`
  - `func (c *Checker) Allow(ctx context.Context, cl tenant.Claims, lim Limits, n int, now time.Time) (Decision, error)`
  - `type Decision struct { Allowed bool; RetryAfter time.Duration; Reason string }`

- [ ] **Step 1: Write the failing test**

`services/ingest/internal/quota/quota_test.go`:

```go
package quota_test

import (
	"context"
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
)

func TestDailyQuotaExhaustionReturns429WithRetryAfter(t *testing.T) {
	ctx := context.Background()
	c := quota.NewChecker(startRedis(t))
	cl := tenant.Claims{TenantID: "t1", InstallID: "i-1"}
	lim := quota.Limits{DailyEvents: 100, RPS: 1000}
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	d, err := c.Allow(ctx, cl, lim, 100, now)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !d.Allowed {
		t.Fatalf("first 100 events denied: %+v", d)
	}

	d, err = c.Allow(ctx, cl, lim, 1, now)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if d.Allowed {
		t.Errorf("101st event allowed, want denied")
	}
	if d.RetryAfter <= 0 {
		t.Errorf("RetryAfter = %v, want > 0 so the client's backoff has a target", d.RetryAfter)
	}
	if d.Reason != "daily_quota" {
		t.Errorf("Reason = %q, want daily_quota", d.Reason)
	}
}

// The primary bucket is install_id. Two installs under the same tenant must not
// share a per-second limit — and one install must not be able to spend the
// whole tenant budget in a second.
func TestRateLimitKeysOnInstallFirst(t *testing.T) {
	ctx := context.Background()
	c := quota.NewChecker(startRedis(t))
	lim := quota.Limits{DailyEvents: 1_000_000, RPS: 10}
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	a := tenant.Claims{TenantID: "t1", InstallID: "install-A"}
	b := tenant.Claims{TenantID: "t1", InstallID: "install-B"}

	if d, _ := c.Allow(ctx, a, lim, 10, now); !d.Allowed {
		t.Fatalf("install A first 10 denied")
	}
	if d, _ := c.Allow(ctx, a, lim, 1, now); d.Allowed {
		t.Errorf("install A 11th allowed, want rate limited")
	}
	// Different install, same tenant, same second: unaffected.
	if d, _ := c.Allow(ctx, b, lim, 10, now); !d.Allowed {
		t.Errorf("install B denied by install A's limit — rate limit is keyed wrong")
	}
}

func TestRateLimitWindowRollsOver(t *testing.T) {
	ctx := context.Background()
	c := quota.NewChecker(startRedis(t))
	cl := tenant.Claims{TenantID: "t1", InstallID: "i-1"}
	lim := quota.Limits{DailyEvents: 1_000_000, RPS: 5}
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	_, _ = c.Allow(ctx, cl, lim, 5, now)
	if d, _ := c.Allow(ctx, cl, lim, 1, now); d.Allowed {
		t.Fatalf("6th in the same second allowed")
	}
	if d, _ := c.Allow(ctx, cl, lim, 1, now.Add(time.Second)); !d.Allowed {
		t.Errorf("next second denied, want allowed")
	}
}
```

Add a `startRedis` helper in `quota_testmain_test.go` mirroring the ClickHouse
helper from Task 1, using
`github.com/testcontainers/testcontainers-go/modules/redis`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/ingest && go test ./internal/quota/...`
Expected: FAIL — `undefined: quota.NewChecker`.

- [ ] **Step 3: Write the implementation**

```bash
mkdir -p services/ingest/internal/quota
cd services/ingest && go get github.com/redis/go-redis/v9@v9.7.0 && go get github.com/testcontainers/testcontainers-go/modules/redis@v0.34.0 && cd ../..
```

`services/ingest/internal/quota/quota.go`:

```go
// Package quota enforces per-tenant daily budgets and per-install rate limits
// with Redis counters.
//
// Ordering of the buckets is deliberate: install_id first, tenant second, IP
// last (IP is not enforced here at all — it survives only as a coarse anomaly
// signal upstream). IP-primary limiting is actively harmful in this market:
// Indonesian carriers CGNAT aggressively, so one Telkomsel egress IP is
// thousands of real users.
package quota

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

type Limits struct {
	DailyEvents int64
	RPS         int
}

type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	Reason     string // "" | "rate_limit" | "daily_quota"
}

type Checker struct {
	rdb *redis.Client
}

func NewChecker(rdb *redis.Client) *Checker {
	return &Checker{rdb: rdb}
}

// Allow accounts n events and reports whether the batch may proceed.
//
// Both counters are incremented before the verdict, then rolled back on denial,
// so two concurrent pods cannot both squeeze past the last unit of budget. The
// rollback is best-effort: over-counting a denied batch costs the tenant
// nothing real, whereas under-counting lets the limit be exceeded.
func (c *Checker) Allow(ctx context.Context, cl tenant.Claims, lim Limits, n int, now time.Time) (Decision, error) {
	rateKey := fmt.Sprintf("rl:{%s}:%s:%d", cl.TenantID, cl.InstallID, now.Unix())
	dayKey := fmt.Sprintf("q:{%s}:%s", cl.TenantID, now.UTC().Format("2006-01-02"))

	rate, err := c.incr(ctx, rateKey, int64(n), 2*time.Second)
	if err != nil {
		return Decision{}, err
	}
	if lim.RPS > 0 && rate > int64(lim.RPS) {
		c.rollback(ctx, rateKey, int64(n))
		return Decision{
			Allowed:    false,
			RetryAfter: time.Second,
			Reason:     "rate_limit",
		}, nil
	}

	day, err := c.incr(ctx, dayKey, int64(n), 48*time.Hour)
	if err != nil {
		return Decision{}, err
	}
	if lim.DailyEvents > 0 && day > lim.DailyEvents {
		c.rollback(ctx, dayKey, int64(n))
		c.rollback(ctx, rateKey, int64(n))
		return Decision{
			Allowed:    false,
			RetryAfter: untilNextUTCDay(now),
			Reason:     "daily_quota",
		}, nil
	}

	return Decision{Allowed: true}, nil
}

func (c *Checker) incr(ctx context.Context, key string, by int64, ttl time.Duration) (int64, error) {
	pipe := c.rdb.TxPipeline()
	incr := pipe.IncrBy(ctx, key, by)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("quota incr %s: %w", key, err)
	}
	return incr.Val(), nil
}

func (c *Checker) rollback(ctx context.Context, key string, by int64) {
	// Best effort. A failed rollback over-counts, which is the safe direction.
	_ = c.rdb.DecrBy(ctx, key, by).Err()
}

// untilNextUTCDay is the honest Retry-After for an exhausted daily budget:
// nothing the client does before midnight UTC will succeed.
func untilNextUTCDay(now time.Time) time.Duration {
	next := now.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	return next.Sub(now.UTC())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/ingest && go test ./internal/quota/... -v`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add services/ingest/internal/quota
git commit -m "feat(ingest): redis quota and install-keyed rate limiting"
```

---

### Task 9: The batch handler

**Files:**
- Create: `services/ingest/internal/handler/batch.go`
- Test: `services/ingest/internal/handler/batch_test.go`

**Interfaces:**
- Consumes: `tenant.Verifier`, `validate.Event`, `enrich.Row`, `enrich.OffsetStore`, `quota.Checker`, `clickhouse.InsertEvents`, `limits.*`.
- Produces:
  - `type Deps struct { Verifier *tenant.Verifier; Offsets enrich.OffsetStore; Quota *quota.Checker; LimitsFor func(ctx context.Context, tenantID string, tier uint8) (quota.Limits, error); Insert func(ctx context.Context, rows []clickhouse.Row) error; Now func() time.Time }`
  - `func NewBatch(d Deps) http.Handler`

- [ ] **Step 1: Write the failing test**

`services/ingest/internal/handler/batch_test.go`:

```go
package handler_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/handler"
)

// One malformed event must not fail the batch. This is the single most
// important behaviour in the service: a batch-level 4xx on a poison event
// wedges the client's outbox forever.
func TestPartialSuccess(t *testing.T) {
	h, sink := newTestHandler(t)

	body := batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "ok", "deviceId": "d1", "tsClient": "1754092800000"},
		{"eventId": "not-a-uuid", "name": "bad", "deviceId": "d1", "tsClient": "1754092800000"},
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000003", "name": "ok2", "deviceId": "d1", "tsClient": "1754092800000"},
	})

	rec := post(t, h, body, false)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (partial success is a 200)", rec.Code)
	}

	var resp trackingv1.BatchResponse
	decode(t, rec.Body.Bytes(), &resp)

	if len(resp.Accepted) != 2 {
		t.Errorf("accepted = %v, want 2", resp.Accepted)
	}
	if len(resp.Rejected) != 1 || resp.Rejected[0].Code != "BAD_EVENT_ID" {
		t.Errorf("rejected = %+v, want one BAD_EVENT_ID", resp.Rejected)
	}
	if got := len(sink.rows); got != 2 {
		t.Errorf("inserted %d rows, want 2 — rejected events must not be stored", got)
	}
}

func TestGzipBodyIsAccepted(t *testing.T) {
	h, sink := newTestHandler(t)
	body := batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "ok", "deviceId": "d1", "tsClient": "1754092800000"},
	})

	rec := post(t, h, body, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(sink.rows) != 1 {
		t.Errorf("inserted %d rows, want 1", len(sink.rows))
	}
}

func TestOverSizedBatchReturns413(t *testing.T) {
	h, _ := newTestHandler(t)
	events := make([]map[string]any, 501)
	for i := range events {
		events[i] = map[string]any{
			"eventId": "0191f4a2-1c3d-7000-8000-00000000" + pad(i), "name": "n", "deviceId": "d1", "tsClient": "1754092800000",
		}
	}
	rec := post(t, h, batchJSON(t, events), false)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

func TestMalformedEnvelopeReturns400(t *testing.T) {
	h, _ := newTestHandler(t)
	rec := post(t, h, []byte(`{"events": "not an array"}`), false)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBadTokenReturns401(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/batch", bytes.NewReader(batchJSON(t, nil)))
	req.Header.Set("Authorization", "Bearer garbage")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestQuotaExhaustionReturns429WithRetryAfter(t *testing.T) {
	h, _ := newTestHandlerWithQuota(t, 0) // zero budget
	rec := post(t, h, batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "n", "deviceId": "d1", "tsClient": "1754092800000"},
	}), false)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header; the client's backoff keys off it")
	}
}

// A ClickHouse failure must surface as 5xx so the client keeps the events.
// Returning 200 here would delete them from the outbox with nothing stored.
func TestInsertFailureReturns503AndAcceptsNothing(t *testing.T) {
	h := newTestHandlerFailingInsert(t)
	rec := post(t, h, batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "n", "deviceId": "d1", "tsClient": "1754092800000"},
	}), false)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "accepted") && strings.Contains(rec.Body.String(), "0191f4a2") {
		t.Error("response acknowledged an event that was never stored")
	}
}

// A tenant_id in the body must be ignored, never preferred over the claim.
func TestBodyTenantIDIsIgnored(t *testing.T) {
	h, sink := newTestHandler(t) // claims tenant is "t1"
	body := []byte(`{"sentAt":"1754092800000","events":[{"eventId":"0191f4a2-1c3d-7000-8000-000000000001","name":"n","deviceId":"d1","tsClient":"1754092800000","tenantId":"attacker-tenant"}]}`)

	rec := post(t, h, body, false)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(sink.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(sink.rows))
	}
	if sink.rows[0].TenantID != "t1" {
		t.Errorf("stored tenant_id = %q, want t1 from the verified claim", sink.rows[0].TenantID)
	}
}
```

Write the fixtures (`newTestHandler`, `newTestHandlerWithQuota`,
`newTestHandlerFailingInsert`, `batchJSON`, `post`, `decode`, `pad`) in
`handler_fixtures_test.go`. They wire a `Deps` with an in-memory offset store, a
`quota.Checker` backed by a `miniredis`-style test double or a real container, a
capturing insert function (`sink.rows`), and a `tenant.Verifier` primed against a
local JWKS `httptest` server minting `tenant_id=t1`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/ingest && go test ./internal/handler/...`
Expected: FAIL — `undefined: handler.NewBatch`.

- [ ] **Step 3: Write the handler**

`services/ingest/internal/handler/batch.go`:

```go
// Package handler holds the HTTP surface. The status codes it returns are part
// of the public contract — the SDK's entire retry logic keys off them, and the
// SDK cannot be force-upgraded. Changing one is a breaking change.
package handler

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/limits"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/validate"
)

type Deps struct {
	Verifier  *tenant.Verifier
	Offsets   enrich.OffsetStore
	Quota     *quota.Checker
	LimitsFor func(ctx context.Context, tenantID string, tier uint8) (quota.Limits, error)
	Insert    func(ctx context.Context, rows []clickhouse.Row) error
	Now       func() time.Time
}

func NewBatch(d Deps) http.Handler {
	if d.Now == nil {
		d.Now = func() time.Time { return time.Now().UTC() }
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAt := d.Now()

		claims, err := d.Verifier.Verify(r.Context(), r.Header.Get("Authorization"), receivedAt)
		if err != nil {
			// 401 tells the client to re-exchange for a fresh token and retry
			// once. Any other code here would make an expired token look like
			// a permanent failure and stop the device syncing.
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}

		body, err := readBody(r)
		if err != nil {
			if errors.Is(err, errTooLarge) {
				httpError(w, http.StatusRequestEntityTooLarge, "batch too large")
				return
			}
			httpError(w, http.StatusBadRequest, "unreadable body")
			return
		}

		var req trackingv1.BatchRequest
		// DiscardUnknown: a newer SDK sending a field this server does not know
		// must not fail the batch. Notably this is also what makes a stray
		// tenant_id in the body a no-op rather than an error — it is discarded,
		// never read.
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, &req); err != nil {
			httpError(w, http.StatusBadRequest, "malformed batch envelope")
			return
		}
		if len(req.Events) > limits.MaxEventsPerBatch {
			httpError(w, http.StatusRequestEntityTooLarge, "too many events")
			return
		}

		lim, err := d.LimitsFor(r.Context(), claims.TenantID, claims.TrustTier)
		if err != nil {
			httpError(w, http.StatusServiceUnavailable, "limits unavailable")
			return
		}

		dec, err := d.Quota.Allow(r.Context(), claims, lim, len(req.Events), receivedAt)
		if err != nil {
			httpError(w, http.StatusServiceUnavailable, "quota unavailable")
			return
		}
		if !dec.Allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(dec.RetryAfter.Seconds())+1))
			httpError(w, http.StatusTooManyRequests, dec.Reason)
			return
		}

		resp := &trackingv1.BatchResponse{ReceivedAt: receivedAt.UnixMilli()}
		rows := make([]clickhouse.Row, 0, len(req.Events))
		accepted := make([]string, 0, len(req.Events))

		offset := d.sessionOffset(r.Context(), claims, req, receivedAt)

		for _, e := range req.Events {
			if rej := validate.Event(e); rej != nil {
				resp.Rejected = append(resp.Rejected, rej)
				continue
			}
			row, _ := enrich.Row(e, claims, offset, receivedAt)
			rows = append(rows, row)
			accepted = append(accepted, e.GetEventId())
		}

		if err := d.Insert(r.Context(), rows); err != nil {
			// The client still holds these events. 503 keeps them there.
			httpError(w, http.StatusServiceUnavailable, "storage unavailable")
			return
		}

		// Only set accepted after the insert returns. wait_for_async_insert=1
		// means this ack is a durability claim, and the client deletes its copy
		// on the strength of it.
		resp.Accepted = accepted

		w.Header().Set("Content-Type", "application/json")
		out, err := protojson.Marshal(resp)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "encode")
			return
		}
		_, _ = w.Write(out)
	})
}

// sessionOffset resolves the clock offset once per batch. Every event in a
// batch shares a device and session in practice; if a batch spans sessions the
// first event's session wins, which is acceptable because the offset is a
// device-clock property, not a session property.
func (d Deps) sessionOffset(ctx context.Context, c tenant.Claims, req trackingv1.BatchRequest, receivedAt time.Time) int64 {
	if len(req.Events) == 0 {
		return 0
	}
	first := req.Events[0]
	key := enrich.SessionKey{
		TenantID:  c.TenantID,
		DeviceID:  first.GetDeviceId(),
		SessionID: first.GetSessionId(),
	}
	offset, err := d.Offsets.GetOrSet(ctx, key, enrich.CandidateOffset(req.GetSentAt(), receivedAt))
	if err != nil {
		// A store failure must not drop the batch. Zero offset means the raw
		// device clock is used, which the clamp still bounds.
		return 0
	}
	return offset
}

var errTooLarge = errors.New("body too large")

// readBody decompresses and enforces the decompressed size cap. The cap is on
// the decompressed bytes on purpose: a gzip bomb is small on the wire.
func readBody(r *http.Request) ([]byte, error) {
	var src io.Reader = http.MaxBytesReader(nil, r.Body, limits.MaxBatchBytes)

	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		src = io.LimitReader(zr, limits.MaxBatchBytes+1)
	}

	b, err := io.ReadAll(src)
	if err != nil {
		return nil, err
	}
	if len(b) > limits.MaxBatchBytes {
		return nil, errTooLarge
	}
	return b, nil
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/ingest && go test ./internal/handler/... -v`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add services/ingest/internal/handler
git commit -m "feat(ingest): partial-success batch handler with the full status contract"
```

---

### Task 10: Attestation and the token exchange endpoint

**Files:**
- Create: `services/ingest/internal/attest/attest.go`
- Create: `services/ingest/internal/handler/token.go`
- Test: `services/ingest/internal/handler/token_test.go`

**Interfaces:**
- Consumes: `tenant.Minter`, `trackingv1.TokenRequest/TokenResponse`.
- Produces:
  - `type Attestor interface { Verify(ctx context.Context, platform, blob string) (subject string, ok bool) }`
  - `type NoopAttestor struct{}` — always Tier 1, for local dev
  - `type TokenDeps struct { Minter *tenant.Minter; Attestor Attestor; ResolveTenant func(ctx, clientID string) (string, error); IssueInstall func(ctx context.Context, tenantID, platform, subject string, tier uint8) (string, error); Now func() time.Time }`
  - `func NewToken(d TokenDeps) http.Handler`

- [ ] **Step 1: Write the failing test**

`services/ingest/internal/handler/token_test.go`:

```go
package handler_test

import (
	"net/http"
	"testing"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
)

// Attestation failure must not block. Rooted, custom-ROM, and de-Googled
// devices fail legitimately; simulators fail; Play Integrity is quota-limited.
func TestAttestationFailureYieldsTier1NotRejection(t *testing.T) {
	h := newTokenHandler(t, failingAttestor{})

	rec := postJSON(t, h, "/v1/auth/token", `{"clientId":"pk_live_abc","platform":"android","attestation":"garbage"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — attestation failure assigns a tier, it does not block", rec.Code)
	}

	var resp trackingv1.TokenResponse
	decode(t, rec.Body.Bytes(), &resp)
	if resp.TrustTier != 1 {
		t.Errorf("trust_tier = %d, want 1", resp.TrustTier)
	}
	if resp.AccessToken == "" {
		t.Error("no token issued")
	}
}

func TestSuccessfulAttestationYieldsTier0(t *testing.T) {
	h := newTokenHandler(t, passingAttestor{subject: "attest-subject-1"})

	rec := postJSON(t, h, "/v1/auth/token", `{"clientId":"pk_live_abc","platform":"ios","attestation":"valid"}`)
	var resp trackingv1.TokenResponse
	decode(t, rec.Body.Bytes(), &resp)

	if resp.TrustTier != 0 {
		t.Errorf("trust_tier = %d, want 0", resp.TrustTier)
	}
	if resp.ExpiresIn < 1800 || resp.ExpiresIn > 3600 {
		t.Errorf("expires_in = %d, want 1800..3600", resp.ExpiresIn)
	}
}

func TestUnknownClientIDReturns401(t *testing.T) {
	h := newTokenHandler(t, passingAttestor{})
	rec := postJSON(t, h, "/v1/auth/token", `{"clientId":"pk_live_unknown","platform":"ios"}`)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// A client must never be able to choose its own install_id: it is the primary
// rate-limit bucket, so choosing it means resetting the limit at will.
func TestClientSuppliedInstallIDIsIgnored(t *testing.T) {
	h := newTokenHandler(t, passingAttestor{subject: "s1"})
	rec := postJSON(t, h, "/v1/auth/token",
		`{"clientId":"pk_live_abc","platform":"ios","attestation":"valid","installId":"i-chosen-by-attacker"}`)

	var resp trackingv1.TokenResponse
	decode(t, rec.Body.Bytes(), &resp)

	installID := installIDFromToken(t, resp.AccessToken)
	if installID == "i-chosen-by-attacker" {
		t.Error("install_id was taken from the request body")
	}
	if installID == "" {
		t.Error("no install_id issued")
	}
}
```

Fixtures: `newTokenHandler`, `failingAttestor`, `passingAttestor`, `postJSON`,
`installIDFromToken` (parses the JWT without verifying and reads the claim).

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/ingest && go test ./internal/handler/... -run Token`
Expected: FAIL — `undefined: handler.NewToken`.

- [ ] **Step 3: Write the attestor interface**

`services/ingest/internal/attest/attest.go`:

```go
// Package attest verifies platform attestations at the token exchange.
//
// Deliberately not on the ingest hot path: ingest only verifies a signature.
// Attestation is a per-app-start cost, not a per-batch cost.
package attest

import "context"

// Attestor verifies a platform attestation blob and returns a stable subject
// identifying the app install, plus whether verification succeeded.
//
// A false result is not an error condition — it assigns Tier 1.
type Attestor interface {
	Verify(ctx context.Context, platform, blob string) (subject string, ok bool)
}

// Noop always reports failure, so every install lands at Tier 1. This is the
// correct local-dev and simulator behaviour: simulators cannot attest, and the
// design already treats attestation-unavailable as a tier, not a rejection.
type Noop struct{}

func (Noop) Verify(context.Context, string, string) (string, bool) { return "", false }
```

Real App Attest / Play Integrity verification is a separate integration and is
intentionally out of scope for this plan: `Noop` and the tier-1 path are fully
functional without it, and every downstream behaviour (rate limits, storage,
queries) is already exercised at Tier 1.

- [ ] **Step 4: Write the token handler**

`services/ingest/internal/handler/token.go`:

```go
package handler

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/attest"
)

type TokenDeps struct {
	Minter   *tenant.Minter
	Attestor attest.Attestor

	// ResolveTenant maps a public client ID to a tenant. Returns an error for
	// unknown or revoked IDs.
	ResolveTenant func(ctx context.Context, clientID string) (string, error)

	// IssueInstall returns the server-side install identity. At Tier 0 it is
	// derived from the attestation subject and is stable across app restarts;
	// at Tier 1 it is minted and persisted server-side.
	IssueInstall func(ctx context.Context, tenantID, platform, subject string, tier uint8) (string, error)

	Now func() time.Time
}

// NewToken serves POST /v1/auth/token: the public client ID plus a platform
// attestation are exchanged for a short-lived scoped JWT.
//
// The embedded client ID authorizes nothing on its own. Anything shipped inside
// a third party's mobile binary is extractable — that is a certainty, not a
// risk — so its extraction is designed to be boring.
func NewToken(d TokenDeps) http.Handler {
	if d.Now == nil {
		d.Now = func() time.Time { return time.Now().UTC() }
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			httpError(w, http.StatusBadRequest, "unreadable body")
			return
		}

		var req trackingv1.TokenRequest
		// DiscardUnknown is what makes a client-supplied install_id a no-op:
		// the field does not exist in TokenRequest, so it is dropped here and
		// can never reach IssueInstall.
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, &req); err != nil {
			httpError(w, http.StatusBadRequest, "malformed request")
			return
		}

		tenantID, err := d.ResolveTenant(r.Context(), req.GetClientId())
		if err != nil {
			httpError(w, http.StatusUnauthorized, "unknown client_id")
			return
		}

		subject, ok := d.Attestor.Verify(r.Context(), req.GetPlatform(), req.GetAttestation())
		tier := uint8(1)
		if ok {
			tier = 0
		}

		installID, err := d.IssueInstall(r.Context(), tenantID, req.GetPlatform(), subject, tier)
		if err != nil {
			httpError(w, http.StatusServiceUnavailable, "install issuance unavailable")
			return
		}

		token, expiresIn, err := d.Minter.Mint(tenant.Claims{
			TenantID:  tenantID,
			InstallID: installID,
			Scope:     tenant.ScopeWriteEvents,
			TrustTier: tier,
		}, d.Now())
		if err != nil {
			httpError(w, http.StatusInternalServerError, "mint failed")
			return
		}

		out, err := protojson.Marshal(&trackingv1.TokenResponse{
			AccessToken: token,
			ExpiresIn:   expiresIn,
			TrustTier:   uint32(tier),
		})
		if err != nil {
			httpError(w, http.StatusInternalServerError, "encode")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(out)
	})
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd services/ingest && go test ./... -v`
Expected: PASS, all handler and attest tests.

- [ ] **Step 6: Commit**

```bash
git add services/ingest/internal/attest services/ingest/internal/handler
git commit -m "feat(ingest): token exchange with attestation-as-trust-tier"
```

---

### Task 11: Legacy write-key dual-accept

**Files:**
- Create: `pkg/tenant/legacy.go`
- Test: `pkg/tenant/legacy_test.go`

**Interfaces:**
- Consumes: `Claims`, `ErrExpired` and friends.
- Produces:
  - `type LegacyMode string` with consts `ModeDualAccept`, `ModeDeprecating`, `ModeCutoff`
  - `type LegacyResolver interface { Resolve(ctx context.Context, key string) (tenantID string, mode LegacyMode, err error) }`
  - `func (v *Verifier) VerifyOrLegacy(ctx context.Context, bearer string, now time.Time, lr LegacyResolver) (Claims, bool, error)` — the bool reports "this was a legacy credential", so the caller can apply the tighter `rps_legacy` limit and emit the deprecation metric.

- [ ] **Step 1: Write the failing test**

`pkg/tenant/legacy_test.go`:

```go
package tenant_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

type stubResolver struct {
	tenantID string
	mode     tenant.LegacyMode
	err      error
}

func (s stubResolver) Resolve(context.Context, string) (string, tenant.LegacyMode, error) {
	return s.tenantID, s.mode, s.err
}

func TestLegacyKeyAcceptedInDualAcceptMode(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	claims, isLegacy, err := v.VerifyOrLegacy(context.Background(), "Bearer wk_live_abc123", time.Now(),
		stubResolver{tenantID: "t1", mode: tenant.ModeDualAccept})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !isLegacy {
		t.Error("isLegacy = false, want true so the caller applies rps_legacy and logs the deprecation")
	}
	if claims.TenantID != "t1" {
		t.Errorf("tenant = %q, want t1", claims.TenantID)
	}
	// A legacy key has no attestation and no install identity. Tier 1 with a
	// key-derived install bucket is the honest representation.
	if claims.TrustTier != 1 {
		t.Errorf("trust_tier = %d, want 1", claims.TrustTier)
	}
	if claims.InstallID == "" {
		t.Error("no install_id derived; the rate limiter needs a bucket")
	}
}

// Cutoff is the whole point of the migration: a leaked key must stop working as
// an ingest bearer credential, or the short token lifetime buys nothing.
func TestLegacyKeyRejectedAtCutoff(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	_, _, err := v.VerifyOrLegacy(context.Background(), "Bearer wk_live_abc123", time.Now(),
		stubResolver{tenantID: "t1", mode: tenant.ModeCutoff})
	if !errors.Is(err, tenant.ErrLegacyCutoff) {
		t.Errorf("err = %v, want ErrLegacyCutoff", err)
	}
}

func TestJWTPathUnaffectedByLegacyResolver(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	claims, isLegacy, err := v.VerifyOrLegacy(context.Background(), mint(t, priv, "kid-1", nil), time.Now(),
		stubResolver{err: errors.New("resolver must not be consulted")})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if isLegacy {
		t.Error("isLegacy = true for a JWT")
	}
	if claims.TenantID != "t1" {
		t.Errorf("tenant = %q, want t1", claims.TenantID)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/tenant && go test ./... -run Legacy`
Expected: FAIL — `undefined: tenant.ModeDualAccept`.

- [ ] **Step 3: Write the implementation**

`pkg/tenant/legacy.go`:

```go
package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// LegacyPrefix identifies the pre-token write keys. The same value becomes the
// bootstrap client_id at the exchange endpoint — it just stops working as an
// ingest bearer credential.
const LegacyPrefix = "wk_live_"

var ErrLegacyCutoff = errors.New("tenant: legacy write key no longer accepted for ingest")

type LegacyMode string

const (
	ModeDualAccept  LegacyMode = "dual_accept"
	ModeDeprecating LegacyMode = "deprecating"
	ModeCutoff      LegacyMode = "cutoff"
)

// LegacyResolver maps a legacy write key to its tenant and that tenant's
// current cutover mode. The mode is per-tenant config, not a deploy flag, so a
// stranded customer can be rolled back to dual-accept without a release.
type LegacyResolver interface {
	Resolve(ctx context.Context, key string) (tenantID string, mode LegacyMode, err error)
}

// VerifyOrLegacy accepts either a JWT or, during the cutover, a legacy write
// key. The second return value reports whether the legacy path was taken so the
// caller can apply the tighter rps_legacy limit and count the usage per tenant
// and SDK version — that count is what tells you when cutoff is safe.
func (v *Verifier) VerifyOrLegacy(ctx context.Context, bearer string, now time.Time, lr LegacyResolver) (Claims, bool, error) {
	raw := strings.TrimPrefix(bearer, "Bearer ")

	if !strings.HasPrefix(raw, LegacyPrefix) {
		c, err := v.Verify(ctx, bearer, now)
		return c, false, err
	}

	tenantID, mode, err := lr.Resolve(ctx, raw)
	if err != nil {
		return Claims{}, true, fmt.Errorf("resolve legacy key: %w", err)
	}
	if mode == ModeCutoff {
		return Claims{}, true, ErrLegacyCutoff
	}

	// A legacy key carries no attestation and no install identity. Tier 1 with
	// a key-derived bucket is the honest representation: it is rate limited
	// below Tier 1 by the caller's rps_legacy, and it is never sampled.
	sum := sha256.Sum256([]byte(raw))
	return Claims{
		TenantID:  tenantID,
		InstallID: "legacy-" + hex.EncodeToString(sum[:8]),
		Scope:     ScopeWriteEvents,
		TrustTier: 1,
	}, true, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd pkg/tenant && go test ./... -v`
Expected: PASS, 13 tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/tenant
git commit -m "feat(tenant): staged legacy write-key cutover with per-tenant cutoff"
```

---

### Task 12: Service wiring, config, and end-to-end test

**Files:**
- Create: `services/ingest/cmd/main.go`
- Create: `services/ingest/internal/handler/e2e_test.go`
- Create: `deploy/docker-compose.yml`
- Modify: `.github/workflows/ci.yml` — add an `ingest` job

**Interfaces:**
- Consumes: everything above.
- Produces: a runnable binary; `docker compose up` brings up ClickHouse, Postgres, Redis, and ingest.

- [ ] **Step 1: Write the failing end-to-end test**

`services/ingest/internal/handler/e2e_test.go`:

```go
//go:build e2e

package handler_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
)

// Exchange a token, post a batch, read it back out of a real ClickHouse.
// A green unit suite can still ship a service that has never written a row.
func TestExchangeThenIngestThenQuery(t *testing.T) {
	ctx := context.Background()
	env := startFullStack(t) // ClickHouse + Redis + wired handlers

	tokenRec := postJSON(t, env.Token, "/v1/auth/token",
		`{"clientId":"pk_live_test","platform":"android","attestation":""}`)
	if tokenRec.Code != http.StatusOK {
		t.Fatalf("token exchange: %d %s", tokenRec.Code, tokenRec.Body)
	}
	var tok trackingv1.TokenResponse
	decode(t, tokenRec.Body.Bytes(), &tok)
	if tok.TrustTier != 1 {
		t.Errorf("trust_tier = %d, want 1 (Noop attestor)", tok.TrustTier)
	}

	body := batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-0000000000e1", "name": "checkout", "deviceId": "d1", "sessionId": "s1",
			"userId": "u1", "tsClient": "1754092800000",
			"props": map[string]any{"amount": map[string]any{"numberValue": 19.99}}},
	})
	rec := postWithToken(t, env.Batch, body, tok.AccessToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("batch: %d %s", rec.Code, rec.Body)
	}

	var n uint64
	err := env.CH.QueryRow(ctx,
		`SELECT count() FROM events WHERE tenant_id = ? AND event_id = ?`,
		"t-test", "0191f4a2-1c3d-7000-8000-0000000000e1").Scan(&n)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Errorf("stored rows = %d, want 1", n)
	}

	// Trust tier must be persisted on the row, not just in the token.
	var tier uint8
	if err := env.CH.QueryRow(ctx,
		`SELECT trust_tier FROM events WHERE event_id = ?`,
		"0191f4a2-1c3d-7000-8000-0000000000e1").Scan(&tier); err != nil {
		t.Fatalf("read tier: %v", err)
	}
	if tier != 1 {
		t.Errorf("stored trust_tier = %d, want 1", tier)
	}
}

// Replaying the identical batch must be a no-op from the client's perspective:
// the same IDs come back accepted, and after a merge there is still one row.
func TestReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	env := startFullStack(t)
	token := exchangeToken(t, env)

	body := batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-0000000000e2", "name": "checkout", "deviceId": "d1", "sessionId": "s1",
			"tsClient": "1754092800000"},
	})

	for i := 0; i < 3; i++ {
		rec := postWithToken(t, env.Batch, body, token)
		if rec.Code != http.StatusOK {
			t.Fatalf("attempt %d: %d %s", i, rec.Code, rec.Body)
		}
		var resp trackingv1.BatchResponse
		decode(t, rec.Body.Bytes(), &resp)
		if len(resp.Accepted) != 1 {
			t.Fatalf("attempt %d accepted = %v, want 1", i, resp.Accepted)
		}
	}

	if err := env.CH.Exec(ctx, `OPTIMIZE TABLE events FINAL`); err != nil {
		t.Fatalf("optimize: %v", err)
	}
	var n uint64
	if err := env.CH.QueryRow(ctx, `SELECT count() FROM events WHERE event_id = ?`,
		"0191f4a2-1c3d-7000-8000-0000000000e2").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("rows after merge = %d, want 1 — the ts must be stable across replays", n)
	}
	_ = clickhouse.Row{}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd services/ingest && go test -tags e2e ./internal/handler/...`
Expected: FAIL — `undefined: startFullStack`.

- [ ] **Step 3: Write `main.go`**

`services/ingest/cmd/main.go`:

```go
// Command ingest serves POST /v1/auth/token and POST /v1/batch.
//
// The service is stateless: scale it horizontally and it costs nothing but
// pods. All buffering lives either on the client (the outbox) or in
// ClickHouse's async insert queue — never in this process.
package main

import (
	"context"
	"crypto/ed25519"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/attest"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/handler"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
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

	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()

	issuer := env("TOKEN_ISSUER", "https://issuer.local")
	audience := env("TOKEN_AUDIENCE", "https://ingest.local")

	priv, kid := loadSigningKey(log)
	minter := tenant.NewMinter(kid, priv, issuer, audience, 45*time.Minute)
	verifier := tenant.NewVerifier(env("JWKS_URL", issuer+"/.well-known/jwks.json"), issuer, audience, nil)

	checker := quota.NewChecker(rdb)

	mux := http.NewServeMux()
	mux.Handle("POST /v1/auth/token", handler.NewToken(handler.TokenDeps{
		Minter:        minter,
		Attestor:      attest.Noop{},
		ResolveTenant: pgResolveTenant(ctx),
		IssueInstall:  pgIssueInstall(ctx),
	}))
	mux.Handle("POST /v1/batch", handler.NewBatch(handler.Deps{
		Verifier:  verifier,
		Offsets:   enrich.NewMemoryOffsetStore(),
		Quota:     checker,
		LimitsFor: pgLimitsFor(ctx),
		Insert: func(ctx context.Context, rows []clickhouse.Row) error {
			return clickhouse.InsertEvents(ctx, conn, rows)
		},
	}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              env("LISTEN_ADDR", ":8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// Generous: wait_for_async_insert=1 can add ~1s, and a slow mobile
		// upload on a bad connection is normal, not an attack.
		WriteTimeout: 30 * time.Second,
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
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func loadSigningKey(log *slog.Logger) (ed25519.PrivateKey, string) {
	// Reads the active signing key from Postgres in production. For local dev,
	// generate an ephemeral one — tokens do not need to survive a restart.
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		log.Error("keygen", "err", err)
		os.Exit(1)
	}
	return priv, "dev-kid-1"
}
```

`pgResolveTenant`, `pgIssueInstall`, and `pgLimitsFor` are thin `pgx` queries
against the Task 3 schema. Write them in `services/ingest/cmd/postgres.go`;
each is a single `QueryRow` with the SQL inlined.

- [ ] **Step 4: Write the compose file**

`deploy/docker-compose.yml`:

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.8-alpine
    environment:
      CLICKHOUSE_DB: tracking
    ports: ["8123:8123", "9000:9000"]
    ulimits:
      nofile: { soft: 262144, hard: 262144 }

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: control
      POSTGRES_PASSWORD: dev
    ports: ["5432:5432"]

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  ingest:
    build:
      context: ..
      dockerfile: deploy/ingest.Dockerfile
    environment:
      CLICKHOUSE_ADDRS: clickhouse:9000
      REDIS_ADDR: redis:6379
      LISTEN_ADDR: ":8080"
    ports: ["8080:8080"]
    depends_on: [clickhouse, postgres, redis]
```

- [ ] **Step 5: Add the CI job**

Append to `.github/workflows/ci.yml`:

```yaml
  ingest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.23'
      - name: Unit tests
        run: cd services/ingest && go test ./...
      - name: Integration tests
        run: |
          cd pkg/clickhouse && go test ./...
          cd ../../services/ingest && go test -tags e2e ./...
```

- [ ] **Step 6: Run everything**

Run: `go build ./... && cd services/ingest && go test ./... && go test -tags e2e ./...`
Expected: build clean, all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add services/ingest deploy .github/workflows/ci.yml go.work
git commit -m "feat(ingest): wire the service, add compose stack and e2e tests"
```

---

### Task 13: Document the ingest service

**Files:**
- Modify: `README.md`
- Create: `services/ingest/README.md`

- [ ] **Step 1: Write `services/ingest/README.md`**

````markdown
# Ingest Service

`POST /v1/auth/token` and `POST /v1/batch`.

## Status code contract

The SDK's entire retry logic keys off these, and the SDK cannot be
force-upgraded. Changing one is a breaking change.

| Status | Meaning | Client action |
|---|---|---|
| `200` | Batch processed (may contain rejects) | Delete accepted, mark rejected `dead` |
| `400` | Batch envelope malformed | Drop batch, do not retry |
| `401` | Token expired or invalid | Re-exchange, retry once; if the exchange fails, stop syncing and surface to the host app |
| `413` | Batch too large | Halve batch size, retry. Single event already? Mark it `dead` |
| `429` | Quota or rate limit | Retry after `Retry-After` |
| `5xx` | Server or ClickHouse fault | Retry with backoff |

## What a 200 guarantees

`wait_for_async_insert=1` means the data was flushed to the ClickHouse node
that received the insert. It does **not** wait for replication or a backup.

At single-node scale, "flushed" and "durable" are the same thing — but after
the `200` the client deletes its copy, so if that node loses flushed data
before replication exists, the event is gone with no retry path and no
counter. This is **accepted risk at single-node scale**, not something the
outbox rescues. Adding a replica with `insert_quorum`, or node-level backups,
is the fix.

## Trust tiers

| Tier | Meaning | Treatment |
|---|---|---|
| 0 | Attested (App Attest / Play Integrity) | Normal rate limits |
| 1 | Attestation unavailable | Tighter rate limits. **Never sampled.** |

Sampling is forbidden here. The client deletes the outbox row on a `200`, so a
silently discarded event has no retry and no counter. A `429` degrades into
ordinary counted loss instead — same cost control, no hole in the guarantee.

Rooted, custom-ROM, and de-Googled devices fail attestation legitimately;
simulators fail; Play Integrity is quota-limited. Blocking would buy little
and cost real users.

## Rate limit key order

`install_id` first, tenant second, IP last. IP-primary limiting is actively
harmful in this market: Indonesian carriers CGNAT aggressively, so one
Telkomsel egress IP is thousands of real users. IP survives only as a coarse
anomaly signal.

## Local development

```bash
docker compose -f deploy/docker-compose.yml up -d
go run ./services/ingest/cmd
```

## Tests

```bash
go test ./services/ingest/...              # unit
go test -tags e2e ./services/ingest/...    # needs Docker
```
````

- [ ] **Step 2: Link it from the root README**

Add to `README.md` under a `## Services` heading:

```markdown
## Services

- [`services/ingest`](services/ingest/README.md) — token exchange and event ingestion
```

- [ ] **Step 3: Verify the documented commands run**

Run: `go test ./services/ingest/...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add README.md services/ingest/README.md
git commit -m "docs(ingest): document the status contract, durability, and trust tiers"
```

---

## Completion criteria

- `go test ./...` and `go test -tags e2e ./services/ingest/...` pass.
- `golangci-lint run ./...` clean.
- `make check-boundaries` passes — ingest imports no other service.
- A batch containing one poison event returns `200` with a per-event reject and
  stores the rest.
- Replaying an identical batch three times leaves one row after `OPTIMIZE FINAL`.
- A token with `alg: none`, an HMAC signature, a wrong `aud`, a wrong `iss`, a
  missing scope, or an unknown `kid` is rejected with `401`.

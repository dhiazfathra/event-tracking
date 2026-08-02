# Event Tracking Platform — Design

**Date:** 2026-08-02
**Status:** Approved

## Constraints

These four answers drove every decision below. If one changes, re-read this document.

| Constraint | Value | Consequence |
|---|---|---|
| Tenancy | Third-party SaaS | Write keys, server-derived `tenant_id`, per-tenant quotas, untrusted clients |
| Scale target | ~1k events/sec (~86M/day, ~2.6B/month) | No Kafka. ClickHouse async inserts are the buffer |
| Delivery | At-least-once, dedup on read | Client retries freely; `uniqExact(event_id)` at query time |
| Client sync | Events + local query cache | Append-only writes (no conflicts) + a read-through cache (server authoritative) |
| Query consumers | Customer-facing analytics API | Constrained JSON DSL compiled to SQL. Never raw SQL passthrough |

---

## 1. High-Level Architecture

```
┌──────────────────────────┐
│  Flutter app + SDK       │
│  ┌────────────────────┐  │
│  │ drift / SQLite     │  │
│  │  outbox            │  │  append-only, durable
│  │  query_cache       │  │  read-through, etag
│  └────────────────────┘  │
└────────┬─────────────▲───┘
         │             │
  POST /v1/batch   POST /v1/query/*
  (JSON + gzip)    (JSON DSL)
         │             │
┌────────▼─────────────┴───────────────┐
│  Go services                         │
│  ┌──────────┐      ┌──────────────┐  │
│  │ ingest   │      │ query        │  │
│  │ validate │      │ DSL→SQL      │  │
│  │ enrich   │      │ tenant guard │  │
│  │ quota    │      │ result caps  │  │
│  └────┬─────┘      └──────┬───────┘  │
└───────┼───────────────────┼──────────┘
        │ async_insert      │ SELECT
        ▼                   ▼
┌──────────────────────────────────────┐
│  ClickHouse                          │
│   events            (ReplacingMergeTree, source of truth)
│   events_daily      (AggregatingMergeTree, approximate rollup)
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│  Postgres (control plane)            │
│   tenants, write_keys, read_keys,    │
│   quotas, event schema registry      │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│  Redis — quota counters, key cache   │
└──────────────────────────────────────┘
```

**The load-bearing idea:** the client's durable outbox *is* the ingestion queue. Because every phone holds its own unsent events on disk and retries with backoff, the backend does not need a separate durable buffer tier. ClickHouse down means the ingest service returns `503`, the client keeps the events, and they arrive later. This is what lets a multi-tenant ingestion platform run without Kafka at this scale.

**Data flow, write path:**
1. App calls `Tracker.track(name, props)`.
2. SDK assigns `event_id` (UUID v7) and a per-device monotonic `seq`, writes one row to SQLite, returns. Never blocks, never throws into app code.
3. A flush trigger (timer / threshold / lifecycle / connectivity regain) claims up to 500 pending rows in a transaction and marks them `inflight`.
4. `POST /v1/batch` with gzip.
5. Ingest authenticates the write key, resolves `tenant_id`, validates, corrects clock skew, checks quota, inserts to ClickHouse with `async_insert=1, wait_for_async_insert=1`.
6. Response reports accepted IDs and per-event rejects. Client deletes accepted, marks rejects `dead`, returns the rest to `pending` with backoff.

**Data flow, read path:**
1. App requests an aggregate. SDK checks `query_cache`; if fresh, returns immediately.
2. Otherwise `POST /v1/query/timeseries` (etc.) with `If-None-Match` from the cached etag.
3. Query service compiles the DSL to parameterized SQL with `tenant_id` injected server-side, runs it under a per-tenant settings profile, returns result + etag + `computed_at`.
4. SDK caches and returns. `304` refreshes the TTL without transferring a body.

---

## 2. Monorepo Structure

```
event-tracking/
├── buf.yaml
├── buf.gen.yaml
├── go.work                          # services/* + pkg/* + gen/go
│
├── proto/
│   └── tracking/v1/
│       ├── event.proto              # envelope, property value union
│       ├── ingest.proto             # batch request/response
│       └── query.proto              # query DSL + result shapes
│
├── gen/                             # generated, committed to git
│   ├── go/tracking/v1/
│   └── dart/tracking/v1/
│
├── services/
│   ├── ingest/                      # POST /v1/batch
│   │   ├── cmd/main.go
│   │   └── internal/{handler,validate,enrich,quota}/
│   └── query/                       # POST /v1/query/*
│       ├── cmd/main.go
│       └── internal/{handler,compile,execute}/
│
├── pkg/                             # shared Go, no service may import another service
│   ├── clickhouse/                  # pool, retry, async-insert settings
│   ├── tenant/                      # key → tenant resolution + cache
│   ├── querydsl/                    # DSL AST → parameterized SQL
│   └── testsupport/                 # testcontainers helpers, golden fixtures
│
├── clients/
│   ├── flutter_sdk/                 # pub package: event_tracking
│   │   └── lib/src/{tracker,outbox,sync,cache,transport}/
│   └── example_app/
│
├── migrations/
│   ├── clickhouse/                  # goose-style .sql, forward-only
│   └── postgres/
│
├── testdata/golden/                 # shared JSON fixtures, read by Go AND Dart tests
├── deploy/
└── docs/
    ├── decisions/                   # ADRs
    └── superpowers/specs/
```

### Module boundaries

Three rules, enforced in CI:

1. **`services/*` may not import each other.** They share only `pkg/*` and `gen/go`. If ingest and query need the same logic, it moves to `pkg/`.
2. **`clients/flutter_sdk` and `services/*` share exactly one thing: `gen/`.** No hand-written duplicate of the envelope on either side.
3. **`pkg/*` may not import `services/*`.** Dependency direction is one-way.

### Shared contracts

Protobuf in `proto/` is the single source of truth. `buf generate` produces Go and Dart types; both are committed so neither a Go build nor a `flutter pub get` requires the proto toolchain.

**The wire format is JSON, not protobuf binary.** Proto is used for schema definition and codegen only. Reasons: JSON+gzip is debuggable through any proxy, needs no gRPC stack on mobile, and at 1k eps the byte savings are irrelevant. The cost is proto-JSON mapping quirks (64-bit ints as strings, well-known type encodings) — pinned by the golden fixtures in `testdata/golden/`, which both the Go tests and the Dart tests read.

`buf breaking` runs in CI against the previous release tag. This is the mechanism that matters most: an SDK deployed to third-party apps cannot be force-upgraded, so a breaking envelope change is unfixable once shipped.

**Tradeoff:** committing generated code creates merge noise and requires a `make gen && git diff --exit-code` CI check. The alternative — generating at build time — adds `buf` to every contributor's machine and to the Flutter build, which is worse for an SDK consumed by outsiders.

---

## 3. Backend

### 3.1 Event envelope

```protobuf
message Event {
  string event_id     = 1;  // UUID v7, client-generated. Dedup key + rough time ordering.
  string name         = 2;
  int64  ts_client    = 3;  // client wall clock, millis. Untrusted.
  uint64 seq          = 4;  // per-device monotonic. Gap detection only.
  string device_id    = 5;
  string session_id   = 6;
  string anonymous_id = 7;
  string user_id      = 8;  // empty until identify()
  map<string, Value> props = 9;
  Context context     = 10; // sdk_version, app_version, os, os_version, locale
}
```

`tenant_id` is deliberately absent. It is resolved server-side from the write key. A client-supplied tenant field is a cross-tenant write primitive.

UUID v7 rather than v4: time-ordered UUIDs cluster in the ORDER BY tail and compress far better than random ones, and give a usable secondary sort when two events share a millisecond.

### 3.2 Ingestion API

```
POST /v1/batch
Authorization: Bearer wk_live_...
Content-Encoding: gzip
Content-Type: application/json

{ "sent_at": 1754092800000, "events": [ ...≤500 events, ≤1 MB decompressed... ] }
```

Response is **always partial-success shaped**:

```json
{
  "received_at": 1754092800412,
  "accepted": ["0191f4a2-...", "0191f4a3-..."],
  "rejected": [
    { "event_id": "0191f4a4-...", "code": "INVALID_PROPERTY_TYPE", "message": "props.amount: expected number" }
  ]
}
```

**Why this matters more than it looks:** if one malformed event fails the whole batch with a 4xx, the client retries that batch forever, the outbox never drains, and every subsequent event is stuck behind it. A single poison event silently kills a user's entire telemetry stream. Per-event rejection is the difference between a bug and an outage.

**Status code contract** — the client's entire retry logic keys off this, so it is part of the public contract:

| Status | Meaning | Client action |
|---|---|---|
| `200` | Batch processed (may contain rejects) | Delete accepted, mark rejected `dead` |
| `400` | Batch envelope malformed | Drop batch, do not retry |
| `401` | Bad write key | Stop syncing, surface to host app |
| `413` | Batch too large | Halve batch size, retry |
| `429` | Quota or rate limit | Retry after `Retry-After` |
| `5xx` | Server or ClickHouse fault | Retry with backoff |
| timeout | Unknown | Retry — safe, because `event_id` is stable |

### 3.3 Enrichment and trust

- **`tenant_id`** — from the write key. Cached in-process, 60s TTL, negative-cached to survive credential-stuffing.
- **Clock skew correction** — `ts = ts_client + (received_at_server − sent_at_client)`. Mobile clocks are wrong by minutes to years. Both `ts_client` (raw) and `ts` (corrected) are stored; `ts` is what queries use.
- **Skew clamp** — corrected timestamps more than 24h in the future or 30d in the past are clamped to `received_at` and flagged. Without this a single device with a broken clock creates partitions years out and wrecks the partition count.
- **`ts_received`** — server-set, also the `ReplacingMergeTree` version column.
- **Quota** — Redis counter per `(tenant_id, day)`. Over quota returns `429`, and the client's backoff keeps the retry pressure bounded.

### 3.4 Batching strategy

Batching happens in three places, deliberately:

1. **Client-side** — up to 500 events per HTTP request. Amortizes TLS and the request-per-event overhead. This is the batching that matters most, because it is where the network is.
2. **ClickHouse async inserts** — `async_insert=1`, `wait_for_async_insert=1`, `async_insert_busy_timeout_ms=1000`, `async_insert_max_data_size=10000000`. ClickHouse coalesces concurrent small inserts server-side into large parts. This is what makes many ingest pods writing independently safe: without it, thousands of small parts per minute would push the table into "too many parts."
3. **No in-process buffering in the ingest service.** A Go-side accumulator would create a window where the service has acked events it has not durably written — losing them on a pod restart while the client has already deleted them. `wait_for_async_insert=1` is the whole point: the `200` means durable.

**Tradeoff:** `wait_for_async_insert=1` adds up to ~1s to p99 ingest latency. `=0` acks in microseconds but silently drops data if ClickHouse rejects the insert, which is incompatible with promising at-least-once. Latency is the correct thing to trade here — nobody is watching an ingestion request finish.

### 3.5 ClickHouse schema

```sql
CREATE TABLE events
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

    props         JSON,

    INDEX idx_user  user_id  TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_sess  session_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ts_received)
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, name, ts, event_id)
TTL event_date + INTERVAL 13 MONTH DELETE
SETTINGS index_granularity = 8192;
```

**ORDER BY `(tenant_id, name, ts, event_id)`:**

- `tenant_id` first — every single query is tenant-scoped. Putting it first makes tenant isolation a primary-key prefix scan rather than a filter, which is the difference between reading one tenant's data and reading everyone's.
- `name` second — dashboard queries almost always pin one event name (`"checkout_completed" over time`). Low cardinality, so this stays a cheap prefix.
- `ts` third — the actual range scan dimension.
- `event_id` last — makes the sort key unique, which `ReplacingMergeTree` needs to collapse duplicates rather than the row it thinks is a duplicate.

**The cost of that choice, stated plainly:** any query that filters by user *without* pinning an event name (`"everything user X did"` — the session-replay / debugging shape) cannot use the primary key past `tenant_id` and degrades to a partition scan. The bloom filter helps but is not a substitute. If that access pattern becomes common, add:

```sql
ALTER TABLE events ADD PROJECTION by_user
  (SELECT * ORDER BY (tenant_id, user_id, ts));
```

That roughly doubles storage for the table. It is deliberately *not* in the initial schema — add it when a real query pattern demands it, not speculatively.

**PARTITION BY month:** 13-month TTL gives 13 partitions. Daily partitioning would give ~400, more merge pressure, and no query benefit that the `ts` sort key does not already provide. The tradeoff is coarse deletion granularity — dropping one bad day, or one tenant's data on request, becomes an `ALTER TABLE ... DELETE` mutation (slow, rewrites parts) rather than an instant `DROP PARTITION`. Given GDPR deletion requests are per-user and would need a mutation regardless, monthly is the right call.

**`props JSON`:** ClickHouse's native JSON type gives dynamic subcolumns without a schema migration per property. Alternative considered: `Map(String, String)` + `Map(String, Float64)`, which is more predictable but forces every numeric filter through a cast and makes typed aggregation awkward. JSON's cost is less predictable storage and a column explosion if a tenant sends high-cardinality keys — mitigated by rejecting events with >50 properties at ingest.

### 3.6 Dedup on read

The client retries on ambiguous failures, so duplicates are expected by design. Handling:

- **Query layer** uses `uniqExact(event_id)` and `count(DISTINCT event_id)` rather than `count(*)`.
- **`ReplacingMergeTree(ts_received)`** collapses duplicates during background merges, so storage does not grow unboundedly.
- **`FINAL` is never used.** It forces a merge at query time and is the single most common way to make a ClickHouse dashboard slow.

**Tradeoff:** `uniqExact` over a large set costs memory relative to `count(*)`. For very large ranges, `uniq()` (HyperLogLog, ~0.5% error) is the escape hatch, exposed as an explicit `approximate: true` flag in the query DSL rather than a silent substitution. Silently swapping in an approximate count is how dashboards start lying.

### 3.7 Rollups

```sql
CREATE TABLE events_daily
(
    tenant_id  LowCardinality(String),
    event_date Date,
    name       LowCardinality(String),
    events     SimpleAggregateFunction(sum, UInt64),
    users      AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, event_date, name);

CREATE MATERIALIZED VIEW events_daily_mv TO events_daily AS
SELECT tenant_id, event_date, name, count() AS events, uniqState(user_id) AS users
FROM events GROUP BY tenant_id, event_date, name;
```

**Rollups are approximate and this is documented, not hidden.** A materialized view fires on the inserted block; it never sees the `ReplacingMergeTree` deduplication that happens later during merges. Duplicate events therefore inflate rollup counts. The raw `events` table is the only source of truth. The query service routes to rollups only for coarse queries (daily totals over long ranges) and always labels the response `"source": "rollup"`.

The alternative — rebuilding rollups nightly from the deduplicated raw table — is correct but adds a scheduled job and a consistency window. Deferred until rollup drift is measured to actually matter.

### 3.8 Scaling path

The design is honest about what it is: a single-cluster design sized for the stated 1k eps, with named triggers for each next step.

| Bottleneck | Symptom | Response |
|---|---|---|
| Ingest CPU/network | p99 latency climbs, pods saturated | Scale ingest pods horizontally — they are stateless. Free. |
| ClickHouse insert throughput | "too many parts", merge lag | Raise `async_insert_max_data_size`, then add a replica. |
| Query contention with ingest | Dashboards slow during traffic peaks | Split reads to a dedicated replica. |
| Single-node storage/CPU | Node saturated regardless of tuning | Shard on `cityHash64(tenant_id)`; `Distributed` table over shards. `tenant_id` leading the sort key means a tenant lives on one shard — no scatter-gather for normal queries. |
| Need stream replay or a 2nd consumer | Schema bug requires reprocessing; someone wants a real-time stream | **Now** add Kafka/Redpanda between ingest and ClickHouse. |

**Why Kafka is absent at the start:** at 1k eps it buys a buffer the system already has (the client outbox) at the price of a second cluster, a consumer service, partition/offset/lag operations, and a rebalance failure mode. The three things that genuinely justify it — replay after a bad deploy, multiple consumers of the raw stream, and accepting writes while ClickHouse is down — are all either absent or already solved by the client holding its own events. Listed as triggers rather than built now.

---

## 4. Flutter Client

### 4.1 Local storage: drift (SQLite)

| Option | Verdict |
|---|---|
| **drift / SQLite** | **Chosen.** Transactional claim-and-delete, indexed range scans on `(state, next_attempt_at)`, real migrations, present on every device. An outbox is exactly a queue table — the relational shape is the right one. |
| Isar | Faster raw object writes, but the outbox is not write-throughput-bound (a busy app writes tens of events/sec, not thousands), the migration story is weaker, and project maintenance has been unstable — a bad property for a dependency shipping inside third-party apps. |
| ObjectBox | Fast, but a native binary in every consuming app (size + platform support) and licensing friction for a redistributable SDK. |
| Hive | No transactions, no queries. An outbox without transactional claim loses or duplicates events on crash. Wrong tool. |
| Plain `sqflite` | Would work. drift adds typed queries and managed migrations for one build-time dependency. |

**Cost accepted:** drift pulls `build_runner` into the SDK's build and adds generated files. For a package third parties depend on, migration safety is worth more than build simplicity.

```sql
CREATE TABLE outbox (
  event_id        TEXT PRIMARY KEY,     -- UUID v7
  seq             INTEGER NOT NULL,
  payload         BLOB    NOT NULL,     -- serialized envelope
  created_at      INTEGER NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  state           TEXT    NOT NULL      -- pending | inflight | dead
);
CREATE INDEX outbox_ready ON outbox(state, next_attempt_at, seq);

CREATE TABLE query_cache (
  cache_key   TEXT PRIMARY KEY,
  body        BLOB    NOT NULL,
  etag        TEXT,
  computed_at INTEGER NOT NULL,   -- server-side computation time
  fetched_at  INTEGER NOT NULL,
  ttl_ms      INTEGER NOT NULL
);

CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
-- 'seq', 'dropped_events'
```

`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`

### 4.2 Capture path

`Tracker.track(name, props)` builds the event, does one `INSERT`, and returns. Three non-negotiable properties:

- **Never blocks the caller meaningfully.** One WAL insert with `synchronous=NORMAL` is sub-millisecond.
- **Never throws into host app code.** A telemetry SDK that can crash the app it measures is a liability. All errors are swallowed and counted.
- **No in-memory buffer.** An accumulator flushed every ~200ms would save a few fsyncs and create a window where a crash loses the events that most need capturing — the ones right before the crash. WAL mode makes immediate writes cheap enough that the buffer earns nothing.

**Bounded outbox:** capped at 10,000 events. On overflow the oldest `pending` rows are dropped and `counters.dropped_events` is incremented; that counter ships with the next batch as an `sdk_dropped_events` event. An unbounded outbox on a long-offline device fills the host app's storage and gets the app uninstalled. Losing the oldest telemetry is the correct failure — but it must be *visible*, hence the counter.

### 4.3 Sync engine

**Flush triggers:** periodic timer (30s while foregrounded), pending count ≥ 50, app lifecycle transition to background, connectivity regained (`connectivity_plus`), explicit `Tracker.flush()`.

Connectivity events are a *hint*, never proof — captive portals report connected. Only an HTTP success counts as success.

**Send algorithm:**

```
1. BEGIN IMMEDIATE
     SELECT * FROM outbox
      WHERE state='pending' AND next_attempt_at <= now
      ORDER BY seq LIMIT 500;
     UPDATE those SET state='inflight';
   COMMIT
2. POST /v1/batch
3. on 200:
     DELETE WHERE event_id IN (accepted)
     UPDATE state='dead' WHERE event_id IN (rejected)
4. on 429/5xx/timeout:
     UPDATE state='pending', attempts=attempts+1,
            next_attempt_at = now + backoff(attempts)
5. on 400/401: per the status contract above
```

**`inflight` is persisted, not in-memory.** If the process dies mid-request, an in-memory marker loses the events entirely. On SDK init, any `inflight` row older than 5 minutes is reset to `pending`. This resend after a crash is safe precisely because `event_id` is stable and the server dedups on read — the at-least-once choice is what makes crash recovery a two-line fix instead of a distributed-systems problem.

**Backoff:** exponential from 2s, doubling, capped at 5 minutes, with ±25% jitter. Jitter is not cosmetic: without it, every client that failed during an outage retries in lockstep the moment the service recovers and knocks it over again. After 20 attempts (~1.5h of retries) the event is marked `dead` and retained 7 days for diagnostics.

**Background sync:** `workmanager` — Android `WorkManager` (periodic, ~15 min minimum) and iOS `BGProcessingTask`.

**iOS background sync is opportunistic and the SDK does not promise it.** iOS schedules background tasks at its own discretion based on usage patterns, battery, and network; an app the user opens rarely may not get a background window for days. The reliable flush points on iOS are foreground entry and background transition. Documenting this honestly matters, because the alternative is customers reporting "missing events" that are actually working as the OS designed.

### 4.4 Reconciliation

**For events: there are no conflicts, and pretending otherwise would be inventing work.**

Events are immutable facts with a client-assigned UUID. Two devices cannot disagree about an event; the server never modifies one. What is often called "conflict resolution" reduces here to three concrete mechanisms:

1. **Dedup** — `event_id` is stable across every retry, so re-delivery is idempotent. Resolved at query time with `uniqExact(event_id)`.
2. **Ordering** — `(device_id, seq)` is monotonic per install. Gaps indicate dropped or lost events and are detectable server-side; they do not need repair, only observability.
3. **Clock reconciliation** — the device clock is untrusted. Server-side skew correction, described in §3.3, is the only "merge" that happens.

No CRDTs, no vector clocks, no last-write-wins. Adding any of them would be complexity with no conflict to resolve.

**For the query cache: the server always wins.**

The cache is read-through and strictly derived. It holds server-computed aggregates keyed by a hash of the query spec, with an etag and a TTL. On request: return if fresh, otherwise revalidate with `If-None-Match` (a `304` refreshes `fetched_at` with no body transfer). The client never computes an authoritative aggregate locally.

**Locally-captured-but-unsent events are deliberately not merged into cached aggregates.** If the client adds 12 pending events to a cached count of 5,000, the display shows 5,012 — and then drops back to 5,000 when the server's next authoritative answer arrives, because the aggregation window had already closed or the rollup had not caught up. Numbers moving backwards destroys trust in an analytics product faster than numbers being slightly stale.

An opt-in `optimistic: true` mode is available, which surfaces the pending count as a **separately labeled** value ("5,000 · 12 syncing") rather than folding it into the total. Same information, no lying number.

---

## 5. Sync Protocol Summary

| Property | Mechanism |
|---|---|
| Delivery guarantee | At-least-once. Client retries until `2xx` or `dead`. |
| Idempotency | Client-generated UUID v7 `event_id`, stable across retries. |
| Dedup point | Query time, `uniqExact(event_id)`. Storage reclaimed by `ReplacingMergeTree` merges. |
| Ordering | `(device_id, seq)` monotonic per install; for observability, not for correctness. |
| Time authority | Server. `ts = ts_client + (server_received − client_sent_at)`, clamped to ±24h/−30d. |
| Partial failure | Per-event accept/reject in a `200`. No poison pills. |
| Backpressure | `429` + `Retry-After`; client backoff with jitter. |
| Overload shedding | Per-tenant Redis quota → `429`. Client holds the data. |
| Crash safety | `inflight` persisted; reset to `pending` on init. |
| Bounded loss | Outbox capped at 10k; oldest dropped, counted, and reported. |
| Read consistency | Server-authoritative, read-through cache with etag + TTL. |
| Contract safety | Protobuf source of truth, `buf breaking` in CI, golden fixtures shared by Go and Dart tests. |

---

## 6. Testing

- **Go services** — table-driven unit tests, plus `testcontainers` running a real ClickHouse. Schema and SQL-generation bugs are invisible against a mocked driver; a container is the only way ORDER BY behavior, `async_insert` settings, and materialized-view semantics get exercised.
- **`pkg/querydsl`** — golden tests mapping DSL input to expected SQL, including adversarial inputs. Every test asserts that `tenant_id` is present in the emitted `WHERE` clause; a missing tenant predicate is a cross-tenant data leak and must be impossible to merge.
- **Flutter SDK** — drift in-memory database, injectable HTTP client. Required scenarios: partial accept, `429` with `Retry-After`, `5xx`, timeout mid-request, process kill while `inflight`, outbox overflow, clock jumping backwards, and offline→online transition.
- **Contract** — `buf breaking` against the last release tag. Golden JSON fixtures in `testdata/golden/` are parsed by both the Go and the Dart test suites, so an encoding divergence fails CI on both sides.

---

## 7. Open Questions and Tradeoffs

### Open questions

1. **Retention tiering** — 13 months of raw events at 1k eps is roughly 2.6B rows/month. Is 13 months of *raw* retention actually required, or would raw-90-days plus rollups-13-months be acceptable? This materially changes storage cost and the TTL design.
2. **GDPR/CCPA deletion** — per-user deletion requires an `ALTER TABLE ... DELETE` mutation, which rewrites parts and is slow at scale. Options: accept mutation cost, batch deletions into a weekly window, or add a `deleted_users` join-filter table that the query layer applies. Needs a decision before the first deletion request, not after.
3. **Schema registry enforcement** — should unknown event names or unexpected property types be rejected at ingest, or accepted and flagged? Rejection keeps the data clean; acceptance keeps a customer's release from silently losing telemetry. A per-tenant strict/lenient mode is probably the answer but adds configuration surface.
4. **SDK key rotation** — write keys are embedded in shipped mobile binaries and are therefore public. Extraction and abuse are a matter of when. Mitigations to evaluate: per-tenant rate limits keyed on IP+device, origin/bundle-ID attestation, short-lived tokens exchanged at app start. Each trades operational complexity for abuse resistance.
5. **`props JSON` cardinality** — the >50-properties-per-event rejection is a guess. Needs a real limit derived from measured tenant behavior, plus monitoring on subcolumn count per tenant.
6. **Query cache invalidation** — TTL-based caching means a customer's dashboard can be up to `ttl_ms` stale with no way to force freshness across all clients. Is a server-pushed invalidation signal needed, or is "pull to refresh" sufficient?

### Tradeoffs by layer

**Contracts** — proto-as-source-of-truth with a JSON wire format buys codegen and breaking-change detection at the cost of committed generated code and proto-JSON mapping quirks. Rejected: hand-written types on both sides (drift is inevitable), and protobuf binary on the wire (opaque to proxies, gRPC stack on mobile for no benefit at this scale).

**Ingestion** — `wait_for_async_insert=1` trades up to ~1s of p99 latency for the ability to honestly claim durability on a `200`. Partial-success responses add response-shape complexity but eliminate the poison-pill outage class.

**No Kafka** — saves an entire cluster and its operational surface, and works because the client outbox already provides durability. Costs: no stream replay after a bad deploy, no second consumer, and ClickHouse downtime surfaces to clients as `503` (absorbed by the outbox, but visible in error metrics). Named triggers for adding it are in §3.8.

**ClickHouse ordering key** — `(tenant_id, name, ts, event_id)` optimizes the dominant dashboard query and makes tenant isolation a key-prefix property. It penalizes per-user queries that do not pin an event name. The `by_user` projection fixes that at roughly 2× storage and is deliberately deferred.

**Monthly partitions** — 13 partitions total, minimal merge overhead. Costs instant `DROP PARTITION` for surgical deletes; per-day or per-tenant removal becomes a mutation.

**Dedup on read** — `uniqExact(event_id)` is exact and costs memory proportional to distinct events in range. Rollups bypass dedup entirely and are therefore approximate — labeled as such in responses rather than quietly conflated with raw results.

**Query DSL, not SQL** — bounded resource usage, no injection surface, per-tenant enforcement that cannot be bypassed. Costs expressiveness: every new analytical question requires backend work, and sophisticated customers will ask for raw SQL. Rejected raw SQL passthrough because a read-only user with row policies still leaves runaway queries, resource exhaustion, and schema coupling that makes every future migration a breaking change.

**drift over Isar** — migration safety and transactional semantics over raw write throughput, for a dependency that ships inside third parties' apps and cannot be hot-fixed. Costs `build_runner` in the SDK build.

**No client-side event buffer** — every event is durable the moment it is captured, including the ones immediately preceding a crash. Costs one SQLite write per event instead of one per batch; WAL mode makes this cheap enough not to matter.

**Bounded outbox (10k)** — protects the host app's storage, at the cost of losing the oldest events for very long offline periods. Made visible via a dropped-event counter rather than silently discarded.

**Server-authoritative query cache** — numbers never move backwards, at the cost of not reflecting local unsent events in aggregates. `optimistic: true` surfaces pending counts as a separate labeled value rather than folding them into the total.

**iOS background sync** — treated as opportunistic. Costs freshness for rarely-opened apps; buys accurate expectations instead of support tickets about "missing" events that the OS simply never scheduled.

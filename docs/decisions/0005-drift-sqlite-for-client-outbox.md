# ADR-0005: drift (SQLite) for the Flutter client outbox

## Status
Accepted

## Date
2026-08-02

## Context

The Flutter SDK is local-first: every captured event is persisted on device before any network attempt, and survives app restarts, crashes, and long offline periods. It ships inside third-party customers' apps, so it cannot be hot-fixed and must not be able to crash or bloat its host.

The storage layer must provide:

- Transactional claim-and-mark (`SELECT pending → UPDATE inflight`) in one atomic step. Without atomicity, a crash between select and update either loses events or resends them unbounded.
- Indexed range scans on `(state, next_attempt_at, seq)` to find due work cheaply.
- Real, testable schema migrations — an SDK upgrade must not discard a user's unsent events.
- Small binary footprint and broad platform support.

Write throughput is modest: a busy app emits tens of events per second, not thousands.

## Decision

Use **drift** over SQLite, with `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL`.

Two tables plus a counters table: `outbox`, `query_cache`, `counters`.

The outbox is **bounded at 10,000 events**. On overflow the oldest `pending` rows are dropped and a `dropped_events` counter is incremented, shipped with the next batch as an `sdk_dropped_events` event.

Events are inserted **immediately** on capture — no in-memory accumulator.

## Alternatives Considered

### Isar
- Pros: Faster raw object writes; good Dart ergonomics.
- Cons: The outbox is not write-throughput-bound, so the main advantage does not apply. The migration story is weaker than SQLite's, and project maintenance has been unstable — a poor property for a dependency shipping inside third parties' apps.
- Rejected: Optimizes a dimension that does not bind; weaker on the one that does.

### ObjectBox
- Pros: Very fast, mature.
- Cons: A native binary in every consuming app (size and platform-support surface) plus licensing friction for a redistributable SDK.
- Rejected: Redistribution cost.

### Hive
- Pros: Pure Dart, trivial to set up.
- Cons: No transactions, no queries. A claim-and-mark without atomicity loses or duplicates events on crash — the exact failure the outbox exists to prevent.
- Rejected: Wrong tool for a queue.

### Plain `sqflite`
- Pros: One fewer build-time dependency; no code generation.
- Cons: Untyped queries and hand-rolled migrations in a package that cannot be hot-fixed.
- Rejected: drift's typed queries and managed migrations are worth the `build_runner` cost here.

### In-memory buffer flushed to SQLite every ~200ms
- Pros: Fewer fsyncs.
- Cons: Creates a window in which a crash loses precisely the events most worth capturing — the ones immediately preceding the crash.
- Rejected: WAL mode with `synchronous=NORMAL` makes immediate writes sub-millisecond. The buffer earns nothing and costs the crash window.

### Unbounded outbox
- Pros: Never loses data.
- Cons: A long-offline device fills the host app's storage. The app gets uninstalled and all the data is lost anyway, along with the customer.
- Rejected: Bounded loss that is *counted and reported* beats unbounded growth.

## Consequences

- `build_runner` and generated files enter the SDK build. Accepted for migration safety.
- `inflight` is a persisted state, not an in-memory flag. On SDK init, rows stuck in `inflight` for more than 5 minutes reset to `pending`. This is safe because of the stable `event_id` from ADR-0004.
- Very long offline periods lose the oldest events. The loss is visible via `sdk_dropped_events`, never silent.
- `Tracker.track()` never throws into host app code and never blocks meaningfully. A telemetry SDK that can crash the app it measures is a liability.

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

The outbox is **bounded at 10,000 total rows, across every state**. On overflow the oldest **`pending`** rows are dropped first, incrementing `dropped_events`. If the table is at 10,000 and every row is `inflight` (no `pending` row exists to evict — meaning a flush is in progress or stuck), the new capture is dropped instead of exceeding the cap, and that drop also increments `dropped_events`. The cap is a hard ceiling on the table, not a ceiling on `pending` alone; there is no state that can grow it past 10,000.

Events are inserted **immediately** on capture. `Tracker.track()` awaits the SQLite insert — this is the commit point, and it is what "immediately" means: the call returns only once the row exists on disk (WAL, `synchronous=NORMAL` — sub-millisecond, per the "no in-process buffer" rejection below). There is no in-memory accumulator and no fire-and-forget path, because either would reopen the crash window this decision exists to close. A failed insert (disk full, an in-progress migration) is caught inside `track()`, incremented into a `write_failures` counter, and swallowed — consistent with never throwing into host app code (§Consequences). This is a real, accepted gap: on disk-full, the event is lost rather than durable, because there is no durable place left to put it. It is not silent — `write_failures` ships with the next successful batch — but it is a genuine exception to "every captured event is persisted," and is called out as such rather than glossed over.

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
- `inflight` is a persisted state, not an in-memory flag. Rows stuck in `inflight` for more than 5 minutes reset to `pending` — checked both on SDK init and at the start of every flush cycle, not init alone. Init-only recovery would leave a row stuck until the next process restart if a request times out or the flush worker dies mid-request while the app keeps running for hours; checking every flush cycle bounds the stuck window to one cycle instead. This is safe because of the stable `event_id` from ADR-0004: re-claiming and resending a falsely-`inflight` row is an ordinary retry, not a special case.
- Very long offline periods, or the outbox filling with `inflight` rows (see the overflow policy above), lose the oldest events. The loss is visible via `sdk_dropped_events`, never silent.
- `Tracker.track()` never throws into host app code and never blocks meaningfully. A telemetry SDK that can crash the app it measures is a liability.

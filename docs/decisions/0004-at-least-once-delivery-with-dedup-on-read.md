# ADR-0004: At-least-once delivery with deduplication on read

## Status

Accepted

## Date

2026-08-02

## Context

A local-first client retries events until acknowledged. Network timeouts are ambiguous — the client cannot distinguish "the server never received this" from "the server processed it and the response was lost." Any retry policy that is safe against loss will therefore produce duplicates.

The platform must decide where duplicates are resolved: at write time (server-side deduplication store), at merge time, at read time, or not at all.

## Decision

**At-least-once delivery with deduplication on read, bounded by retry exhaustion and outbox capacity.**

"At-least-once" describes the retry behavior while the client is still trying: every send attempt is retried until acknowledged. It is not an unconditional never-lose-data guarantee — a client that exhausts its retry budget (`dead` after 20 attempts) or its bounded outbox (10k rows, oldest `pending` dropped on overflow, see ADR-0005) has made a deliberate, counted decision to stop trying. Delivery is at-least-once up to that boundary, then it is bounded loss, visible via `sdk_dropped_events`.

- The client assigns a UUID v7 `event_id` at capture time. It is stable across every retry of that event.
- The client retries freely on `429`, `5xx`, and timeouts.
- Queries count distinct events with `uniqExact(event_id)`, never `count(*)`.
- **Row-returning and property-aggregating queries need the same discipline.** `uniqExact(event_id)` solves distinct-event *counts*, but the DSL also serves property breakdowns and per-user session inspection, which return or aggregate one row per event. Those paths would double-count an unmerged duplicate just as `count(*)` would. Since `FINAL` is banned, every such query must deduplicate explicitly — group by `event_id` and take `argMax(col, ts_received)` per event before aggregating or returning. A DSL path that cannot be expressed that way must be restricted to distinct-event metrics rather than shipped with a known double-count. This is a compiler-level obligation in `pkg/querydsl` (ADR-0006), enforced by golden tests, not a convention for query authors to remember.
- The table uses `ReplacingMergeTree(ts_received)`, which collapses duplicates during background merges so storage does not grow unboundedly.
- `FINAL` is never used in queries.

An `approximate: true` flag in the query DSL switches to `uniq()` (HyperLogLog, ~0.5% error) for very large ranges. It is explicit and opt-in — never a silent substitution.

## Alternatives Considered

### Duplicates tolerated, no deduplication at all

- Pros: Cheapest possible read path.
- Cons: Counts are wrong by an unbounded, traffic-dependent margin. Fine for trend shapes, unacceptable for anything a customer reports as a number.
- Rejected: This is a customer-facing analytics product.

### Effectively-exactly-once via a server-side dedup window

- Pros: Clean data at rest; `count(*)` is correct.
- Cons: Requires a dedup store (Redis or a ClickHouse lookup) on the hot ingest path, adds latency and memory proportional to the window, and is still only correct *within* the window. Retries after a long offline period fall outside it.
- Rejected: Real complexity and latency for a guarantee that remains approximate at the boundary.

### `SELECT ... FINAL`

- Pros: Reads the deduplicated view directly.
- Cons: Forces a merge at query time. The most common way to make a ClickHouse dashboard slow.
- Rejected: `uniqExact(event_id)` achieves the same result without the merge.

### Server-generated event IDs

- Pros: Server controls uniqueness.
- Cons: A retried event gets a new ID and becomes a genuine duplicate. Destroys idempotency entirely.
- Rejected: The client must own the ID for retries to be safe.

## Consequences

- **Crash recovery becomes trivial.** The client persists an `inflight` state and resets stale rows to `pending` on startup. Resending is safe because the ID is stable. Under exactly-once semantics this would be a distributed-systems problem instead of a two-line fix.
- `uniqExact` costs memory proportional to distinct events in range. The `approximate` flag is the documented escape hatch.
- **Rollups are approximate.** A materialized view fires on the inserted block and never sees merge-time deduplication, so duplicates inflate rollup counts. The raw `events` table is the only source of truth; rollup-sourced responses are labeled `"source": "rollup"`.
- UUID v7 rather than v4: time-ordered IDs cluster in the sort key tail, compress better than random ones, and give a usable secondary sort within a millisecond.

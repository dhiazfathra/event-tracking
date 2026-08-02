# ADR-0002: No message queue — the client outbox is the durable buffer

## Status

Accepted

## Date

2026-08-02

## Context

The reference architecture for event ingestion is `clients → API → Kafka → consumer → warehouse`. Kafka absorbs write spikes, decouples ingestion from storage, and allows replay.

This platform targets ~1k events/sec (~86M/day). Every client is a Flutter SDK with a local-first architecture: it already persists every captured event to SQLite and retries with backoff until acknowledged.

The question is whether a server-side durable buffer earns its operational cost when every producer already has one.

## Decision

Do not deploy Kafka or an equivalent. The ingest service writes directly to ClickHouse using `async_insert=1, wait_for_async_insert=1`. When ClickHouse is unavailable, ingest returns `503` and clients retain the events in their outboxes.

Add a queue when one of these three triggers fires, not before:

1. Stream replay is needed to recover from a bad schema deploy.
2. A second consumer of the raw event stream exists (real-time alerting, webhooks).
3. Accepting writes during ClickHouse downtime becomes a hard requirement — for example, if a server-to-server ingestion path without a durable client is added.

## Alternatives Considered

### Kafka / Redpanda between ingest and ClickHouse

- Pros: Standard, well understood, enables replay and fan-out, absorbs spikes.
- Cons: A second stateful cluster; a consumer service to write, deploy, and monitor; partition, offset, lag, and rebalance operations; a whole new class of failure modes.
- Rejected for now: **within the scope the outbox already covers**, it duplicates existing durability. That scope is narrower than Kafka's and the difference is real: the outbox is bounded, per-device retry storage, so it does not survive an uninstall, an outbox overflow, an outage longer than the retry budget, or a device that never comes back. Kafka would provide central retention and replay across all of those. The judgment is that those cases are acceptable losses at this scale (they are documented in Consequences and in ADR-0004's bounded-loss contract), not that Kafka would add nothing. Listed as a triggered addition above.

### Ingest-side write-ahead log to disk or S3, replayed on recovery

- Pros: Cheaper than Kafka, survives ClickHouse downtime.
- Cons: Custom durable-storage code — the most dangerous kind to hand-roll — plus a replay path that only executes during incidents and is therefore never exercised.
- Rejected: for the failure modes the client outbox already covers, this adds custom durable-storage code without adding reach — the clients hold the data anyway. It would help in the cases the outbox misses (uninstall, overflow, long outages), but a WAL replayed only during incidents is the weakest possible answer to those; if they start mattering, trigger 1 or 3 fires and the answer is a real queue, not a hand-rolled log.

### In-process buffering in the ingest service with fast acknowledgement

- Pros: Very low ingest latency.
- Cons: Acknowledges events that are not yet durable. A pod restart loses events the client has already deleted, breaking the at-least-once guarantee.
- Rejected: Incompatible with the delivery contract.

## Consequences

- ClickHouse downtime is visible to clients as `503` and in error metrics. The client holds the data and keeps retrying — it is not lost *because of the downtime itself*. Data can still be lost if the outage outlasts the client's retry budget (20 attempts, ~1.5h) or its 10k-row outbox cap, which is bounded, counted loss rather than an unconditional guarantee (see ADR-0005). The outage is visible either way, never silent.
- No stream replay exists. A schema bug that corrupts data on write cannot be reprocessed from a log — it must be fixed forward in ClickHouse.
- Any future server-to-server ingestion path (no durable client) immediately trips trigger 3 and requires this decision to be revisited.
- The ingest service stays stateless and scales horizontally for free.

# ADR-0003: ClickHouse ordering key and partitioning for the events table

## Status

Accepted

## Date

2026-08-02

## Context

The `events` table is the source of truth for a multi-tenant analytics platform at ~1k events/sec (~2.6B rows/month, 13-month retention). In ClickHouse the `ORDER BY` clause is the primary key and determines which queries are fast; it is expensive to change once the table holds data.

Two query shapes compete:

- **Dashboard** (dominant): one tenant, one event name, over a time range, optionally broken down by a property.
- **Per-user** (debugging, session inspection): one tenant, one user, over a time range, all event names.

Multi-tenancy adds a hard constraint: tenant isolation must be structural, not a filter applied late.

## Decision

```sql
ENGINE = ReplacingMergeTree(ts_received)
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, name, ts, event_id)
TTL event_date + INTERVAL 13 MONTH DELETE
```

One table for all tenants, with `tenant_id` as the leading sort key. Bloom-filter skip indexes on `user_id` and `session_id`.

The `by_user` projection — `ORDER BY (tenant_id, user_id, ts)` — is specified but **not** created initially. Add it when a measured query pattern demands it.

## Rationale

- **`tenant_id` first** — every query is tenant-scoped. This is a pruning optimization, not the isolation mechanism: as a key prefix it lets ClickHouse skip granules outside one tenant's range instead of scanning the table, making a correctly-scoped query cheap. The isolation guarantee itself comes from the query compiler always injecting the `tenant_id` predicate server-side (ADR-0006) — the ordering key does nothing to stop a query that omits that predicate. It also sets up future sharding on `cityHash64(tenant_id)`, where a tenant's data living on one shard lets shard pruning skip the others, conditional on `optimize_skip_unused_shards` being enabled and the query actually filtering on the sharding key.
- **`name` second** — dashboards pin one event name. Low cardinality keeps the prefix cheap.
- **`ts` third** — the range-scan dimension.
- **`event_id` last** — makes the sort key unique, which `ReplacingMergeTree` requires to collapse actual duplicates rather than distinct rows sharing a key.
- **Monthly partitions** — 13-month TTL yields 13 partitions. Daily would yield ~400 with more merge pressure and no query benefit the `ts` sort key does not already provide.

## Alternatives Considered

### `ORDER BY (tenant_id, ts, name, event_id)`

- Pros: Uniformly good for any time-range query regardless of event name.
- Cons: Loses the ability to scan a single event name's rows contiguously — the dominant dashboard query.
- Rejected: Optimizes the less common shape.

### `ORDER BY (tenant_id, user_id, ts, event_id)`

- Pros: Excellent for per-user debugging.
- Cons: High-cardinality `user_id` early in the key fragments compression; makes name-filtered dashboard queries scan broadly.
- Rejected: Wrong dominant workload. Available as a projection instead.

### Table-per-tenant

- Pros: Perfect isolation, trivial per-tenant deletion via `DROP TABLE`.
- Cons: Thousands of tables is a known ClickHouse anti-pattern — metadata pressure, unusable cross-tenant operations, painful schema migrations.
- Rejected: Does not survive tenant growth.

### Daily partitions (`toYYYYMMDD`)

- Pros: `DROP PARTITION` for surgical removal of a bad day.
- Cons: ~400 partitions under a 13-month TTL, more parts and merge overhead, no query benefit.
- Rejected: Cost without matching benefit at this retention.

### `MergeTree` instead of `ReplacingMergeTree`

- Pros: Simpler, no version column, no merge-time collapsing.
- Cons: Duplicates from at-least-once delivery accumulate in storage forever.
- Rejected: `ReplacingMergeTree` reclaims that space in the background at negligible cost. See ADR-0004.

## Consequences

- Queries filtering by user without pinning an event name degrade to a partition scan. The bloom filter helps; it is not a substitute. The escape hatch is the `by_user` projection, at roughly 2× table storage.
- Per-user and per-day deletion require `ALTER TABLE ... DELETE` mutations (slow, rewrites parts) rather than instant `DROP PARTITION`. GDPR deletion is per-user and would need a mutation under any partitioning scheme, so this is not a new cost.
- `tenant_id` never comes from the client. It is resolved server-side from the write key; a client-supplied tenant field would be a cross-tenant write primitive.
- Changing the ordering key later means rewriting the table. Revisit only with measured evidence.

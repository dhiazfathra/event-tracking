# ADR-0006: Constrained query DSL instead of SQL passthrough

## Status
Accepted

## Date
2026-08-02

## Context

Third-party customers query their own event data through a public API. ClickHouse will happily execute a query that reads billions of rows and exhausts a node's memory. In a multi-tenant single-table design, a query missing its `tenant_id` predicate reads other customers' data.

The exposure model must bound resource usage, make cross-tenant reads structurally impossible, and avoid coupling the public API to the physical schema.

## Decision

Expose a **constrained JSON query DSL**, compiled server-side by `pkg/querydsl` into parameterized SQL. Never accept SQL from clients.

```json
{
  "metric": "count_distinct_users",
  "event": "checkout_completed",
  "filters": [{ "property": "plan", "op": "eq", "value": "pro" }],
  "breakdown": ["country"],
  "granularity": "day",
  "date_range": { "from": "2026-07-01", "to": "2026-07-31" },
  "limit": 500
}
```

Endpoints: `POST /v1/query/{timeseries,funnel,retention,breakdown}`.

- `tenant_id` is injected by the compiler from the authenticated read key. It is never accepted from the request body.
- Every query executes under a per-tenant-tier ClickHouse settings profile: `max_execution_time`, `max_rows_to_read`, `max_memory_usage`, `max_result_rows`.
- Responses carry `etag`, `computed_at`, and `source` (`"raw"` or `"rollup"`), feeding the client-side query cache.
- Every `pkg/querydsl` golden test asserts that the emitted SQL contains a `tenant_id` predicate. A missing tenant predicate is a cross-tenant data leak and must be impossible to merge.

## Alternatives Considered

### Raw SQL with a read-only ClickHouse user and row policies
- Pros: Maximum expressiveness; no backend work for new questions; ClickHouse row policies can enforce tenant isolation at the database layer.
- Cons: Runaway queries and resource exhaustion remain (settings profiles help, but a customer can still write an accidental cross join); the physical schema becomes public API, so every future migration — including the deferred `by_user` projection or a partitioning change — is a breaking change; the abuse surface is unbounded.
- Rejected: Schema coupling alone is disqualifying for a platform expected to evolve its storage layout.

### GraphQL
- Pros: Familiar, good tooling, client-specified field selection.
- Cons: Analytics queries are aggregations over time ranges, not object graphs. GraphQL's shape does not match, and query-cost bounding must be bolted on anyway.
- Rejected: Wrong model for the domain.

### Fixed endpoints with no filtering or breakdown
- Pros: Trivially safe and fast.
- Cons: Too rigid to be a useful product. Every customer question becomes a feature request.
- Rejected: Under-serves the actual use case.

## Consequences

- **Expressiveness is bounded.** Every new analytical question requires backend work, and sophisticated customers will ask for raw SQL. This is the accepted cost.
- The physical ClickHouse schema stays private. The `by_user` projection, sharding, and partitioning changes can all land without a public API change.
- Query cost is enforced by ClickHouse settings profiles rather than by trusting the DSL compiler — defense in depth, since a compiler bug should not become an outage.
- The DSL is versioned alongside the event envelope under `proto/tracking/v1/query.proto` and is covered by `buf breaking` (see ADR-0001).
- Rollup-sourced results are approximate (see ADR-0004) and are labeled in the response rather than silently conflated with raw results.

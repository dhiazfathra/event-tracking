# event-tracking

Multi-tenant event tracking platform: Go services + ClickHouse for ingestion, storage, and querying, with a local-first Flutter SDK for offline event capture and background sync.

**Status: design only.** No implementation yet. The design is approved and documented; the next step is an implementation plan.

## Design

Start here: [Event Tracking Platform — Design](docs/superpowers/specs/2026-08-02-event-tracking-platform-design.md).

It covers the high-level architecture, monorepo layout and module boundaries, the ingestion API and batching strategy, the ClickHouse schema, the Flutter client's local storage and sync engine, the sync protocol, and the tradeoffs and open questions at each layer.

## Architecture Decisions

| ADR | Decision |
|---|---|
| [0001](docs/decisions/0001-protobuf-contracts-json-wire-format.md) | Protobuf as contract source of truth, JSON on the wire |
| [0002](docs/decisions/0002-no-message-queue-client-outbox-is-the-buffer.md) | No message queue — the client outbox is the durable buffer |
| [0003](docs/decisions/0003-clickhouse-ordering-and-partitioning.md) | ClickHouse ordering key and partitioning for the events table |
| [0004](docs/decisions/0004-at-least-once-delivery-with-dedup-on-read.md) | At-least-once delivery with deduplication on read |
| [0005](docs/decisions/0005-drift-sqlite-for-client-outbox.md) | drift (SQLite) for the Flutter client outbox |
| [0006](docs/decisions/0006-constrained-query-dsl-not-sql-passthrough.md) | Constrained query DSL instead of SQL passthrough |

Decisions are not deleted when they change. Supersede them with a new ADR that references the old one.

## Planned Layout

```text
proto/tracking/v1/      contract source of truth (buf)
gen/{go,dart}/          generated types, committed
services/{ingest,query}/
pkg/{clickhouse,tenant,querydsl,testsupport}/
clients/flutter_sdk/    pub package
clients/example_app/
migrations/{clickhouse,postgres}/
testdata/golden/        fixtures read by both Go and Dart tests
```

Boundary rules, enforced in CI: `services/*` never import each other; the Flutter SDK and the services share only `gen/`; `pkg/*` never imports `services/*`.

## Note on `.gitignore`

The current `.gitignore` ignores `go.work`. That is the right default for a single-module repository and the wrong one for the planned monorepo — once the Go workspace lands, `go.work` will wire `services/*`, `pkg/*`, and `gen/go` together and must be committed. Remove the ignore entry in the same change that adds the workspace.

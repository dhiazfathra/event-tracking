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
| [0007](docs/decisions/0007-public-client-id-and-short-lived-ingest-tokens.md) | Public client ID and short-lived ingest tokens |

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

## Development

### Prerequisites

- Go 1.23+
- [`buf`](https://buf.build/docs/installation) 1.47+
- Dart 3.5+ / Flutter 3.24+ (only for `clients/flutter_sdk`)

### Common commands

```bash
make gen               # regenerate gen/ from proto/ — commit the result
make gen-check         # CI gate: fails if gen/ is stale
make lint              # buf lint + golangci-lint
make check-boundaries  # enforce the three module rules
make test              # test each Go workspace module
```

### Module rules

1. `services/*` may not import each other — shared logic moves to `pkg/`.
2. `clients/flutter_sdk` and `services/*` share exactly one *dependency*: `gen/`. (`testdata/golden/` is a shared test fixture path, not a dependency, and is fine on both sides.)
3. `pkg/*` may not import `services/*`.

Rules 1 and 3 are enforced by `tools/checkboundaries` in CI. **Rule 2 is not** —
it spans the Go/Dart line, which the Go import graph cannot see. It holds
structurally instead: the SDK is a separate pub package with no path dependency
on any Go module, so `flutter pub get` has nothing to resolve against
`services/*`. The `flutter-sdk` CI job building the package in isolation is what
would fail if that ever changed.

### Why `gen/` is committed

So neither a Go build nor `flutter pub get` requires the proto toolchain. The
cost is merge noise, paid for by the `make gen-check` CI gate.

### Wire format

JSON, not protobuf binary. Proto defines the schema and drives codegen only.
Note that proto-JSON encodes 64-bit integers (`ts_client`, `seq`, `received_at`)
as **strings** — `testdata/golden/` pins this, and both the Go and Dart test
suites read those same files.

# ADR-0001: Protobuf as contract source of truth, JSON on the wire

## Status

Accepted

## Date

2026-08-02

## Context

A Go backend and a Flutter SDK must agree on an event envelope and a query DSL. The SDK ships inside third-party customers' apps, so it cannot be force-upgraded — a breaking envelope change is unfixable once released into the wild. Any drift between the server's idea of the schema and the SDK's is a silent data-loss bug that surfaces weeks later.

The monorepo therefore needs a mechanism that (a) generates types for both languages from one definition and (b) detects breaking changes before they ship.

## Decision

Define contracts as Protobuf in `proto/tracking/v1/`. Use `buf` to generate Go and Dart types into `gen/`, committed to the repository. Run `buf breaking` in CI against the previous release tag.

Use **JSON** as the wire format. Protobuf is used for schema definition and code generation only, not for binary encoding.

Pin the proto↔JSON encoding with golden fixtures in `testdata/golden/`, read by both the Go and the Dart test suites.

**One canonical ProtoJSON configuration, shared by both languages:**

- Field names: `camelCase` (ProtoJSON default), not `preserveProtoFieldNames`.
- Enums: string names (`"EVENT_TYPE_CLICK"`), never numeric — numeric enum values are a silent breaking change waiting to happen if a value is ever renumbered.
- Default-field emission: `emitUnpopulated=false`. Absent fields are absent in the JSON, not emitted as zero values — this matters for partial-update and optional-field semantics.
- Unknown fields: ignored on decode, not rejected — this is what lets an older SDK receive a payload with a field it doesn't know about without hard-failing.
- Null handling: `null` is accepted as equivalent to "field absent" on decode; the encoder never emits `null` for an unpopulated field (see `emitUnpopulated` above).
- MIME type: `application/json` on the wire (not `application/protobuf+json`) — this is the format proxies and tooling already understand, consistent with the JSON-not-binary decision above.

The golden fixtures in `testdata/golden/` encode exactly this configuration; a change to any of these settings is a breaking change to the fixtures and must go through `buf breaking` review.

## Alternatives Considered

### Hand-written types on both sides

- Pros: No toolchain, no generated code in git.
- Cons: Guaranteed to drift. Nothing mechanically detects a field added to Go and forgotten in Dart.
- Rejected: The failure mode is silent and expensive.

### Protobuf binary on the wire

- Pros: Smaller payloads, faster parse.
- Cons: Opaque to every proxy, load balancer, and debugging tool; requires a gRPC or protobuf runtime on mobile; at ~1k events/sec the byte savings are irrelevant.
- Rejected: Cost is debuggability, benefit is a rounding error at this scale.

### JSON Schema as source of truth

- Pros: Native to the wire format, no mapping layer.
- Cons: Dart codegen tooling is weaker, and there is no equivalent of `buf breaking` for automated backward-compatibility checking.
- Rejected: Breaking-change detection is the highest-value property here.

### Generate at build time instead of committing `gen/`

- Pros: No merge noise from generated files.
- Cons: Every contributor and the Flutter build need `buf` installed; consumers of the pub package would need it too.
- Rejected: Committed generated code plus a `make gen && git diff --exit-code` CI check is the smaller cost.

## Consequences

- Generated code is committed; a CI check enforces that it matches the protos.
- Proto→JSON mapping quirks (64-bit integers as strings, well-known type encodings) are real and are pinned by shared golden fixtures rather than by convention.
- The SDK and the services share exactly one thing: `gen/`. Neither may hand-write the envelope.
- `buf breaking` failing is a hard stop, not a warning. An unfixable breaking change in a shipped SDK is worse than a blocked release.

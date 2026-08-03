# Ingest Service

`POST /v1/auth/challenge`, `POST /v1/auth/token`, `GET /.well-known/jwks.json`,
and `POST /v1/batch`.

## Status code contract

The SDK's entire retry logic keys off these, and the SDK cannot be
force-upgraded. Changing one is a breaking change.

| Status | Meaning | Client action |
|---|---|---|
| `200` | Batch processed (may contain rejects) | Delete accepted, mark rejected `dead` |
| `400` | Batch envelope malformed | Drop batch, do not retry |
| `401` | Token expired or invalid | Re-exchange, retry once; if the exchange fails, stop syncing and surface to the host app |
| `413` | Batch too large | Halve batch size, retry. Single event already? Mark it `dead` |
| `429` | Quota or rate limit | Retry after `Retry-After` |
| `5xx` | Server or ClickHouse fault | Retry with backoff |

## What a 200 guarantees

`wait_for_async_insert=1` means the data was flushed to the ClickHouse node
that received the insert. It does **not** wait for replication or a backup.

At single-node scale, "flushed" and "durable" are the same thing — but after
the `200` the client deletes its copy, so if that node loses flushed data
before replication exists, the event is gone with no retry path and no
counter. This is **accepted risk at single-node scale**, not something the
outbox rescues. Adding a replica with `insert_quorum`, or node-level backups,
is the fix.

## Trust tiers

| Tier | Meaning | Treatment |
|---|---|---|
| 0 | Attested (App Attest / Play Integrity) | Normal rate limits |
| 1 | Attestation unavailable | Tighter rate limits. **Never sampled.** |

Sampling is forbidden here. The client deletes the outbox row on a `200`, so a
silently discarded event has no retry and no counter. A `429` degrades into
ordinary counted loss instead — same cost control, no hole in the guarantee.

Rooted, custom-ROM, and de-Googled devices fail attestation legitimately;
simulators fail; Play Integrity is quota-limited. Blocking would buy little
and cost real users.

## Rate limit key order

`install_id` first, tenant second, IP last. IP-primary limiting is actively
harmful in this market: Indonesian carriers CGNAT aggressively, so one
Telkomsel egress IP is thousands of real users. IP survives only as a coarse
anomaly signal.

`POST /v1/auth/token` is separately rate-limited per `client_id` (30/minute)
to bound install-row write amplification from `device_hint` churn — see
`tokenRatePerMinute` in `internal/handler/token.go`.

## Local development

```bash
docker compose -f deploy/docker-compose.yml up -d --wait clickhouse postgres redis
CLICKHOUSE_PASSWORD=dev go run ./services/ingest/cmd
```

## Tests

```bash
go test ./services/ingest/...              # unit
go test -tags e2e ./services/ingest/...    # needs Docker
```

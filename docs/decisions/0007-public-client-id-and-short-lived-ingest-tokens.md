# ADR-0007: Public client ID and short-lived ingest tokens

## Status

Accepted

## Date

2026-08-02

## Context

The Flutter SDK ships inside third-party customers' apps. The original design authenticated ingestion with a long-lived bearer write key (`wk_live_...`) embedded in the binary. Anything embedded in a distributed mobile binary is extractable — this is not a risk to mitigate, it is a certainty to design around.

The naive framing ("how do we protect the embedded key?") has no good answer. Obfuscation, HMAC signing with an embedded secret, and certificate pinning all reduce to the same problem: a secret on a device the attacker owns is not a secret.

The reframe that resolves this: **the embedded identifier is not a credential, it is a public client identifier.** It identifies a tenant; it authorizes nothing. Once that is true, its extraction is boring, and the security question moves to what the client can exchange it for.

Rotation deserves special mention because it was the original framing. On mobile, rotating an embedded key requires a release and a forced-upgrade cycle — that is weeks. **Key rotation cannot function as an abuse control.** It is an incident-response lever only. Making the *token* the thing that expires removes rotation from the critical path entirely.

Two constraints shape the specifics:

- **Deployment context is Indonesian mobile.** Carriers (Telkomsel and peers) CGNAT aggressively — a single egress IP represents thousands of distinct real users. Any IP-primary rate limit is either set low enough to throttle half of Jakarta or high enough not to bind.
- **Clinic data sits behind this platform**, bringing PDP Law obligations around *data spesifik* and an ISO 27001 posture that an assessor will read.

## Decision

Three mitigations, not as peers. **The token exchange is the architecture; the other two are the fence around it.**

| Mitigation | Role | Sequencing |
|---|---|---|
| Short-lived exchanged tokens | The actual control | Build first — everything composes onto it |
| Platform attestation | Gate on the exchange endpoint | Build second, with a documented degrade path |
| Rate limits (install-keyed) | Cost cap, not security | Build, re-keyed off IP |

### 1. Ship a client ID, not a secret

The embedded identifier is public, useless on its own, and not rotation-critical. It identifies a tenant and authorizes nothing. This classification is deliberate and recorded (see Consequences).

### 2. Attest at the exchange, not at ingest

App Attest (iOS) and Play Integrity (Android) are the only mitigations here with cryptographic teeth: they prove a genuine unmodified binary on a genuine device. Bundle-ID and origin headers prove nothing — `curl` sets them. They are retained as telemetry, never as a control.

Attestation runs once at the exchange endpoint, not on every ingest request. Ingest stays a hot path that only verifies a JWT.

Planned-for failure modes:

- Play Integrity standard requests are quota-limited.
- App Attest fails on simulators.
- Rooted, custom-ROM, and de-Googled devices fail **legitimately** — these are real users, not attackers.

Therefore attestation failure does not block. It assigns a **trust tier**:

- **Tier 0** — attestation passed. Normal limits.
- **Tier 1** — attestation unavailable or failed. Tight limits, sampled ingestion.

Blocking outright creates support load and data holes in exchange for very little.

### 3. Exchange for a short-lived scoped token

JWT signed with ES256 or EdDSA, so the ingest edge verifies with a public key and holds no shared secret.

Claims: `tenant_id`, `install_id`, `scope=write:events`, `trust_tier`, `exp` 30–60 minutes. Silent refresh. Signing keys rotate via a JWKS endpoint with overlapping validity.

### 4. Rate limits keyed on `install_id` first, tenant second, IP last

IP-primary limiting is actively harmful in this deployment (see CGNAT above). IP is retained only as a coarse anomaly signal.

The per-tenant cap remains, but as **budget protection**: one abused tenant must not consume the whole ingestion spend.

### 5. Write-only means write-only

No reads, no enumeration, no listing, no cross-tenant access. The worst case of a fully abused pipeline is then cost plus data pollution — never exfiltration.

This scoping is also the PDP Law argument: a stolen write token cannot reach *data spesifik*.

### 6. Stamp every row

Server-authoritative timestamp, server-assigned event ID, and persisted `trust_tier`. Cheap, and it is what permits retroactively quarantining a pollution window instead of nuking a table.

### Sequencing

- **Sprint 1** — token exchange (the current write key becomes the bootstrap client ID), `install_id`-keyed limits, write-scope audit. This alone converts a permanent public secret into a 60-minute one.
- **Sprint 2–3** — attestation, trust tiers, degrade policy.
- **Sprint 4** — remote-config rotation of the client ID, JWKS rotation runbook.

## Alternatives Considered

### Obfuscating the key in the binary

- Pros: Feels like defense.
- Cons: Zero security value against anyone willing to run a disassembler; non-zero build complexity.
- Rejected: Cost without benefit.

### HMAC request signing with an embedded secret

- Pros: Requests are unforgeable without the secret.
- Cons: The secret is still in the binary — identical problem, more code, more ways to get the canonicalization wrong.
- Rejected: Restates the problem as a solution.

### Certificate pinning as an abuse control

- Pros: Stops casual MITM.
- Cons: Does not stop a determined extractor, and adds real outage risk when a certificate changes unexpectedly.
- Rejected as an *abuse control*. If shipped for other reasons, it requires backup pins and a kill switch.

### Rotating the embedded key on a schedule

- Pros: Bounds the useful lifetime of a leaked key.
- Cons: Requires a release and forced-upgrade cycle — weeks of latency. Cannot respond to an active incident.
- Rejected as a control; retained as an incident-response lever only.

### Blocking on attestation failure

- Pros: Strongest possible gate.
- Cons: Rooted, custom-ROM, and de-Googled devices fail legitimately; simulators fail; Play Integrity quota limits bite. Produces support load and silent data holes.
- Rejected in favor of trust tiers.

## Consequences

- **The shipped identifier is classified as public, not a secret.** This classification is recorded deliberately, with the compensating controls named above. Without it, an ISO 27001 assessor reading "credential in a distributed binary" flags A.5.17 and A.8.5, and the argument happens in the audit rather than in the document.
- **The rotation SLA and the attestation-failure degrade policy are decisions of record**, not things discovered under incident. An auditor will ask whether they were made deliberately.
- Ingest gains a JWT verification step and loses the write-key lookup on the hot path. The control plane gains an exchange endpoint, a JWKS endpoint, and attestation integrations.
- `trust_tier` becomes a column on `events` (see spec §3.5) and a dimension available for filtering polluted data.
- Tier 1 traffic is sampled, so analytics from attestation-unavailable devices are statistically incomplete by design. This must be surfaced to customers rather than presented as complete.
- The 30–60 minute token lifetime bounds a stolen token's usefulness without requiring a client release. Rotation stops being load-bearing.

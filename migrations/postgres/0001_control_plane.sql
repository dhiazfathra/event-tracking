CREATE TABLE tenants (
    tenant_id    TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Per-tenant flag, not a deploy, so a stranded customer can be rolled back
    -- to dual-accept without a release.
    legacy_key_mode TEXT NOT NULL DEFAULT 'dual_accept'
        CHECK (legacy_key_mode IN ('dual_accept', 'deprecating', 'cutoff'))
);

-- The identifier embedded in the SDK. Public: anything shipped inside a third
-- party's mobile binary is extractable. It identifies a tenant and authorizes
-- nothing.
CREATE TABLE client_ids (
    client_id   TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    revoked_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- SHA-256 of a legacy `wk_live_` bearer key, for dual-accept lookups only.
    -- Never the plaintext key: client_id itself is public (embedded in SDKs),
    -- but a legacy key is a secret, so it is looked up by digest, the same as
    -- read_keys.key_hash.
    legacy_key_hash BYTEA UNIQUE
);
CREATE INDEX client_ids_tenant ON client_ids(tenant_id);

-- Read keys for the query API. Separate credential, separate scope.
CREATE TABLE read_keys (
    key_hash    BYTEA PRIMARY KEY,       -- sha256 of the key; never store the key
    tenant_id   TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    revoked_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ed25519 signing keys. Rotation overlaps old and new so in-flight tokens stay
-- verifiable.
CREATE TABLE signing_keys (
    kid          TEXT PRIMARY KEY,
    public_key   BYTEA NOT NULL,
    private_key  BYTEA NOT NULL,        -- encrypted at rest by the deployment
    active       BOOLEAN NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at   TIMESTAMPTZ
);
-- At most one active, non-retired key at a time. This is what makes the
-- bootstrap insert below conflict-safe instead of racy: two replicas that both
-- try to create the first key hit this constraint, not a 50/50 chance of two
-- active rows.
CREATE UNIQUE INDEX signing_keys_one_active
    ON signing_keys ((true)) WHERE active AND retired_at IS NULL;

CREATE TABLE quotas (
    tenant_id        TEXT PRIMARY KEY REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    daily_events     BIGINT NOT NULL,
    rps_tier0        INTEGER NOT NULL DEFAULT 50,
    rps_tier1        INTEGER NOT NULL DEFAULT 10,   -- tighter, never sampled
    rps_legacy       INTEGER NOT NULL DEFAULT 5     -- below tier 1, deprecation pressure
);

-- Server-issued. Never client-supplied: a client able to choose or rotate its
-- install_id could reset its own rate limit at will.
CREATE TABLE installs (
    install_id     TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    platform       TEXT NOT NULL,
    trust_tier     SMALLINT NOT NULL,
    attest_subject TEXT,                -- derived from the attestation at tier 0
    device_key     TEXT,                -- tier 1 anchor; see deviceKey() in Task 10
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Two partial indexes, not one UNIQUE over nullable columns. Postgres treats
-- NULLs as distinct, so UNIQUE (tenant_id, attest_subject) would happily admit
-- unlimited unattested rows — letting a Tier 1 client mint a fresh install_id,
-- and therefore a fresh rate-limit bucket, on every exchange.
CREATE UNIQUE INDEX installs_attested
    ON installs (tenant_id, attest_subject) WHERE attest_subject IS NOT NULL;
CREATE UNIQUE INDEX installs_unattested
    ON installs (tenant_id, device_key) WHERE attest_subject IS NULL;

-- Per-session clock offset. Persisted so a retry gets the same ts as the
-- original: recomputing per-request would move the row under the sort key and
-- stop ReplacingMergeTree from ever collapsing the duplicate.
CREATE TABLE session_offsets (
    tenant_id   TEXT NOT NULL,
    device_id   TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    offset_ms   BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, device_id, session_id)
);

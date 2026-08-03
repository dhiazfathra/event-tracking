// Package limits holds the numeric constants that appear in the public
// contract. They are shared so the ingest validator and the SDK cannot drift.
package limits

const (
	// MaxEventsPerBatch caps a single POST /v1/batch. Over this: 413.
	MaxEventsPerBatch = 500

	// MaxBatchBytes caps the decompressed batch body. Over this: 413.
	MaxBatchBytes = 1 << 20 // 1 MiB

	// MaxPropsPerEvent bounds ClickHouse JSON subcolumn explosion.
	// Per-event reject, not a batch failure.
	MaxPropsPerEvent = 50

	// MaxOutboxRows caps the client outbox across every state. Overflow evicts
	// the oldest pending rows and increments dropped_events.
	MaxOutboxRows = 10_000

	// MaxRetryAttempts is when the client gives up and marks an event dead.
	MaxRetryAttempts = 20
)

// Quota is the per-tenant budget shape shared by the control plane (which
// reads it from Postgres) and the ingest quota checker (which enforces it in
// Redis). It lives here, not in services/ingest, because pkg/controlplane may
// not import services/* — this is the one place both sides can depend on.
type Quota struct {
	DailyEvents int64
	RPS         int

	// LegacyRPS applies to pre-token wk_live_ credentials during the cutover.
	// Deliberately below the tier-1 rate: deprecation pressure the SDK already
	// absorbs, because it backs off on 429.
	LegacyRPS int
}

// DefaultQuota is what a tenant gets when no row exists in the `quotas`
// table yet. Provisioning a client_id without a matching quotas row is a
// valid, un-seeded local-dev/staging state — nothing in this codebase inserts
// one automatically — and the rest of the service already treats "missing
// config" this way (see env() in cmd/main.go, or Deps.Now's default) rather
// than erroring. Values mirror the column defaults in
// migrations/postgres/0001_control_plane.sql.
var DefaultQuota = Quota{
	DailyEvents: 1_000_000,
	RPS:         50, // tier 0; LimitsFor applies DefaultRPSTier1 for tier 1
	LegacyRPS:   5,
}

// DefaultRPSTier1 is DefaultQuota's tier-1 rate, kept separate because Quota
// only carries one RPS field (the caller's tier picks which column applies).
const DefaultRPSTier1 = 10

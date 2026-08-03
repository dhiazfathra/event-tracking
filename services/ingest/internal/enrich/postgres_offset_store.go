package enrich

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresOffsetStore persists first-contact offsets so a retry lands on the
// same ts as the original — across restarts and across pods.
type PostgresOffsetStore struct{ pool *pgxpool.Pool }

func NewPostgresOffsetStore(pool *pgxpool.Pool) *PostgresOffsetStore {
	return &PostgresOffsetStore{pool: pool}
}

// GetOrSet is a single atomic upsert-returning: two concurrent first requests
// for one session agree on one value. If they disagreed, the two events would
// land at different ts and neither would deduplicate against its own retry.
func (s *PostgresOffsetStore) GetOrSet(ctx context.Context, k SessionKey, candidate int64) (int64, error) {
	var offset int64
	// last_seen is bumped on every touch, including the no-op offset_ms update
	// on conflict, so a retention sweep (see migrations/postgres/0002) only
	// reclaims sessions that have actually gone quiet, not ones still retrying.
	err := s.pool.QueryRow(ctx, `
		INSERT INTO session_offsets (tenant_id, device_id, session_id, offset_ms)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (tenant_id, device_id, session_id)
		DO UPDATE SET offset_ms = session_offsets.offset_ms, last_seen = now()
		RETURNING offset_ms`,
		k.TenantID, k.DeviceID, k.SessionID, candidate).Scan(&offset)
	if err != nil {
		return 0, fmt.Errorf("session offset: %w", err)
	}
	return offset, nil
}

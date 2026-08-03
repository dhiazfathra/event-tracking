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
	err := s.pool.QueryRow(ctx, `
		INSERT INTO session_offsets (tenant_id, device_id, session_id, offset_ms)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (tenant_id, device_id, session_id)
		DO UPDATE SET offset_ms = session_offsets.offset_ms
		RETURNING offset_ms`,
		k.TenantID, k.DeviceID, k.SessionID, candidate).Scan(&offset)
	if err != nil {
		return 0, fmt.Errorf("session offset: %w", err)
	}
	return offset, nil
}

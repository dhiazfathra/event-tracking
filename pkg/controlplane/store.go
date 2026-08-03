package controlplane

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dhiazfathra/event-tracking/pkg/limits"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

var ErrNotFound = errors.New("controlplane: not found")

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// ResolveTenant maps a public client ID to its tenant, rejecting revoked IDs.
func (s *Store) ResolveTenant(ctx context.Context, clientID string) (string, error) {
	var tenantID string
	err := s.pool.QueryRow(ctx,
		`SELECT tenant_id FROM client_ids WHERE client_id = $1 AND revoked_at IS NULL`,
		clientID).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return tenantID, err
}

// IssueInstall returns a stable install identity, creating one on first
// contact. Both upsert paths target a partial unique index, so a repeated
// exchange reuses the same install_id rather than minting a fresh rate-limit
// bucket each time.
func (s *Store) IssueInstall(ctx context.Context, tenantID, platform, subject, deviceKey string, tier uint8) (string, error) {
	newID := make([]byte, 16)
	if _, err := rand.Read(newID); err != nil {
		return "", fmt.Errorf("install id entropy: %w", err)
	}
	candidate := "ins_" + hex.EncodeToString(newID)

	var installID string
	var err error

	if subject != "" {
		err = s.pool.QueryRow(ctx, `
			INSERT INTO installs (install_id, tenant_id, platform, trust_tier, attest_subject, device_key)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (tenant_id, attest_subject) WHERE attest_subject IS NOT NULL
			DO UPDATE SET trust_tier = EXCLUDED.trust_tier
			RETURNING install_id`,
			candidate, tenantID, platform, tier, subject, deviceKey).Scan(&installID)
	} else {
		err = s.pool.QueryRow(ctx, `
			INSERT INTO installs (install_id, tenant_id, platform, trust_tier, attest_subject, device_key)
			VALUES ($1, $2, $3, $4, NULL, $5)
			ON CONFLICT (tenant_id, device_key) WHERE attest_subject IS NULL
			DO UPDATE SET trust_tier = EXCLUDED.trust_tier
			RETURNING install_id`,
			candidate, tenantID, platform, tier, deviceKey).Scan(&installID)
	}
	if err != nil {
		return "", fmt.Errorf("issue install: %w", err)
	}
	return installID, nil
}

func (s *Store) LimitsFor(ctx context.Context, tenantID string, tier uint8) (limits.Quota, error) {
	var lim limits.Quota
	var rps0, rps1 int
	err := s.pool.QueryRow(ctx,
		`SELECT daily_events, rps_tier0, rps_tier1, rps_legacy FROM quotas WHERE tenant_id = $1`,
		tenantID).Scan(&lim.DailyEvents, &rps0, &rps1, &lim.LegacyRPS)
	if errors.Is(err, pgx.ErrNoRows) {
		return limits.Quota{}, ErrNotFound
	}
	if err != nil {
		return limits.Quota{}, err
	}

	lim.RPS = rps0
	if tier == 1 {
		lim.RPS = rps1
	}
	return lim, nil
}

// ResolveLegacy backs tenant.LegacyResolver.
//
// Looks up by the SHA-256 digest of the bearer key, never the plaintext key —
// client_ids.client_id is a public identifier, not a place to compare secrets
// against, and a database or backup reader must not be able to recover an
// active legacy credential from this table.
func (s *Store) ResolveLegacy(ctx context.Context, key string) (string, tenant.LegacyMode, error) {
	sum := sha256.Sum256([]byte(key))

	var tenantID string
	var mode string
	err := s.pool.QueryRow(ctx, `
		SELECT c.tenant_id, t.legacy_key_mode
		FROM client_ids c JOIN tenants t USING (tenant_id)
		WHERE c.legacy_key_hash = $1 AND c.revoked_at IS NULL`,
		sum[:]).Scan(&tenantID, &mode)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrNotFound
	}
	return tenantID, tenant.LegacyMode(mode), err
}

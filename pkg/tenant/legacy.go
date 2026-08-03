package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// LegacyPrefix identifies the pre-token write keys. The same value becomes the
// bootstrap client_id at the exchange endpoint — it just stops working as an
// ingest bearer credential.
const LegacyPrefix = "wk_live_"

var ErrLegacyCutoff = errors.New("tenant: legacy write key no longer accepted for ingest")

type LegacyMode string

const (
	ModeDualAccept  LegacyMode = "dual_accept"
	ModeDeprecating LegacyMode = "deprecating"
	ModeCutoff      LegacyMode = "cutoff"
)

// LegacyResolver maps a legacy write key to its tenant and that tenant's
// current cutover mode. The mode is per-tenant config, not a deploy flag, so a
// stranded customer can be rolled back to dual-accept without a release.
type LegacyResolver interface {
	Resolve(ctx context.Context, key string) (tenantID string, mode LegacyMode, err error)
}

// VerifyOrLegacy accepts either a JWT or, during the cutover, a legacy write
// key. The second return value reports whether the legacy path was taken so the
// caller can apply the tighter rps_legacy limit and count the usage per tenant
// and SDK version — that count is what tells you when cutoff is safe.
func (v *Verifier) VerifyOrLegacy(ctx context.Context, bearer string, now time.Time, lr LegacyResolver) (Claims, bool, error) {
	raw := strings.TrimPrefix(bearer, "Bearer ")

	if !strings.HasPrefix(raw, LegacyPrefix) {
		c, err := v.Verify(ctx, bearer, now)
		return c, false, err
	}

	tenantID, mode, err := lr.Resolve(ctx, raw)
	if err != nil {
		return Claims{}, true, fmt.Errorf("resolve legacy key: %w", err)
	}
	switch mode {
	case ModeDualAccept, ModeDeprecating:
		// allowed
	case ModeCutoff:
		return Claims{}, true, ErrLegacyCutoff
	default:
		// Unknown/unset mode must fail closed, not fail open.
		return Claims{}, true, ErrLegacyCutoff
	}

	// A legacy key carries no attestation and no install identity. Tier 1 with
	// a key-derived bucket is the honest representation: it is rate limited
	// below Tier 1 by the caller's rps_legacy, and it is never sampled.
	sum := sha256.Sum256([]byte(raw))
	return Claims{
		TenantID:  tenantID,
		InstallID: "legacy-" + hex.EncodeToString(sum[:8]),
		Scope:     ScopeWriteEvents,
		TrustTier: 1,
	}, true, nil
}

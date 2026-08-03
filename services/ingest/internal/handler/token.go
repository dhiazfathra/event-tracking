package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/attest"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
)

type TokenDeps struct {
	Minter     *tenant.Minter
	Attestor   attest.Attestor
	Challenges attest.ChallengeStore

	// ResolveTenant maps a public client ID to a tenant. Returns an error for
	// unknown or revoked IDs.
	ResolveTenant func(ctx context.Context, clientID string) (string, error)

	// IssueInstall returns the server-side install identity, stable across app
	// restarts at both tiers. At Tier 0 it keys off the attestation subject; at
	// Tier 1 it keys off deviceKey (below).
	IssueInstall func(ctx context.Context, tenantID, platform, subject, deviceKey string, tier uint8) (string, error)

	// RateLimit caps token exchanges per client_id. Without it, an
	// unauthenticated caller can mint an unbounded number of installs rows by
	// rotating device_hint on every call. Optional: nil skips the check (used
	// by existing tests that don't exercise this).
	RateLimit *quota.Checker

	Now func() time.Time
}

// tokenRatePerMinute and tokenRateWindow bound POST /v1/auth/token per
// client_id. Generous on purpose — this guards against write amplification
// (installs row growth), not against a determined attacker; the tenant-wide
// ingest quota is the real budget protection.
const (
	tokenRatePerMinute = 30
	tokenRateWindow    = time.Minute
)

// NewToken serves POST /v1/auth/token: the public client ID plus a platform
// attestation are exchanged for a short-lived scoped JWT.
//
// The embedded client ID authorizes nothing on its own. Anything shipped inside
// a third party's mobile binary is extractable — that is a certainty, not a
// risk — so its extraction is designed to be boring.
func NewToken(d TokenDeps) http.Handler {
	if d.Now == nil {
		d.Now = func() time.Time { return time.Now().UTC() }
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			httpError(w, http.StatusBadRequest, "unreadable body")
			return
		}

		var req trackingv1.TokenRequest
		// Strict decoding. install_id is not a field of TokenRequest, so a
		// client that tries to supply one gets a 400 rather than a silently
		// ignored value — it is the primary rate-limit bucket, and being loud
		// about the attempt is better than being quietly correct.
		if err := protojson.Unmarshal(body, &req); err != nil {
			httpError(w, http.StatusBadRequest, "malformed request")
			return
		}

		tenantID, err := d.ResolveTenant(r.Context(), req.GetClientId())
		if err != nil {
			httpError(w, http.StatusUnauthorized, "unknown client_id")
			return
		}

		if d.RateLimit != nil {
			key := "rl:token:" + req.GetClientId()
			ok, err := d.RateLimit.AllowClient(r.Context(), key, tokenRatePerMinute, tokenRateWindow)
			if err != nil {
				httpError(w, http.StatusServiceUnavailable, "rate limit unavailable")
				return
			}
			if !ok {
				w.Header().Set("Retry-After", "60")
				httpError(w, http.StatusTooManyRequests, "rate_limit")
				return
			}
		}

		// Tier 0 requires a live, unspent challenge. A replayed attestation
		// fails the nonce check and falls back to Tier 1 rather than being
		// rejected — attestation assigns a tier, it never blocks.
		tier := uint8(1)
		subject := ""
		if d.Challenges.Consume(r.Context(), req.GetClientId(), req.GetPlatform(), req.GetChallenge()) {
			if s, ok := d.Attestor.Verify(
				r.Context(), req.GetPlatform(), req.GetAttestation(), req.GetChallenge()); ok {
				subject, tier = s, 0
			}
		}

		installID, err := d.IssueInstall(
			r.Context(), tenantID, req.GetPlatform(), subject, deviceKey(&req), tier)
		if err != nil {
			httpError(w, http.StatusServiceUnavailable, "install issuance unavailable")
			return
		}

		token, expiresIn, err := d.Minter.Mint(tenant.Claims{
			TenantID:  tenantID,
			InstallID: installID,
			Scope:     tenant.ScopeWriteEvents,
			TrustTier: tier,
		}, d.Now())
		if err != nil {
			httpError(w, http.StatusInternalServerError, "mint failed")
			return
		}

		out, err := protojson.Marshal(&trackingv1.TokenResponse{
			AccessToken: token,
			ExpiresIn:   expiresIn,
			TrustTier:   uint32(tier),
		})
		if err != nil {
			httpError(w, http.StatusInternalServerError, "encode")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(out)
	})
}

// NewChallenge serves POST /v1/auth/challenge, returning the one-time nonce the
// client embeds in its attestation.
func NewChallenge(d TokenDeps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			httpError(w, http.StatusBadRequest, "unreadable body")
			return
		}
		var req trackingv1.TokenRequest
		if err := protojson.Unmarshal(body, &req); err != nil {
			httpError(w, http.StatusBadRequest, "malformed request")
			return
		}
		if _, err := d.ResolveTenant(r.Context(), req.GetClientId()); err != nil {
			httpError(w, http.StatusUnauthorized, "unknown client_id")
			return
		}

		nonce, err := d.Challenges.Issue(r.Context(), req.GetClientId(), req.GetPlatform())
		if err != nil {
			httpError(w, http.StatusServiceUnavailable, "challenge unavailable")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]string{"challenge": nonce})
	})
}

// deviceKey is the Tier 1 install-identity anchor.
//
// Without it, a failed attestation leaves subject empty and every exchange
// mints a fresh install_id — which is the primary rate-limit bucket, so a
// client could reset its own limit at will just by re-exchanging. Note that
// Postgres treats NULLs as distinct, so a UNIQUE constraint on a nullable
// attest_subject does not prevent this on its own.
//
// This is a weak anchor by construction: the client supplies device_hint and
// can rotate it. It is not a security boundary — the tenant-wide daily quota
// remains the real budget protection. It exists so the *ordinary* Tier 1 client
// keeps one stable bucket across restarts, and so churning it is at least
// visible as an anomalous install-creation rate per tenant.
func deviceKey(req *trackingv1.TokenRequest) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		req.GetClientId(),
		req.GetPlatform(),
		req.GetDeviceHint(),
	}, "\x00")))
	return hex.EncodeToString(sum[:])
}

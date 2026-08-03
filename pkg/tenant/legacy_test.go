package tenant_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

type stubResolver struct {
	tenantID string
	mode     tenant.LegacyMode
	err      error
}

func (s stubResolver) Resolve(context.Context, string) (string, tenant.LegacyMode, error) {
	return s.tenantID, s.mode, s.err
}

func TestLegacyKeyAcceptedInDualAcceptMode(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	claims, isLegacy, err := v.VerifyOrLegacy(context.Background(), "Bearer wk_live_abc123", time.Now(),
		stubResolver{tenantID: "t1", mode: tenant.ModeDualAccept})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !isLegacy {
		t.Error("isLegacy = false, want true so the caller applies rps_legacy and logs the deprecation")
	}
	if claims.TenantID != "t1" {
		t.Errorf("tenant = %q, want t1", claims.TenantID)
	}
	// A legacy key has no attestation and no install identity. Tier 1 with a
	// key-derived install bucket is the honest representation.
	if claims.TrustTier != 1 {
		t.Errorf("trust_tier = %d, want 1", claims.TrustTier)
	}
	if claims.InstallID == "" {
		t.Error("no install_id derived; the rate limiter needs a bucket")
	}
}

// Cutoff is the whole point of the migration: a leaked key must stop working as
// an ingest bearer credential, or the short token lifetime buys nothing.
func TestLegacyKeyRejectedAtCutoff(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	_, _, err := v.VerifyOrLegacy(context.Background(), "Bearer wk_live_abc123", time.Now(),
		stubResolver{tenantID: "t1", mode: tenant.ModeCutoff})
	if !errors.Is(err, tenant.ErrLegacyCutoff) {
		t.Errorf("err = %v, want ErrLegacyCutoff", err)
	}
}

func TestLegacyKeyRejectedForUnknownMode(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	_, _, err := v.VerifyOrLegacy(context.Background(), "Bearer wk_live_abc123", time.Now(),
		stubResolver{tenantID: "t1"}) // zero-value mode
	if err == nil {
		t.Error("unknown legacy mode accepted; the mode check must fail closed")
	}
}

func TestLegacyResolverErrorRejects(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	_, _, err := v.VerifyOrLegacy(context.Background(), "Bearer wk_live_abc123", time.Now(),
		stubResolver{err: errors.New("db down")})
	if err == nil {
		t.Error("resolver error accepted")
	}
}

func TestJWTPathUnaffectedByLegacyResolver(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	claims, isLegacy, err := v.VerifyOrLegacy(context.Background(), mint(t, priv, "kid-1", nil), time.Now(),
		stubResolver{err: errors.New("resolver must not be consulted")})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if isLegacy {
		t.Error("isLegacy = true for a JWT")
	}
	if claims.TenantID != "t1" {
		t.Errorf("tenant = %q, want t1", claims.TenantID)
	}
}

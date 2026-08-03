package tenant_test

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

func TestMintProducesRequiredClaims(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	_ = pub

	m, err := tenant.NewMinter("kid-1", priv, "https://issuer.example", "https://ingest.example", 45*time.Minute)
	if err != nil {
		t.Fatalf("new minter: %v", err)
	}
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	raw, expiresIn, err := m.Mint(tenant.Claims{
		TenantID:  "t1",
		InstallID: "i-1",
		Scope:     "write:events",
		TrustTier: 0,
	}, now)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if expiresIn != 2700 {
		t.Errorf("expiresIn = %d, want 2700", expiresIn)
	}

	// Parse without verification to inspect claims; verification is Task 4.
	tok, err := jwt.Parse([]byte(raw), jwt.WithVerify(false), jwt.WithValidate(false))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	var tenantID, installID, scope string
	if err := tok.Get("tenant_id", &tenantID); err != nil || tenantID != "t1" {
		t.Errorf("tenant_id = %q (err %v), want t1", tenantID, err)
	}
	if err := tok.Get("install_id", &installID); err != nil || installID != "i-1" {
		t.Errorf("install_id = %q (err %v), want i-1", installID, err)
	}
	if err := tok.Get("scope", &scope); err != nil || scope != "write:events" {
		t.Errorf("scope = %q (err %v), want write:events", scope, err)
	}
	if exp, _ := tok.Expiration(); !exp.Equal(now.Add(45 * time.Minute)) {
		t.Errorf("exp = %v, want %v", exp, now.Add(45*time.Minute))
	}
}

// typ=at+jwt and alg=EdDSA are part of the acceptance contract on the verify
// side. If the minter stops setting them, every token stops being accepted.
func TestMintSetsHeaderTypAndAlg(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	m, err := tenant.NewMinter("kid-1", priv, "iss", "aud", time.Hour)
	if err != nil {
		t.Fatalf("new minter: %v", err)
	}

	raw, _, err := m.Mint(tenant.Claims{TenantID: "t1", InstallID: "i-1", Scope: "write:events"}, time.Now())
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	msg, err := jws.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("jws parse: %v", err)
	}
	hdr := msg.Signatures()[0].ProtectedHeaders()
	if alg, ok := hdr.Algorithm(); !ok || alg.String() != "EdDSA" {
		t.Errorf("alg = %q (ok %v), want EdDSA", alg, ok)
	}
	var typ string
	if err := hdr.Get("typ", &typ); err != nil || typ != "at+jwt" {
		t.Errorf("typ = %q (err %v), want at+jwt", typ, err)
	}
	if kid, ok := hdr.KeyID(); !ok || kid != "kid-1" {
		t.Errorf("kid = %q (ok %v), want kid-1", kid, ok)
	}
}

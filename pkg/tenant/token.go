// Package tenant owns tenant identity: minting short-lived ingest tokens,
// verifying them, and the legacy write-key cutover.
package tenant

import (
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

// TokenType is the required `typ` header. RFC 9068. Verification rejects
// anything else, which stops an ID token or a refresh token from being
// replayed at the ingest endpoint.
const TokenType = "at+jwt"

// ScopeWriteEvents means exactly that: no reads, no enumeration, no
// cross-tenant. The worst case of a fully abused pipeline is cost plus data
// pollution, never exfiltration.
const ScopeWriteEvents = "write:events"

// Claims are the tenant identity facts embedded in a minted ingest token.
// TenantID and InstallID are the only identifiers a verifier trusts on the
// hot path; both are set here, server-side, never echoed from client input.
type Claims struct {
	TenantID  string
	InstallID string
	Scope     string
	TrustTier uint8
}

// Minter issues short-lived EdDSA-signed ingest tokens for one signing key.
type Minter struct {
	kid      string
	key      jwk.Key
	issuer   string
	audience string
	ttl      time.Duration
}

func NewMinter(kid string, priv ed25519.PrivateKey, issuer, audience string, ttl time.Duration) (*Minter, error) {
	key, err := jwk.Import(priv)
	if err != nil {
		return nil, fmt.Errorf("import key: %w", err)
	}
	return &Minter{kid: kid, key: key, issuer: issuer, audience: audience, ttl: ttl}, nil
}

// Mint issues a short-lived ingest token. The token, not the embedded client
// ID, is the actual credential — which is what makes rotation an
// incident-response lever rather than an abuse control.
func (m *Minter) Mint(c Claims, now time.Time) (string, int64, error) {
	tok, err := jwt.NewBuilder().
		Issuer(m.issuer).
		Audience([]string{m.audience}).
		IssuedAt(now).
		NotBefore(now).
		Expiration(now.Add(m.ttl)).
		Claim("tenant_id", c.TenantID).
		Claim("install_id", c.InstallID).
		Claim("scope", c.Scope).
		Claim("trust_tier", int(c.TrustTier)).
		Build()
	if err != nil {
		return "", 0, fmt.Errorf("build token: %w", err)
	}

	hdrs := jws.NewHeaders()
	if err := hdrs.Set("typ", TokenType); err != nil {
		return "", 0, fmt.Errorf("set typ header: %w", err)
	}
	if err := hdrs.Set(jws.KeyIDKey, m.kid); err != nil {
		return "", 0, fmt.Errorf("set kid header: %w", err)
	}

	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.EdDSA(), m.key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		return "", 0, fmt.Errorf("sign: %w", err)
	}
	return string(signed), int64(m.ttl.Seconds()), nil
}

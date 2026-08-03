package tenant_test

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

const (
	testIssuer   = "https://issuer.example"
	testAudience = "https://ingest.example"
)

type jwksServer struct {
	*httptest.Server
	hits int
}

func newJWKS(t *testing.T, kid string, pub ed25519.PublicKey, use, kty string) *jwksServer {
	t.Helper()
	key, err := jwk.Import(pub)
	if err != nil {
		t.Fatalf("import pub: %v", err)
	}
	_ = key.Set(jwk.KeyIDKey, kid)
	_ = key.Set(jwk.KeyUsageKey, use)
	set := jwk.NewSet()
	_ = set.AddKey(key)

	s := &jwksServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(s.Close)
	return s
}

func mint(t *testing.T, priv ed25519.PrivateKey, kid string, mutate func(*jwt.Builder)) string {
	t.Helper()
	now := time.Now()
	b := jwt.NewBuilder().
		Issuer(testIssuer).
		Audience([]string{testAudience}).
		IssuedAt(now).NotBefore(now).Expiration(now.Add(time.Hour)).
		Claim("tenant_id", "t1").
		Claim("install_id", "i-1").
		Claim("scope", tenant.ScopeWriteEvents).
		Claim("trust_tier", 0)
	if mutate != nil {
		mutate(b)
	}
	tok, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	hdrs := jws.NewHeaders()
	_ = hdrs.Set("typ", tenant.TokenType)
	_ = hdrs.Set(jws.KeyIDKey, kid)
	key, _ := jwk.Import(priv)
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.EdDSA(), key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return string(signed)
}

func TestVerifyHappyPath(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	claims, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), time.Now())
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.TenantID != "t1" || claims.InstallID != "i-1" || claims.TrustTier != 0 {
		t.Errorf("claims = %+v, want tenant t1 / install i-1 / tier 0", claims)
	}
}

// alg=none is the oldest JWT bypass there is. The allowlist must be consulted
// before verification, never inferred from the token's own header.
func TestVerifyRejectsAlgNone(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	// Hand-built unsigned token: header {"alg":"none","typ":"at+jwt","kid":"kid-1"}
	unsigned := "eyJhbGciOiJub25lIiwidHlwIjoiYXQrand0Iiwia2lkIjoia2lkLTEifQ." +
		"eyJ0ZW5hbnRfaWQiOiJ0MSJ9."

	if _, err := v.Verify(context.Background(), unsigned, time.Now()); !errors.Is(err, tenant.ErrBadAlg) {
		t.Errorf("err = %v, want ErrBadAlg", err)
	}
}

// An HMAC token signed with the *public* key as the secret is the classic
// confusion attack against an asymmetric verifier.
func TestVerifyRejectsHMACAlgorithm(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	tok, _ := jwt.NewBuilder().Issuer(testIssuer).Claim("tenant_id", "t1").Build()
	hdrs := jws.NewHeaders()
	_ = hdrs.Set("typ", tenant.TokenType)
	_ = hdrs.Set(jws.KeyIDKey, "kid-1")
	hmacKey, _ := jwk.Import([]byte(pub))
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256(), hmacKey, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatalf("sign hmac: %v", err)
	}

	if _, err := v.Verify(context.Background(), string(signed), time.Now()); !errors.Is(err, tenant.ErrBadAlg) {
		t.Errorf("err = %v, want ErrBadAlg", err)
	}
}

// A valid signature alone does not bind a token to this endpoint. iss/aud are
// what stop a token minted for another audience being replayed here.
func TestVerifyRejectsWrongAudienceAndIssuer(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	wrongAud := mint(t, priv, "kid-1", func(b *jwt.Builder) { b.Audience([]string{"https://other.example"}) })
	if _, err := v.Verify(context.Background(), wrongAud, time.Now()); !errors.Is(err, tenant.ErrBadAudience) {
		t.Errorf("wrong aud: err = %v, want ErrBadAudience", err)
	}

	wrongIss := mint(t, priv, "kid-1", func(b *jwt.Builder) { b.Issuer("https://evil.example") })
	if _, err := v.Verify(context.Background(), wrongIss, time.Now()); !errors.Is(err, tenant.ErrBadIssuer) {
		t.Errorf("wrong iss: err = %v, want ErrBadIssuer", err)
	}
}

func TestVerifyRejectsMissingScope(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	tok := mint(t, priv, "kid-1", func(b *jwt.Builder) { b.Claim("scope", "read:events") })
	if _, err := v.Verify(context.Background(), tok, time.Now()); !errors.Is(err, tenant.ErrBadScope) {
		t.Errorf("err = %v, want ErrBadScope", err)
	}
}

func TestVerifyRejectsExpiredBeyondSkew(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	tok := mint(t, priv, "kid-1", nil)
	future := time.Now().Add(2 * time.Hour) // token exp was now+1h
	if _, err := v.Verify(context.Background(), tok, future); !errors.Is(err, tenant.ErrExpired) {
		t.Errorf("err = %v, want ErrExpired", err)
	}
}

// A key published for encryption, or of the wrong type, must be rejected even
// if the signature would verify.
func TestVerifyRejectsKeyNotMarkedForSigning(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "enc", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), time.Now()); !errors.Is(err, tenant.ErrUnknownKID) {
		t.Errorf("err = %v, want ErrUnknownKID", err)
	}
}

// "Unspecified" must not be read as "signing".
func TestVerifyRejectsKeyWithNoUse(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), time.Now()); !errors.Is(err, tenant.ErrUnknownKID) {
		t.Errorf("err = %v, want ErrUnknownKID for a key with no use field", err)
	}
}

// Containment is not enough: a token minted for several audiences, one of them
// ours, was issued for somebody else's purpose.
func TestVerifyRejectsExtraAudiences(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	tok := mint(t, priv, "kid-1", func(b *jwt.Builder) {
		b.Audience([]string{testAudience, "https://other.example"})
	})
	if _, err := v.Verify(context.Background(), tok, time.Now()); !errors.Is(err, tenant.ErrBadAudience) {
		t.Errorf("err = %v, want ErrBadAudience", err)
	}
}

// A missing trust_tier must not silently become Tier 0, the privileged tier.
func TestVerifyRejectsMissingOrInvalidTrustTier(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	cases := map[string]func(*jwt.Builder){
		"missing":  func(b *jwt.Builder) { b.Claim("trust_tier", nil) },
		"negative": func(b *jwt.Builder) { b.Claim("trust_tier", -1) },
		"oversize": func(b *jwt.Builder) { b.Claim("trust_tier", 256) },
		"unknown":  func(b *jwt.Builder) { b.Claim("trust_tier", 7) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", mutate), time.Now()); err == nil {
				t.Error("accepted a token with an invalid trust_tier")
			}
		})
	}
}

// A JWKS outage may be ridden out briefly, never indefinitely.
func TestVerifyFailsClosedOnceStaleGraceExpires(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	start := time.Now()
	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), start); err != nil {
		t.Fatalf("prime: %v", err)
	}
	js.Close() // JWKS endpoint goes away

	// Inside the grace window: still verifiable.
	within := start.Add(11 * time.Minute)
	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), within); err != nil {
		t.Errorf("rejected inside the stale grace window: %v", err)
	}

	// Past it: fails closed rather than trusting a key set of unknown age.
	beyond := start.Add(30 * time.Minute)
	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), beyond); err == nil {
		t.Error("accepted a token against an indefinitely stale JWKS")
	}
}

// A forged kid must not be able to drive unbounded JWKS traffic. One refetch,
// rate-limited, then the answer is cached-miss.
func TestVerifyForgedKIDRefetchesAtMostOnce(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	// Prime the cache.
	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), time.Now()); err != nil {
		t.Fatalf("prime: %v", err)
	}
	before := js.hits

	for i := 0; i < 20; i++ {
		_, _ = v.Verify(context.Background(), mint(t, priv, "forged-kid", nil), time.Now())
	}

	if extra := js.hits - before; extra > 1 {
		t.Errorf("JWKS refetches for forged kid = %d, want at most 1", extra)
	}
}

// Garbage input must not panic the verifier or reach signature verification.
func TestVerifyRejectsMalformedToken(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	if _, err := v.Verify(context.Background(), "not-a-jwt", time.Now()); !errors.Is(err, tenant.ErrMalformed) {
		t.Errorf("err = %v, want ErrMalformed", err)
	}
}

// A missing kid can never resolve to a key; this must fail the same way a
// forged kid does, without touching the JWKS endpoint.
func TestVerifyRejectsEmptyKID(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	tok := mint(t, priv, "", nil)
	if _, err := v.Verify(context.Background(), tok, time.Now()); !errors.Is(err, tenant.ErrUnknownKID) {
		t.Errorf("err = %v, want ErrUnknownKID", err)
	}
}

// NewVerifier must be usable with a nil client, defaulting to one with a
// sane timeout rather than panicking on first fetch.
func TestNewVerifierDefaultsHTTPClient(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, nil)

	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), time.Now()); err != nil {
		t.Fatalf("verify with default client: %v", err)
	}
}

// A wrong typ (e.g. an ID token or refresh token replayed here) must be
// rejected even with a valid signature and otherwise-correct claims.
func TestVerifyRejectsBadTyp(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	now := time.Now()
	tok, err := jwt.NewBuilder().
		Issuer(testIssuer).
		Audience([]string{testAudience}).
		IssuedAt(now).NotBefore(now).Expiration(now.Add(time.Hour)).
		Claim("tenant_id", "t1").
		Claim("install_id", "i-1").
		Claim("scope", tenant.ScopeWriteEvents).
		Claim("trust_tier", 0).
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	hdrs := jws.NewHeaders()
	_ = hdrs.Set("typ", "JWT") // wrong: not at+jwt
	_ = hdrs.Set(jws.KeyIDKey, "kid-1")
	key, _ := jwk.Import(priv)
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.EdDSA(), key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	if _, err := v.Verify(context.Background(), string(signed), time.Now()); !errors.Is(err, tenant.ErrBadType) {
		t.Errorf("err = %v, want ErrBadType", err)
	}
}

// A null tenant_id/install_id claim must fail closed with ErrMalformed, not
// panic the ingest hot path. jwx's Token.Get panics assigning a JSON null
// into a non-pointer destination; the verifier must recover from that.
func TestVerifyRejectsNullOrMissingTenantAndInstallID(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	v := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())

	cases := map[string]func(*jwt.Builder){
		"null tenant_id":  func(b *jwt.Builder) { b.Claim("tenant_id", nil) },
		"null install_id": func(b *jwt.Builder) { b.Claim("install_id", nil) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			tok := mint(t, priv, "kid-1", mutate)
			_, err := v.Verify(context.Background(), tok, time.Now())
			if !errors.Is(err, tenant.ErrMalformed) {
				t.Errorf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

// An unreachable JWKS with no prior cache must fail closed immediately.
func TestVerifyFailsClosedWithNoCacheAndUnreachableJWKS(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	js := newJWKS(t, "kid-1", pub, "sig", "OKP")
	url := js.URL
	client := js.Client()
	js.Close()

	v := tenant.NewVerifier(url, testIssuer, testAudience, client)
	if _, err := v.Verify(context.Background(), mint(t, priv, "kid-1", nil), time.Now()); !errors.Is(err, tenant.ErrUnknownKID) {
		t.Errorf("err = %v, want ErrUnknownKID", err)
	}
}

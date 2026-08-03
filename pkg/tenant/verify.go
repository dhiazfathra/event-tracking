package tenant

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

var (
	ErrBadAlg      = errors.New("tenant: unsupported token algorithm")
	ErrBadType     = errors.New("tenant: unsupported token type")
	ErrBadIssuer   = errors.New("tenant: issuer mismatch")
	ErrBadAudience = errors.New("tenant: audience mismatch")
	ErrExpired     = errors.New("tenant: token expired or not yet valid")
	ErrBadScope    = errors.New("tenant: token lacks write:events scope")
	ErrUnknownKID  = errors.New("tenant: kid not found in JWKS")
	ErrMalformed   = errors.New("tenant: malformed token")
)

// clockSkew is the tolerance on exp/nbf. Kept tight: this is server-to-server
// time drift, not the multi-year mobile clock skew handled in enrich.
const clockSkew = 60 * time.Second

// jwksMinRefetch rate-limits refetches so forged kid values cannot drive
// unbounded traffic to the JWKS endpoint.
const jwksMinRefetch = 30 * time.Second

// jwksTTL bounds how long a cached key set is trusted. Rotation overlaps old
// and new keys so in-flight tokens stay verifiable across a rotation.
const jwksTTL = 10 * time.Minute

// jwksStaleGrace is how far past the TTL a cached key set may still be used
// when the JWKS endpoint is unreachable. Bounded on purpose: a brief outage
// should not stop ingestion, but an indefinite one must not keep a retired key
// verifiable forever. Past this, verification fails closed.
const jwksStaleGrace = 5 * time.Minute

type Verifier struct {
	url      string
	issuer   string
	audience string
	client   *http.Client

	mu          sync.Mutex
	set         jwk.Set
	fetchedAt   time.Time
	lastRefetch time.Time
}

func NewVerifier(jwksURL, issuer, audience string, client *http.Client) *Verifier {
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	return &Verifier{url: jwksURL, issuer: issuer, audience: audience, client: client}
}

// stripBearer removes a case-insensitive "Bearer " scheme prefix per RFC
// 6750/7235. Shared by Verify and VerifyOrLegacy so JWT and legacy-key
// classification never disagree about where the scheme ends.
func stripBearer(bearer string) string {
	if scheme, rest, ok := strings.Cut(bearer, " "); ok && strings.EqualFold(scheme, "Bearer") {
		return strings.TrimSpace(rest)
	}
	return bearer
}

// Verify checks every acceptance condition and returns the claims. Ingest is a
// hot path: this does signature verification and claim checks only — no
// database lookup, no attestation. Attestation happened at the exchange.
func (v *Verifier) Verify(ctx context.Context, bearer string, now time.Time) (Claims, error) {
	raw := []byte(stripBearer(bearer))
	msg, err := jws.Parse(raw)
	if err != nil || len(msg.Signatures()) != 1 {
		return Claims{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	hdr := msg.Signatures()[0].ProtectedHeaders()

	// Algorithm allowlist, checked BEFORE verification and never inferred from
	// the header. "none" and every HMAC family are rejected outright here, so
	// they never reach a code path that could be tricked into using the public
	// key as a shared secret.
	alg, ok := hdr.Algorithm()
	if !ok || alg.String() != jwa.EdDSA().String() {
		return Claims{}, fmt.Errorf("%w: %s", ErrBadAlg, alg)
	}

	var typ string
	if err := hdr.Get("typ", &typ); err != nil || typ != TokenType {
		return Claims{}, fmt.Errorf("%w: %q", ErrBadType, typ)
	}

	kid, _ := hdr.KeyID()
	key, err := v.key(ctx, kid, now)
	if err != nil {
		return Claims{}, err
	}

	tok, err := jwt.Parse(raw,
		jwt.WithKey(jwa.EdDSA(), key),
		jwt.WithValidate(false), // claim checks are explicit below
	)
	if err != nil {
		return Claims{}, fmt.Errorf("%w: signature: %v", ErrMalformed, err)
	}

	if iss, ok := tok.Issuer(); !ok || iss != v.issuer {
		return Claims{}, fmt.Errorf("%w: %q", ErrBadIssuer, iss)
	}
	// Exact match, not containment. A token carrying this audience alongside
	// others was minted for a different purpose and happens to name us; the
	// contract says aud matches the ingest audience exactly.
	aud, _ := tok.Audience()
	if len(aud) != 1 || aud[0] != v.audience {
		return Claims{}, fmt.Errorf("%w: %v", ErrBadAudience, aud)
	}
	if exp, ok := tok.Expiration(); !ok || now.After(exp.Add(clockSkew)) {
		return Claims{}, ErrExpired
	}
	if nbf, ok := tok.NotBefore(); ok && now.Add(clockSkew).Before(nbf) {
		return Claims{}, ErrExpired
	}

	var scope string
	_ = safeGet(tok, "scope", &scope)
	if !hasScope(scope, ScopeWriteEvents) {
		return Claims{}, fmt.Errorf("%w: %q", ErrBadScope, scope)
	}

	var c Claims
	if err := safeGet(tok, "tenant_id", &c.TenantID); err != nil || c.TenantID == "" {
		return Claims{}, fmt.Errorf("%w: missing tenant_id", ErrMalformed)
	}
	if err := safeGet(tok, "install_id", &c.InstallID); err != nil || c.InstallID == "" {
		return Claims{}, fmt.Errorf("%w: missing install_id", ErrMalformed)
	}
	c.Scope = scope

	// The claim is required and closed to the two defined tiers. Swallowing the
	// error would make a missing claim mean Tier 0 — the *privileged* tier — and
	// a bare uint8 conversion would wrap a negative or oversized value into it.
	// jwt.Token decodes numeric claims as float64 (they round-trip through
	// encoding/json), so that is the type asserted here rather than int.
	if !tok.Has("trust_tier") {
		return Claims{}, fmt.Errorf("%w: missing trust_tier", ErrMalformed)
	}
	var tierRaw any
	if err := safeGet(tok, "trust_tier", &tierRaw); err != nil || tierRaw == nil {
		return Claims{}, fmt.Errorf("%w: missing trust_tier", ErrMalformed)
	}
	tierF, ok := tierRaw.(float64)
	if !ok {
		return Claims{}, fmt.Errorf("%w: trust_tier not a number", ErrMalformed)
	}
	tier := int(tierF)
	if float64(tier) != tierF || (tier != 0 && tier != 1) {
		return Claims{}, fmt.Errorf("%w: trust_tier %v", ErrMalformed, tierRaw)
	}
	c.TrustTier = uint8(tier)

	return c, nil
}

// key resolves a kid to a signing key, refetching the JWKS at most once per
// jwksMinRefetch window on a miss.
func (v *Verifier) key(ctx context.Context, kid string, now time.Time) (jwk.Key, error) {
	if kid == "" {
		return nil, ErrUnknownKID
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	stale := v.set == nil || now.Sub(v.fetchedAt) > jwksTTL
	if stale {
		// Rate-limit refetch attempts the same as the kid-miss path below.
		// Without this, every request during a JWKS outage queues behind a
		// fetch that will time out, serializing the hot path.
		var err error
		if now.Sub(v.lastRefetch) >= jwksMinRefetch {
			err = v.refetchLocked(ctx, now)
		} else {
			err = errors.New("refresh suppressed by rate limit")
		}
		if err != nil {
			// Serving from a cached set after a failed refresh is deliberate —
			// a JWKS blip must not take ingestion down — but it is bounded.
			// Past the grace period the set is discarded: continuing to trust
			// it indefinitely would keep a retired or compromised signing key
			// valid for as long as the endpoint stays unreachable.
			if v.set == nil || now.Sub(v.fetchedAt) > jwksTTL+jwksStaleGrace {
				v.set = nil
				return nil, fmt.Errorf("%w: JWKS unavailable and cache expired: %v", ErrUnknownKID, err)
			}
		}
	}

	if k, ok := lookup(v.set, kid); ok {
		return k, nil
	}

	// Miss. A rotation may have just published a new kid — but a forged kid
	// must not be able to hammer the JWKS endpoint, so this is rate-limited.
	if now.Sub(v.lastRefetch) >= jwksMinRefetch {
		if err := v.refetchLocked(ctx, now); err != nil {
			return nil, fmt.Errorf("%w: refetch: %v", ErrUnknownKID, err)
		}
		if k, ok := lookup(v.set, kid); ok {
			return k, nil
		}
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownKID, kid)
}

func (v *Verifier) refetchLocked(ctx context.Context, now time.Time) error {
	v.lastRefetch = now
	set, err := jwk.Fetch(ctx, v.url, jwk.WithHTTPClient(v.client))
	if err != nil {
		return err
	}
	v.set = set
	v.fetchedAt = now
	return nil
}

// lookup enforces the key-shape requirements: use=sig, kty=OKP, crv=Ed25519.
// A key published for any other purpose or type is rejected even if the
// signature would verify against it.
func lookup(set jwk.Set, kid string) (jwk.Key, bool) {
	if set == nil {
		return nil, false
	}
	k, ok := set.LookupKeyID(kid)
	if !ok {
		return nil, false
	}
	// Explicitly marked for signing. An absent `use` is rejected too: the
	// contract requires the key to be published for this purpose, and treating
	// "unspecified" as "signing" is how an encryption key ends up verifying
	// tokens.
	use, ok := k.KeyUsage()
	if !ok || use != "sig" {
		return nil, false
	}
	if k.KeyType().String() != "OKP" {
		return nil, false
	}
	var crv jwa.EllipticCurveAlgorithm
	if err := k.Get("crv", &crv); err != nil || crv.String() != "Ed25519" {
		return nil, false
	}
	return k, true
}

// safeGet wraps jwt.Token.Get: the underlying JWX library panics trying to
// assign a JSON null claim value into an interface{} destination. A null
// trust_tier claim must be a rejection, not a crash, so the panic is turned
// into an error here.
func safeGet(tok jwt.Token, name string, dst any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: panic reading %q: %v", ErrMalformed, name, r)
		}
	}()
	return tok.Get(name, dst)
}

func hasScope(scope, want string) bool {
	for _, s := range strings.Fields(scope) {
		if s == want {
			return true
		}
	}
	return false
}

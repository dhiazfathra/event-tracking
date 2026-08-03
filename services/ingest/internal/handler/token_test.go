package handler_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/jwx/v3/jwt"
	"google.golang.org/protobuf/encoding/protojson"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/attest"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/handler"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
)

// failingAttestor always reports failure, exercising the Tier 1 fallback path.
type failingAttestor struct{}

func (failingAttestor) Verify(context.Context, string, string, string) (string, bool) {
	return "", false
}

// passingAttestor always succeeds with a fixed subject.
type passingAttestor struct{ subject string }

func (p passingAttestor) Verify(context.Context, string, string, string) (string, bool) {
	return p.subject, true
}

// memInstalls is an in-memory stand-in for the Postgres upsert IssueInstall
// wraps: one bucket per attested subject, one per unattested device key.
type memInstalls struct {
	mu   sync.Mutex
	byID map[string]string
}

func (m *memInstalls) issue(_ context.Context, tenantID, platform, subject, deviceKey string, tier uint8) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.byID == nil {
		m.byID = map[string]string{}
	}
	bucket := subject
	if bucket == "" {
		bucket = deviceKey
	}
	key := tenantID + "|" + platform + "|" + bucket
	if id, ok := m.byID[key]; ok {
		return id, nil
	}
	id := uuid.NewString()
	m.byID[key] = id
	return id, nil
}

func newTokenHandler(t *testing.T, attestor attest.Attestor) (http.Handler, attest.ChallengeStore) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	_ = pub
	minter := tenant.NewMinter(testKID, priv, testIssuer, testAudience, 30*time.Minute)

	rdb := startRedis(t)
	challenges := attest.RedisChallenges{RDB: rdb, TTL: 5 * time.Minute}

	installs := &memInstalls{}

	h := handler.NewToken(handler.TokenDeps{
		Minter:     minter,
		Attestor:   attestor,
		Challenges: challenges,
		ResolveTenant: func(_ context.Context, clientID string) (string, error) {
			if clientID != "pk_live_abc" {
				return "", errors.New("unknown client")
			}
			return "t1", nil
		},
		IssueInstall: installs.issue,
	})
	return h, challenges
}

func issueChallenge(t *testing.T, challenges attest.ChallengeStore, clientID, platform string) string {
	t.Helper()
	nonce, err := challenges.Issue(context.Background(), clientID, platform)
	if err != nil {
		t.Fatalf("issue challenge: %v", err)
	}
	return nonce
}

func postJSON(t *testing.T, h http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// installIDFromToken parses the JWT without verifying its signature and reads
// the install_id claim — this test only cares what the handler put in the
// token, not that pkg/tenant signs correctly (that's Task 3's own suite).
func installIDFromToken(t *testing.T, token string) string {
	t.Helper()
	tok, err := jwt.Parse([]byte(token), jwt.WithVerify(false))
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	var installID string
	if err := tok.Get("install_id", &installID); err != nil {
		t.Fatalf("read install_id claim: %v", err)
	}
	return installID
}

// Attestation failure must not block. Rooted, custom-ROM, and de-Googled
// devices fail legitimately; simulators fail; Play Integrity is quota-limited.
func TestAttestationFailureYieldsTier1NotRejection(t *testing.T) {
	h, _ := newTokenHandler(t, failingAttestor{})

	rec := postJSON(t, h, "/v1/auth/token", `{"clientId":"pk_live_abc","platform":"android","attestation":"garbage"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — attestation failure assigns a tier, it does not block", rec.Code)
	}

	var resp trackingv1.TokenResponse
	decodeToken(t, rec.Body.Bytes(), &resp)
	if resp.TrustTier != 1 {
		t.Errorf("trust_tier = %d, want 1", resp.TrustTier)
	}
	if resp.AccessToken == "" {
		t.Error("no token issued")
	}
}

func TestSuccessfulAttestationYieldsTier0(t *testing.T) {
	h, challenges := newTokenHandler(t, passingAttestor{subject: "attest-subject-1"})

	nonce := issueChallenge(t, challenges, "pk_live_abc", "ios")
	rec := postJSON(t, h, "/v1/auth/token",
		`{"clientId":"pk_live_abc","platform":"ios","attestation":"valid","challenge":"`+nonce+`"}`)
	var resp trackingv1.TokenResponse
	decodeToken(t, rec.Body.Bytes(), &resp)

	if resp.TrustTier != 0 {
		t.Errorf("trust_tier = %d, want 0", resp.TrustTier)
	}
	if resp.ExpiresIn < 1800 || resp.ExpiresIn > 3600 {
		t.Errorf("expires_in = %d, want 1800..3600", resp.ExpiresIn)
	}
}

func TestUnknownClientIDReturns401(t *testing.T) {
	h, _ := newTokenHandler(t, passingAttestor{})
	rec := postJSON(t, h, "/v1/auth/token", `{"clientId":"pk_live_unknown","platform":"ios"}`)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// One captured attestation must not mint Tier 0 tokens forever. The nonce is
// single-use; a replay degrades to Tier 1 rather than being rejected outright,
// because attestation assigns a tier and never blocks.
func TestReplayedAttestationDegradesToTier1(t *testing.T) {
	h, challenges := newTokenHandler(t, passingAttestor{subject: "attest-subject-1"})

	nonce := issueChallenge(t, challenges, "pk_live_abc", "ios")
	body := `{"clientId":"pk_live_abc","platform":"ios","attestation":"valid","challenge":"` + nonce + `"}`

	var first trackingv1.TokenResponse
	decodeToken(t, postJSON(t, h, "/v1/auth/token", body).Body.Bytes(), &first)
	if first.TrustTier != 0 {
		t.Fatalf("first exchange trust_tier = %d, want 0", first.TrustTier)
	}

	var replay trackingv1.TokenResponse
	rec := postJSON(t, h, "/v1/auth/token", body)
	decodeToken(t, rec.Body.Bytes(), &replay)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 — a replay is not an error", rec.Code)
	}
	if replay.TrustTier != 1 {
		t.Errorf("replay trust_tier = %d, want 1", replay.TrustTier)
	}
}

// A Tier 1 client must keep one install bucket across exchanges. A fresh
// install_id each time would reset its own rate limit at will.
func TestRepeatedTier1ExchangesReuseTheInstallBucket(t *testing.T) {
	h, _ := newTokenHandler(t, failingAttestor{})
	body := `{"clientId":"pk_live_abc","platform":"android","deviceHint":"stable-device-1"}`

	var seen []string
	for i := 0; i < 3; i++ {
		var resp trackingv1.TokenResponse
		decodeToken(t, postJSON(t, h, "/v1/auth/token", body).Body.Bytes(), &resp)
		seen = append(seen, installIDFromToken(t, resp.AccessToken))
	}

	if seen[0] != seen[1] || seen[1] != seen[2] {
		t.Errorf("install_id changed across exchanges: %v — the rate-limit bucket resets", seen)
	}
}

// A client must never be able to choose its own install_id: it is the primary
// rate-limit bucket, so choosing it means resetting the limit at will.
func TestClientSuppliedInstallIDIsIgnored(t *testing.T) {
	h, challenges := newTokenHandler(t, passingAttestor{subject: "s1"})

	nonce := issueChallenge(t, challenges, "pk_live_abc", "ios")
	rec := postJSON(t, h, "/v1/auth/token",
		`{"clientId":"pk_live_abc","platform":"ios","attestation":"valid","challenge":"`+nonce+
			`","installId":"i-chosen-by-attacker"}`)

	// Strict decoding rejects the unknown installId outright — which is the
	// strongest form of "never client-supplied" available.
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an unknown installId field", rec.Code)
	}
}

// Without a rate limit, rotating device_hint on every call mints an
// unbounded number of installs rows per tenant. RateLimit caps exchanges per
// client_id regardless of what's inside the body.
func TestTokenExchangeIsRateLimitedPerClient(t *testing.T) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	_ = pub
	minter := tenant.NewMinter(testKID, priv, testIssuer, testAudience, 30*time.Minute)
	rdb := startRedis(t)
	installs := &memInstalls{}

	h := handler.NewToken(handler.TokenDeps{
		Minter:     minter,
		Attestor:   failingAttestor{},
		Challenges: attest.RedisChallenges{RDB: rdb, TTL: 5 * time.Minute},
		ResolveTenant: func(_ context.Context, clientID string) (string, error) {
			return "t1", nil
		},
		IssueInstall: installs.issue,
		RateLimit:    quota.NewChecker(rdb),
	})

	body := `{"clientId":"pk_live_abc","platform":"android","deviceHint":"stable-device-1"}`
	var last *httptest.ResponseRecorder
	for i := 0; i < 40; i++ {
		last = postJSON(t, h, "/v1/auth/token", body)
		if last.Code == http.StatusTooManyRequests {
			break
		}
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d after 40 exchanges, want 429 eventually", last.Code)
	}
	if last.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header on 429")
	}
}

func decodeToken(t *testing.T, b []byte, v *trackingv1.TokenResponse) {
	t.Helper()
	if err := protojson.Unmarshal(b, v); err != nil {
		t.Fatalf("decode token response: %v (body: %s)", err, b)
	}
}

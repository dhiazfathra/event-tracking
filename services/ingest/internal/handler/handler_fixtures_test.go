package handler_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
	"github.com/redis/go-redis/v9"

	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/limits"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/handler"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
)

const (
	testIssuer   = "https://issuer.example"
	testAudience = "https://ingest.example"
	testKID      = "kid-1"
)

// sink records both what was inserted and which session keys were resolved,
// so the per-session offset test can assert on the second.
type sink struct {
	rows       []clickhouse.Row
	offsetKeys []enrich.SessionKey
}

// recordingOffsets wraps a real store and notes every key it was asked for.
type recordingOffsets struct {
	inner enrich.OffsetStore
	sink  *sink
}

func (r *recordingOffsets) GetOrSet(ctx context.Context, k enrich.SessionKey, candidate int64) (int64, error) {
	r.sink.offsetKeys = append(r.sink.offsetKeys, k)
	return r.inner.GetOrSet(ctx, k, candidate)
}

// startRedis backs the quota checker with an in-process miniredis instead of
// a testcontainers-managed real Redis: this sandbox has no Docker daemon, and
// the handler tests exercise handler wiring, not Redis itself (that's the
// quota package's own testcontainers-based suite, task 8). miniredis supports
// the EVAL script quota.Checker relies on, so the real code path under test is
// unchanged.
func startRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

// newJWKS mints a signing key pair behind a JWKS httptest server, and returns
// a verifier primed against it plus a function to mint valid tenant=t1 tokens.
func newJWKS(t *testing.T) (*httptest.Server, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	key, err := jwk.Import(pub)
	if err != nil {
		t.Fatalf("import pub: %v", err)
	}
	_ = key.Set(jwk.KeyIDKey, testKID)
	_ = key.Set(jwk.KeyUsageKey, "sig")
	set := jwk.NewSet()
	_ = set.AddKey(key)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(s.Close)
	return s, priv
}

func mintToken(t *testing.T, priv ed25519.PrivateKey) string {
	t.Helper()
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
	_ = hdrs.Set("typ", tenant.TokenType)
	_ = hdrs.Set(jws.KeyIDKey, testKID)
	key, _ := jwk.Import(priv)
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.EdDSA(), key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return string(signed)
}

func unlimitedLimits(context.Context, string, uint8) (limits.Quota, error) {
	return limits.Quota{DailyEvents: 1_000_000, RPS: 1_000_000}, nil
}

func newTestHandler(t *testing.T) (http.Handler, *sink) {
	t.Helper()
	js, priv := newJWKS(t)
	verifier := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())
	rdb := startRedis(t)

	s := &sink{}
	offsets := &recordingOffsets{inner: enrich.NewMemoryOffsetStore(), sink: s}
	token := mintToken(t, priv)

	h := handler.NewBatch(handler.Deps{
		Verifier:  verifier,
		Offsets:   offsets,
		Quota:     quota.NewChecker(rdb),
		LimitsFor: unlimitedLimits,
		Insert: func(_ context.Context, rows []clickhouse.Row) error {
			s.rows = append(s.rows, rows...)
			return nil
		},
	})

	return authWrapper(h, token), s
}

// authWrapper injects the bearer token automatically so callers of post() do
// not have to thread it through every test.
type authHandler struct {
	inner http.Handler
	token string
}

func (a authHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") == "" {
		r.Header.Set("Authorization", "Bearer "+a.token)
	}
	a.inner.ServeHTTP(w, r)
}

func authWrapper(inner http.Handler, token string) http.Handler {
	return authHandler{inner: inner, token: token}
}

// newTestHandlerWithQuota wires a quota checker whose budget is already
// exhausted when dailyBudget is 0. Note: quota.Allow treats a <=0 limit as
// "unenforced" (its Lua script only compares when rps/daily > 0), so a literal
// zero-value Quota would let every request through. To get an actual denial,
// a positive budget of 1 is pre-consumed here before the handler ever sees a
// request, so the very next call is the one that trips the limit.
func newTestHandlerWithQuota(t *testing.T, dailyBudget int64) (http.Handler, *sink) {
	t.Helper()
	js, priv := newJWKS(t)
	verifier := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())
	rdb := startRedis(t)

	s := &sink{}
	offsets := &recordingOffsets{inner: enrich.NewMemoryOffsetStore(), sink: s}
	token := mintToken(t, priv)

	checker := quota.NewChecker(rdb)
	lim := limits.Quota{DailyEvents: 1, RPS: 1}
	if dailyBudget == 0 {
		// Spend the single unit of budget up front so the test's own request
		// is guaranteed to be denied, regardless of how the RPS bucket's
		// per-second window happens to line up with wall-clock time.
		if _, err := checker.Allow(context.Background(), tenant.Claims{TenantID: "t1", InstallID: "i-1"}, lim, 1, time.Now()); err != nil {
			t.Fatalf("pre-consume quota: %v", err)
		}
	}

	h := handler.NewBatch(handler.Deps{
		Verifier: verifier,
		Offsets:  offsets,
		Quota:    checker,
		LimitsFor: func(context.Context, string, uint8) (limits.Quota, error) {
			return lim, nil
		},
		Insert: func(_ context.Context, rows []clickhouse.Row) error {
			s.rows = append(s.rows, rows...)
			return nil
		},
	})
	return authWrapper(h, token), s
}

func newTestHandlerFailingInsert(t *testing.T) http.Handler {
	t.Helper()
	js, priv := newJWKS(t)
	verifier := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())
	rdb := startRedis(t)
	token := mintToken(t, priv)

	h := handler.NewBatch(handler.Deps{
		Verifier:  verifier,
		Offsets:   enrich.NewMemoryOffsetStore(),
		Quota:     quota.NewChecker(rdb),
		LimitsFor: unlimitedLimits,
		Insert: func(context.Context, []clickhouse.Row) error {
			return errors.New("clickhouse unavailable")
		},
	})
	return authWrapper(h, token)
}

// failingOffsetStore always errors, simulating an unreachable offset backend.
type failingOffsetStore struct{}

func (failingOffsetStore) GetOrSet(context.Context, enrich.SessionKey, int64) (int64, error) {
	return 0, errors.New("offset store unavailable")
}

func newTestHandlerFailingOffsets(t *testing.T) http.Handler {
	t.Helper()
	js, priv := newJWKS(t)
	verifier := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())
	rdb := startRedis(t)
	token := mintToken(t, priv)

	h := handler.NewBatch(handler.Deps{
		Verifier:  verifier,
		Offsets:   failingOffsetStore{},
		Quota:     quota.NewChecker(rdb),
		LimitsFor: unlimitedLimits,
		Insert: func(context.Context, []clickhouse.Row) error {
			return nil
		},
	})
	return authWrapper(h, token)
}

// legacyResolver returns the same (tenantID, mode) for any wk_live_ key.
type legacyResolver struct {
	tenantID string
	mode     tenant.LegacyMode
}

func (r legacyResolver) Resolve(context.Context, string) (string, tenant.LegacyMode, error) {
	return r.tenantID, r.mode, nil
}

// newTestHandlerWithLegacy wires a Legacy resolver returning ("t1", mode, nil)
// for any wk_live_ key, so tests can drive the dual-accept/cutoff cutover
// without a live tenant DB.
func newTestHandlerWithLegacy(t *testing.T, mode tenant.LegacyMode) (http.Handler, *sink) {
	t.Helper()
	js, priv := newJWKS(t)
	verifier := tenant.NewVerifier(js.URL, testIssuer, testAudience, js.Client())
	rdb := startRedis(t)

	s := &sink{}
	offsets := &recordingOffsets{inner: enrich.NewMemoryOffsetStore(), sink: s}
	token := mintToken(t, priv)

	h := handler.NewBatch(handler.Deps{
		Verifier:  verifier,
		Legacy:    legacyResolver{tenantID: "t1", mode: mode},
		Offsets:   offsets,
		Quota:     quota.NewChecker(rdb),
		LimitsFor: unlimitedLimits,
		Insert: func(_ context.Context, rows []clickhouse.Row) error {
			s.rows = append(s.rows, rows...)
			return nil
		},
	})

	// No auto-injected token: legacy tests supply their own Authorization
	// header via postWithAuth.
	return authWrapper(h, token), s
}

func batchJSON(t *testing.T, events []map[string]any) []byte {
	t.Helper()
	body := map[string]any{
		"sentAt": "1754092800000",
		"events": events,
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	return b
}

func post(t *testing.T, h http.Handler, body []byte, gz bool) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if gz {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			t.Fatalf("gzip write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("gzip close: %v", err)
		}
		reader = bytes.NewReader(buf.Bytes())
	} else {
		reader = bytes.NewReader(body)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/batch", reader)
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// postWithAuth is post() with an explicit Authorization header, for tests
// that need to present a credential other than the fixture's default JWT
// (e.g. a legacy wk_live_ write key).
func postWithAuth(t *testing.T, h http.Handler, body []byte, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/batch", bytes.NewReader(body))
	req.Header.Set("Authorization", auth)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, b []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("decode response: %v (body: %s)", err, b)
	}
}

func pad(i int) string {
	return fmt.Sprintf("%08x", i)
}

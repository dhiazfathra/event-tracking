//go:build e2e

package handler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcch "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/controlplane"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/attest"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/handler"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
)

type stack struct {
	CH    chdriver.Conn
	Token http.Handler
	Batch http.Handler
}

// startFullStack boots ClickHouse, Postgres, and Redis, migrates both schemas,
// seeds one tenant, and wires the real handlers against them.
//
// The JWKS is served from the same store the minter signs with — the whole
// point of the exchange-then-ingest test is that those two agree.
func startFullStack(t *testing.T) *stack {
	t.Helper()
	ctx := context.Background()

	chConn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, chConn, clickhouse.Migrations); err != nil {
		t.Fatalf("clickhouse migrate: %v", err)
	}

	pool := startPostgres(t)
	if err := controlplane.Migrate(ctx, pool, controlplane.Migrations); err != nil {
		t.Fatalf("postgres migrate: %v", err)
	}
	store := controlplane.New(pool)
	if err := store.EnsureSigningKey(ctx, "e2e-kid-1"); err != nil {
		t.Fatalf("signing key: %v", err)
	}
	seedTenant(t, ctx, pool) // tenant t-test, client_id pk_live_test, generous quotas

	rdb := startRealRedis(t)

	// The real GET /.well-known/jwks.json handler from cmd/main.go's own
	// wiring, not a reimplementation — this is what makes the test actually
	// exercise the shipped jwks.json code path instead of a copy of it.
	jwks := httptest.NewServer(handler.NewJWKS(store))
	t.Cleanup(jwks.Close)

	const issuer, audience = "https://issuer.e2e", "https://ingest.e2e"

	key, err := store.ActiveSigningKey(ctx)
	if err != nil {
		t.Fatalf("active key: %v", err)
	}
	minter := tenant.NewMinter(key.KID, key.Private, issuer, audience, 45*time.Minute)
	verifier := tenant.NewVerifier(jwks.URL, issuer, audience, jwks.Client())

	tokenDeps := handler.TokenDeps{
		Minter:        minter,
		Attestor:      attest.Noop{},
		Challenges:    attest.RedisChallenges{RDB: rdb, TTL: 5 * time.Minute},
		ResolveTenant: store.ResolveTenant,
		IssueInstall:  store.IssueInstall,
	}

	return &stack{
		CH:    chConn,
		Token: handler.NewToken(tokenDeps),
		Batch: handler.NewBatch(handler.Deps{
			Verifier:  verifier,
			Legacy:    legacyResolverFunc(store.ResolveLegacy),
			Offsets:   enrich.NewPostgresOffsetStore(pool),
			Quota:     quota.NewChecker(rdb),
			LimitsFor: store.LimitsFor,
			Insert: func(ctx context.Context, rows []clickhouse.Row) error {
				return clickhouse.InsertEvents(ctx, chConn, rows)
			},
		}),
	}
}

// legacyResolverFunc adapts a plain function to tenant.LegacyResolver.
type legacyResolverFunc func(ctx context.Context, key string) (string, tenant.LegacyMode, error)

func (f legacyResolverFunc) Resolve(ctx context.Context, key string) (string, tenant.LegacyMode, error) {
	return f(ctx, key)
}

// exchangeToken runs the real exchange and returns the access token.
func exchangeToken(t *testing.T, env *stack) string {
	t.Helper()
	rec := postJSON(t, env.Token, "/v1/auth/token",
		`{"clientId":"pk_live_test","platform":"android","deviceHint":"e2e-device"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("token exchange: %d %s", rec.Code, rec.Body)
	}
	var tok trackingv1.TokenResponse
	decode(t, rec.Body.Bytes(), &tok)
	return tok.AccessToken
}

func postWithToken(t *testing.T, h http.Handler, body []byte, token string) *httptest.ResponseRecorder {
	t.Helper()
	return postWithAuth(t, h, body, "Bearer "+token)
}

// startClickHouse boots a throwaway ClickHouse and returns a live connection.
func startClickHouse(t *testing.T) chdriver.Conn {
	t.Helper()
	ctx := context.Background()

	container, err := tcch.Run(ctx, "clickhouse/clickhouse-server:24.8-alpine",
		tcch.WithUsername("default"),
		tcch.WithPassword("test"),
		tcch.WithDatabase("tracking_test"),
	)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	host, err := container.ConnectionHost(ctx)
	if err != nil {
		t.Fatalf("connection host: %v", err)
	}

	conn, err := ch.Open(&ch.Options{
		Addr: []string{host},
		Auth: ch.Auth{Database: "tracking_test", Username: "default", Password: "test"},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return conn
}

// startPostgres boots a throwaway Postgres and returns a live pool.
func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("control_test"),
		tcpostgres.WithPassword("dev"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	pool, err := controlplane.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// startRealRedis boots a throwaway Redis via testcontainers. Named distinctly
// from the miniredis-backed startRedis in handler_fixtures_test.go (which
// stays in the non-e2e unit suite): both files compile together under
// `-tags e2e`, so the two fixtures cannot share one name.
func startRealRedis(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	opts, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return rdb
}

// seedTenant inserts tenant t-test, client_id pk_live_test, and a generous
// quota — the fixed identity every e2e test exchanges against.
func seedTenant(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (tenant_id, name) VALUES ('t-test', 'e2e')`); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO client_ids (client_id, tenant_id) VALUES ('pk_live_test', 't-test')`); err != nil {
		t.Fatalf("seed client_id: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO quotas (tenant_id, daily_events, rps_tier0, rps_tier1, rps_legacy)
		VALUES ('t-test', 1000000, 1000, 1000, 1000)`); err != nil {
		t.Fatalf("seed quota: %v", err)
	}
}

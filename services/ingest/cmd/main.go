// Command ingest serves POST /v1/auth/token and POST /v1/batch.
//
// The service is stateless: scale it horizontally and it costs nothing but
// pods. All buffering lives either on the client (the outbox) or in
// ClickHouse's async insert queue — never in this process.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/controlplane"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/attest"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/handler"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := clickhouse.Open(ctx, clickhouse.Config{
		Addrs:    strings.Split(env("CLICKHOUSE_ADDRS", "localhost:9000"), ","),
		Database: env("CLICKHOUSE_DB", "tracking"),
		Username: env("CLICKHOUSE_USER", "default"),
		Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	})
	if err != nil {
		log.Error("clickhouse", "err", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	pool, err := controlplane.Open(ctx, env("POSTGRES_DSN", "postgres://postgres:dev@localhost:5432/control"))
	if err != nil {
		log.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Migrations run before the service serves traffic, not from a sidecar: a
	// pod that came up against an un-migrated schema fails every request.
	if err := controlplane.Migrate(ctx, pool, controlplane.Migrations); err != nil {
		log.Error("postgres migrate", "err", err)
		os.Exit(1)
	}
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		log.Error("clickhouse migrate", "err", err)
		os.Exit(1)
	}

	store := controlplane.New(pool)

	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	defer func() { _ = rdb.Close() }()

	issuer := env("TOKEN_ISSUER", "https://issuer.local")
	audience := env("TOKEN_AUDIENCE", "https://ingest.local")

	// One key source for both minting and publication. An ephemeral key with an
	// externally-fetched JWKS means the service rejects its own tokens.
	if err := store.EnsureSigningKey(ctx, env("SIGNING_KID", "kid-1")); err != nil {
		log.Error("ensure signing key", "err", err)
		os.Exit(1)
	}
	key, err := store.ActiveSigningKey(ctx)
	if err != nil {
		log.Error("active signing key", "err", err)
		os.Exit(1)
	}

	// Signing-key rotation requires a rolling restart: the minter above holds
	// the active key for the process lifetime, while the JWKS handler reads
	// the store per request. That's fine — rotation is rare and a restart is
	// the deploy-time story already relied on for config changes.
	minter, err := tenant.NewMinter(key.KID, key.Private, issuer, audience, 45*time.Minute)
	if err != nil {
		log.Error("minter", "err", err)
		os.Exit(1)
	}

	listenAddr := env("LISTEN_ADDR", ":8080")
	_, listenPort, err := net.SplitHostPort(listenAddr)
	if err != nil {
		log.Error("listen addr", "err", err)
		os.Exit(1)
	}
	verifier := tenant.NewVerifier(
		env("JWKS_URL", "http://127.0.0.1:"+listenPort+"/.well-known/jwks.json"),
		issuer, audience, nil)

	checker := quota.NewChecker(rdb)

	tokenDeps := handler.TokenDeps{
		Minter:        minter,
		Attestor:      attest.Noop{},
		Challenges:    attest.RedisChallenges{RDB: rdb, TTL: 5 * time.Minute},
		ResolveTenant: store.ResolveTenant,
		IssueInstall:  store.IssueInstall,
		RateLimit:     checker,
	}

	mux := http.NewServeMux()
	mux.Handle("POST /v1/auth/challenge", handler.NewChallenge(tokenDeps))
	mux.Handle("POST /v1/auth/token", handler.NewToken(tokenDeps))
	mux.Handle("GET /.well-known/jwks.json", handler.NewJWKS(store))
	mux.Handle("POST /v1/batch", handler.NewBatch(handler.Deps{
		Verifier: verifier,
		Legacy:   legacyResolver{store},
		// Persistent, not in-memory: an offset that changes across a restart or
		// differs per replica gives a retry a different ts than its original,
		// which stops ReplacingMergeTree from ever collapsing the pair.
		Offsets:   enrich.NewPostgresOffsetStore(pool),
		Quota:     checker,
		LimitsFor: store.LimitsFor,
		Insert: func(ctx context.Context, rows []clickhouse.Row) error {
			return clickhouse.InsertEvents(ctx, conn, rows)
		},
		OnLegacyUse: func(tenantID, sdkVersion string) {
			log.Warn("legacy write key used", "tenant", tenantID, "sdk_version", sdkVersion)
		},
	}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Readiness is distinct from liveness: compose and Kubernetes need to know
	// the dependencies are actually reachable, not just that the process is up.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := conn.Ping(r.Context()); err != nil {
			http.Error(w, "clickhouse", http.StatusServiceUnavailable)
			return
		}
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w, "postgres", http.StatusServiceUnavailable)
			return
		}
		if err := rdb.Ping(r.Context()).Err(); err != nil {
			http.Error(w, "redis", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// ReadHeaderTimeout does not bound the body. Without ReadTimeout a
		// client can hold a connection open indefinitely, dribbling bytes —
		// cheap for the attacker, one goroutine and one FD each for us.
		//
		// 60s is generous for a <=1 MB gzipped batch even on a bad mobile
		// connection, which is the documented upload behaviour.
		ReadTimeout: 60 * time.Second,
		IdleTimeout: 120 * time.Second,
		// Go arms the write deadline once headers are read, so this must
		// cover ReadTimeout plus handler time, not just the response write.
		// 60s upload + ~30s handler budget (wait_for_async_insert=1 can add
		// ~1s, quota and offset round-trips the rest).
		WriteTimeout: 90 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("serve", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	// Longer than WriteTimeout: a slow upload already in flight must be able
	// to finish and get its response before Shutdown gives up on it.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*srv.WriteTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown", "err", err)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// legacyResolver adapts the control-plane store to tenant.LegacyResolver.
type legacyResolver struct{ store *controlplane.Store }

func (l legacyResolver) Resolve(ctx context.Context, key string) (string, tenant.LegacyMode, error) {
	return l.store.ResolveLegacy(ctx, key)
}

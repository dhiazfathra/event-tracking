package controlplane_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/dhiazfathra/event-tracking/pkg/controlplane"
)

// startPostgres boots a throwaway Postgres, migrates the control-plane schema,
// and returns a live pool. One container per test: shared state between
// control-plane tests is how you get a suite that only passes in one run order.
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

	if err := controlplane.Migrate(ctx, pool, controlplane.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

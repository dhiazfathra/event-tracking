package enrich_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/dhiazfathra/event-tracking/pkg/controlplane"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
)

// startPostgres boots a throwaway Postgres, migrates the control-plane schema
// into it, and returns a live pool. One container per test, same rationale as
// the ClickHouse and Redis fixtures elsewhere in this repo: shared state
// between tests is how you get a suite that only passes in one run order.
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

func TestPostgresOffsetStoreConcurrentGetOrSetAgreeOnOneOffset(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)
	store := enrich.NewPostgresOffsetStore(pool)
	k := enrich.SessionKey{TenantID: "t1", DeviceID: "d1", SessionID: "s1"}

	const workers = 20
	results := make([]int64, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			offset, err := store.GetOrSet(ctx, k, int64(i)+1)
			if err != nil {
				t.Errorf("GetOrSet: %v", err)
				return
			}
			results[i] = offset
		}(i)
	}
	wg.Wait()

	if results[0] == 0 {
		t.Fatal("agreed offset is 0; no worker recorded a stored offset")
	}
	for i := 1; i < workers; i++ {
		if results[i] != results[0] {
			t.Fatalf("worker %d got offset %d, want %d — concurrent first-contact upsert disagreed", i, results[i], results[0])
		}
	}
}

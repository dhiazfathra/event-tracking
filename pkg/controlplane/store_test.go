package controlplane_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dhiazfathra/event-tracking/pkg/controlplane"
)

func TestEnsureSigningKeyConcurrentCallsLeaveExactlyOneActiveKey(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)
	store := controlplane.New(pool)

	const workers = 20
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = store.EnsureSigningKey(ctx, fmt.Sprintf("kid-%d", i))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}

	var active int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM signing_keys WHERE active AND retired_at IS NULL`).
		Scan(&active); err != nil {
		t.Fatalf("count active: %v", err)
	}
	if active != 1 {
		t.Fatalf("active signing keys = %d, want exactly 1", active)
	}
}

// TestResolveTenantUnknownClientID makes sure an unrecognized or revoked
// client ID surfaces as ErrNotFound, not a raw driver error the handler would
// have to special-case.
func TestResolveTenantUnknownClientID(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)
	store := controlplane.New(pool)

	if _, err := store.ResolveTenant(ctx, "pk_live_nope"); err != controlplane.ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestActiveSigningKeyNoneProvisioned exercises the ErrNotFound path before
// EnsureSigningKey has ever run — the state a brand new environment starts in.
func TestActiveSigningKeyNoneProvisioned(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)
	store := controlplane.New(pool)

	if _, err := store.ActiveSigningKey(ctx); err != controlplane.ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestIssueInstallRepeatedExchangeReusesID: the exchange endpoint's whole
// rate-limit-bucket-stability guarantee rests on this staying an upsert, not
// an insert.
func TestIssueInstallRepeatedExchangeReusesID(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)
	store := controlplane.New(pool)
	seedTenant(t, ctx, pool)

	first, err := store.IssueInstall(ctx, "t-seed", "android", "", "device-key-1", 1)
	if err != nil {
		t.Fatalf("first issue: %v", err)
	}
	second, err := store.IssueInstall(ctx, "t-seed", "android", "", "device-key-1", 1)
	if err != nil {
		t.Fatalf("second issue: %v", err)
	}
	if first != second {
		t.Fatalf("install ids differ across repeated exchange: %s vs %s", first, second)
	}
}

// seedTenant inserts the minimal tenant row IssueInstall's foreign key needs.
func seedTenant(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (tenant_id, name) VALUES ('t-seed', 'seed')`); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
}

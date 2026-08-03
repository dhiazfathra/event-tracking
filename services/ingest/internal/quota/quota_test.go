package quota_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/pkg/limits"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
)

func TestDailyQuotaExhaustionReturns429WithRetryAfter(t *testing.T) {
	ctx := context.Background()
	c := quota.NewChecker(startRedis(t))
	cl := tenant.Claims{TenantID: "t1", InstallID: "i-1"}
	lim := limits.Quota{DailyEvents: 100, RPS: 1000}
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	d, err := c.Allow(ctx, cl, lim, 100, now)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !d.Allowed {
		t.Fatalf("first 100 events denied: %+v", d)
	}

	d, err = c.Allow(ctx, cl, lim, 1, now)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if d.Allowed {
		t.Errorf("101st event allowed, want denied")
	}
	if d.RetryAfter <= 0 {
		t.Errorf("RetryAfter = %v, want > 0 so the client's backoff has a target", d.RetryAfter)
	}
	if d.Reason != "daily_quota" {
		t.Errorf("Reason = %q, want daily_quota", d.Reason)
	}
}

// The primary bucket is install_id. Two installs under the same tenant must not
// share a per-second limit — and one install must not be able to spend the
// whole tenant budget in a second.
func TestRateLimitKeysOnInstallFirst(t *testing.T) {
	ctx := context.Background()
	c := quota.NewChecker(startRedis(t))
	lim := limits.Quota{DailyEvents: 1_000_000, RPS: 10}
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	a := tenant.Claims{TenantID: "t1", InstallID: "install-A"}
	b := tenant.Claims{TenantID: "t1", InstallID: "install-B"}

	if d, _ := c.Allow(ctx, a, lim, 10, now); !d.Allowed {
		t.Fatalf("install A first 10 denied")
	}
	if d, _ := c.Allow(ctx, a, lim, 1, now); d.Allowed {
		t.Errorf("install A 11th allowed, want rate limited")
	}
	// Different install, same tenant, same second: unaffected.
	if d, _ := c.Allow(ctx, b, lim, 10, now); !d.Allowed {
		t.Errorf("install B denied by install A's limit — rate limit is keyed wrong")
	}
}

func TestRateLimitWindowRollsOver(t *testing.T) {
	ctx := context.Background()
	c := quota.NewChecker(startRedis(t))
	cl := tenant.Claims{TenantID: "t1", InstallID: "i-1"}
	lim := limits.Quota{DailyEvents: 1_000_000, RPS: 5}
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	_, _ = c.Allow(ctx, cl, lim, 5, now)
	if d, _ := c.Allow(ctx, cl, lim, 1, now); d.Allowed {
		t.Fatalf("6th in the same second allowed")
	}
	if d, _ := c.Allow(ctx, cl, lim, 1, now.Add(time.Second)); !d.Allowed {
		t.Errorf("next second denied, want allowed")
	}
}

// AllowClient guards POST /v1/auth/token, which has no install_id yet to key
// Allow's tenant-scoped buckets off — it must deny once the per-key ceiling is
// hit, and must not share Allow's daily-quota keyspace (spending it here would
// make token minting eat into the tenant's ingest budget).
func TestAllowClientDeniesAfterLimitAndDoesNotShareIngestQuota(t *testing.T) {
	ctx := context.Background()
	c := quota.NewChecker(startRedis(t))
	key := "rl:token:pk_live_abc"

	for i := 0; i < 3; i++ {
		ok, err := c.AllowClient(ctx, key, 3, time.Minute)
		if err != nil {
			t.Fatalf("allow client: %v", err)
		}
		if !ok {
			t.Fatalf("call %d denied, want allowed within the limit of 3", i)
		}
	}

	ok, err := c.AllowClient(ctx, key, 3, time.Minute)
	if err != nil {
		t.Fatalf("allow client: %v", err)
	}
	if ok {
		t.Error("4th call allowed, want denied")
	}

	// The tenant's ingest daily quota must be untouched by AllowClient calls.
	cl := tenant.Claims{TenantID: "t1", InstallID: "i-1"}
	lim := limits.Quota{DailyEvents: 1, RPS: 1000}
	d, err := c.Allow(ctx, cl, lim, 1, time.Now())
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !d.Allowed {
		t.Error("ingest quota already exhausted — AllowClient leaked into Allow's keyspace")
	}
}

// Check-and-consume must be one atomic operation. With separate increment,
// compare, and rollback steps, a denied request's DECRBY can land after another
// pod's increment and quietly refund budget that was legitimately spent.
func TestConcurrentAllowNeverOverspendsOrUndercounts(t *testing.T) {
	ctx := context.Background()
	rdb := startRedis(t)
	c := quota.NewChecker(rdb)
	cl := tenant.Claims{TenantID: "t1", InstallID: "i-1"}
	lim := limits.Quota{DailyEvents: 50, RPS: 1_000_000}
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	const workers = 40
	var granted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := c.Allow(ctx, cl, lim, 2, now)
			if err == nil && d.Allowed {
				granted.Add(2)
			}
		}()
	}
	wg.Wait()

	if got := granted.Load(); got != 50 {
		t.Errorf("granted %d events against a budget of 50", got)
	}

	// And the stored counter must agree with what was granted — a blind
	// rollback racing another increment shows up here as a refund.
	stored, err := rdb.Get(ctx, "q:{t1}:2026-08-02").Int64()
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if stored != granted.Load() {
		t.Errorf("counter = %d but %d granted — rollback raced an increment", stored, granted.Load())
	}
}

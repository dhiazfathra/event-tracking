package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
)

func TestInsertEventsIsDurableOnReturn(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	rows := []clickhouse.Row{{
		TenantID:   "t1",
		EventID:    "0191f4a2-1c3d-7000-8000-00000000000a",
		Name:       "checkout",
		TS:         now,
		TSClient:   now,
		TSReceived: now,
		UserID:     "u1",
		Seq:        1,
		TrustTier:  0,
		InstallID:  "i-1",
		Props:      `{"amount":19.99}`,
	}}

	if err := clickhouse.InsertEvents(ctx, conn, rows); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// wait_for_async_insert=1 means the data is queryable the moment
	// InsertEvents returns. No sleep, no retry loop. If this test ever needs a
	// sleep to pass, the settings are wrong and the 200 is a lie.
	var n uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM events WHERE tenant_id = 't1'`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("count = %d, want 1 immediately after insert returns", n)
	}
}

func TestInsertEventsEmptyIsNoop(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := clickhouse.InsertEvents(ctx, conn, nil); err != nil {
		t.Errorf("empty insert returned %v, want nil", err)
	}
}

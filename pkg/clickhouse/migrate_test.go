package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
)

func TestMigrateCreatesEventsTable(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t) // helper added in Step 3

	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var engine, sortingKey string
	row := conn.QueryRow(ctx,
		`SELECT engine_full, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'events'`)
	if err := row.Scan(&engine, &sortingKey); err != nil {
		t.Fatalf("describe events: %v", err)
	}

	if want := "ReplacingMergeTree(ts_received)"; !contains(engine, want) {
		t.Errorf("engine = %q, want it to contain %q", engine, want)
	}
	if want := "tenant_id, name, ts, event_id"; sortingKey != want {
		t.Errorf("sorting_key = %q, want %q", sortingKey, want)
	}
}

// The sort key must be unique or ReplacingMergeTree collapses rows that are not
// actually duplicates. This test proves two distinct events with the same
// (tenant, name, ts) both survive a forced merge, and that a true duplicate
// (same event_id) collapses to one.
func TestReplacingMergeTreeCollapsesOnlyTrueDuplicates(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)
	if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ts := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	insert := func(eventID string, received time.Time) {
		t.Helper()
		err := conn.Exec(ctx,
			`INSERT INTO events (tenant_id, event_id, name, ts, ts_client, ts_received, trust_tier, install_id, props)
			 VALUES (?, ?, ?, ?, ?, ?, 0, 'i-1', '{}')`,
			"t1", eventID, "checkout", ts, ts, received)
		if err != nil {
			t.Fatalf("insert %s: %v", eventID, err)
		}
	}

	a := "0191f4a2-1c3d-7000-8000-00000000000a"
	b := "0191f4a2-1c3d-7000-8000-00000000000b"
	insert(a, ts)
	insert(a, ts.Add(time.Second)) // true duplicate, later version wins
	insert(b, ts)                  // distinct event, same tenant/name/ts

	if err := conn.Exec(ctx, `OPTIMIZE TABLE events FINAL`); err != nil {
		t.Fatalf("optimize: %v", err)
	}

	var n uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM events`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("row count after merge = %d, want 2 (one collapsed duplicate, two distinct events)", n)
	}
}

// Each migration file must run exactly once, and be recorded as such. Without
// the ledger this only works while every migration is coincidentally
// idempotent.
func TestMigrateAppliesEachFileOnce(t *testing.T) {
	ctx := context.Background()
	conn := startClickHouse(t)

	for i := 0; i < 3; i++ {
		if err := clickhouse.Migrate(ctx, conn, clickhouse.Migrations); err != nil {
			t.Fatalf("migrate run %d: %v", i, err)
		}
	}

	var n uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM (SELECT DISTINCT name FROM schema_migrations)`).Scan(&n); err != nil {
		t.Fatalf("count migrations: %v", err)
	}

	var files uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM schema_migrations`).Scan(&files); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if files != n {
		t.Errorf("schema_migrations has %d rows for %d distinct migrations — a file ran twice", files, n)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

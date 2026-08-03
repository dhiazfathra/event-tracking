package enrich_test

import (
	"testing"
	"time"

	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
)

func TestCorrectAppliesOffset(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	tsClient := received.Add(-5 * time.Minute).UnixMilli() // device 5 min slow

	got, clamped := enrich.Correct(tsClient, (5 * time.Minute).Milliseconds(), received)
	if clamped {
		t.Errorf("clamped = true, want false")
	}
	if !got.Equal(received) {
		t.Errorf("ts = %v, want %v", got, received)
	}
}

// A single device with a broken clock must not create partitions years out.
func TestCorrectClampsFarFuture(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	tsClient := received.Add(400 * 24 * time.Hour).UnixMilli()

	got, clamped := enrich.Correct(tsClient, 0, received)
	if !clamped {
		t.Fatalf("clamped = false, want true")
	}
	if !got.Equal(received) {
		t.Errorf("ts = %v, want clamped to received_at %v", got, received)
	}
}

func TestCorrectClampsFarPast(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	tsClient := received.Add(-90 * 24 * time.Hour).UnixMilli()

	got, clamped := enrich.Correct(tsClient, 0, received)
	if !clamped {
		t.Fatalf("clamped = false, want true")
	}
	if !got.Equal(received) {
		t.Errorf("ts = %v, want clamped to received_at %v", got, received)
	}
}

func TestCorrectBoundariesAreInclusive(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	for _, c := range []struct {
		name  string
		delta time.Duration
	}{
		{"exactly 24h future", enrich.MaxFutureSkew},
		{"exactly 30d past", -enrich.MaxPastSkew},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, clamped := enrich.Correct(received.Add(c.delta).UnixMilli(), 0, received)
			if clamped {
				t.Errorf("clamped at the boundary; boundary must be inclusive")
			}
		})
	}
}

// The offset must be stable per session. If a retry recomputed it, the row
// would move under the (tenant, name, ts, event_id) sort key — and near
// midnight UTC into a different partition — so ReplacingMergeTree would never
// collapse the duplicate.
func TestOffsetIsStableAcrossRetries(t *testing.T) {
	store := enrich.NewMemoryOffsetStore()
	key := enrich.SessionKey{TenantID: "t1", DeviceID: "d1", SessionID: "s1"}

	first, err := store.GetOrSet(t.Context(), key, 5000)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// A retry arriving later computes a different candidate offset.
	second, err := store.GetOrSet(t.Context(), key, 9999)
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if first != second {
		t.Errorf("offset changed across retries: %d then %d", first, second)
	}
	if first != 5000 {
		t.Errorf("offset = %d, want the first-contact value 5000", first)
	}
}

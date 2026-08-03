package enrich_test

import (
	"encoding/json"
	"testing"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
)

func TestRowTakesTenantFromClaimsOnly(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	e := &trackingv1.Event{
		EventId:  "0191f4a2-1c3d-7000-8000-000000000001",
		Name:     "checkout",
		TsClient: received.UnixMilli(),
		DeviceId: "d1",
	}
	claims := tenant.Claims{TenantID: "tenant-A", InstallID: "i-1", TrustTier: 1}

	row, _ := enrich.Row(e, claims, 0, received)

	if row.TenantID != "tenant-A" {
		t.Errorf("tenant_id = %q, want tenant-A (from the verified claim)", row.TenantID)
	}
	if row.InstallID != "i-1" {
		t.Errorf("install_id = %q, want i-1 (server-issued)", row.InstallID)
	}
	if row.TrustTier != 1 {
		t.Errorf("trust_tier = %d, want 1", row.TrustTier)
	}
	if !row.TSReceived.Equal(received) {
		t.Errorf("ts_received = %v, want %v", row.TSReceived, received)
	}
	// The server stamps ts/ts_received/trust_tier/install_id. It never mints an
	// event ID: a per-attempt server ID would make every retry a distinct row
	// and break read-time dedup.
	if row.EventID != e.EventId {
		t.Errorf("event_id = %q, want the client's %q", row.EventID, e.EventId)
	}
}

func TestRowSerialisesPropsAsJSONObject(t *testing.T) {
	received := time.Now().UTC()
	e := &trackingv1.Event{
		EventId:  "0191f4a2-1c3d-7000-8000-000000000001",
		Name:     "checkout",
		TsClient: received.UnixMilli(),
		DeviceId: "d1",
		Props: map[string]*trackingv1.Value{
			"amount":   {Kind: &trackingv1.Value_NumberValue{NumberValue: 19.99}},
			"currency": {Kind: &trackingv1.Value_StringValue{StringValue: "IDR"}},
			"first":    {Kind: &trackingv1.Value_BoolValue{BoolValue: true}},
		},
	}

	row, _ := enrich.Row(e, tenant.Claims{TenantID: "t1", InstallID: "i-1"}, 0, received)

	var got map[string]any
	if err := json.Unmarshal([]byte(row.Props), &got); err != nil {
		t.Fatalf("props is not valid JSON: %v (%q)", err, row.Props)
	}
	// The union wrapper must be flattened — ClickHouse should see
	// {"amount":19.99}, not {"amount":{"numberValue":19.99}}.
	if got["amount"] != 19.99 {
		t.Errorf("props.amount = %v, want 19.99 (flattened, not wrapped)", got["amount"])
	}
	if got["currency"] != "IDR" {
		t.Errorf("props.currency = %v, want IDR", got["currency"])
	}
	if got["first"] != true {
		t.Errorf("props.first = %v, want true", got["first"])
	}
}

func TestRowEmptyPropsIsEmptyObject(t *testing.T) {
	received := time.Now().UTC()
	e := &trackingv1.Event{EventId: "0191f4a2-1c3d-7000-8000-000000000001", Name: "n", DeviceId: "d1", TsClient: received.UnixMilli()}
	row, _ := enrich.Row(e, tenant.Claims{TenantID: "t1", InstallID: "i-1"}, 0, received)
	if row.Props != "{}" {
		t.Errorf("props = %q, want {}", row.Props)
	}
}

func TestRowUsesSkewCorrectedTimestamp(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	sentMS := received.Add(-5 * time.Minute).UnixMilli()
	e := &trackingv1.Event{EventId: "id-1", Name: "n", DeviceId: "d1", TsClient: sentMS}

	row, clamped := enrich.Row(e, tenant.Claims{TenantID: "t1", InstallID: "i-1"}, 60_000, received)

	if clamped {
		t.Fatalf("clamped = true, want false for a small in-range offset")
	}
	want := time.UnixMilli(sentMS + 60_000).UTC()
	if !row.TS.Equal(want) {
		t.Errorf("ts = %v, want %v (ts_client + offset)", row.TS, want)
	}
	if !row.TSClient.Equal(time.UnixMilli(sentMS).UTC()) {
		t.Errorf("ts_client = %v, want raw device time unmodified", row.TSClient)
	}
}

func TestRowClampsFarFutureSkew(t *testing.T) {
	received := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	// ts_client + offset lands 48h in the future: beyond the 24h clamp.
	sentMS := received.Add(48 * time.Hour).UnixMilli()
	e := &trackingv1.Event{EventId: "id-2", Name: "n", DeviceId: "d1", TsClient: sentMS}

	row, clamped := enrich.Row(e, tenant.Claims{TenantID: "t1", InstallID: "i-1"}, 0, received)

	if !clamped {
		t.Fatalf("clamped = false, want true for a 48h-future timestamp")
	}
	if !row.TS.Equal(received) {
		t.Errorf("ts = %v, want received_at %v when clamped", row.TS, received)
	}
}

func TestRowIgnoresContextWhenAbsent(t *testing.T) {
	received := time.Now().UTC()
	e := &trackingv1.Event{EventId: "id-3", Name: "n", DeviceId: "d1", TsClient: received.UnixMilli()}
	row, _ := enrich.Row(e, tenant.Claims{TenantID: "t1", InstallID: "i-1"}, 0, received)
	if row.AppVersion != "" || row.OS != "" {
		t.Errorf("expected empty context fields when Context is nil, got AppVersion=%q OS=%q", row.AppVersion, row.OS)
	}
}

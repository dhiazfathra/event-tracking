package handler_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/limits"
)

// One malformed event must not fail the batch. This is the single most
// important behaviour in the service: a batch-level 4xx on a poison event
// wedges the client's outbox forever.
func TestPartialSuccess(t *testing.T) {
	h, sink := newTestHandler(t)

	body := batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "ok", "deviceId": "d1", "tsClient": "1754092800000"},
		{"eventId": "not-a-uuid", "name": "bad", "deviceId": "d1", "tsClient": "1754092800000"},
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000003", "name": "ok2", "deviceId": "d1", "tsClient": "1754092800000"},
	})

	rec := post(t, h, body, false)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (partial success is a 200)", rec.Code)
	}

	var resp trackingv1.BatchResponse
	decode(t, rec.Body.Bytes(), &resp)

	if len(resp.Accepted) != 2 {
		t.Errorf("accepted = %v, want 2", resp.Accepted)
	}
	if len(resp.Rejected) != 1 || resp.Rejected[0].Code != "BAD_EVENT_ID" {
		t.Errorf("rejected = %+v, want one BAD_EVENT_ID", resp.Rejected)
	}
	if got := len(sink.rows); got != 2 {
		t.Errorf("inserted %d rows, want 2 — rejected events must not be stored", got)
	}
}

func TestGzipBodyIsAccepted(t *testing.T) {
	h, sink := newTestHandler(t)
	body := batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "ok", "deviceId": "d1", "tsClient": "1754092800000"},
	})

	rec := post(t, h, body, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(sink.rows) != 1 {
		t.Errorf("inserted %d rows, want 1", len(sink.rows))
	}
}

func TestOverSizedBatchReturns413(t *testing.T) {
	h, _ := newTestHandler(t)
	events := make([]map[string]any, 501)
	for i := range events {
		events[i] = map[string]any{
			"eventId": "0191f4a2-1c3d-7000-8000-00000000" + pad(i), "name": "n", "deviceId": "d1", "tsClient": "1754092800000",
		}
	}
	rec := post(t, h, batchJSON(t, events), false)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

func TestMalformedEnvelopeReturns400(t *testing.T) {
	h, _ := newTestHandler(t)
	rec := post(t, h, []byte(`{"events": "not an array"}`), false)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBadTokenReturns401(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/batch", strings.NewReader(string(batchJSON(t, nil))))
	req.Header.Set("Authorization", "Bearer garbage")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestQuotaExhaustionReturns429WithRetryAfter(t *testing.T) {
	h, _ := newTestHandlerWithQuota(t, 0) // zero budget
	rec := post(t, h, batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "n", "deviceId": "d1", "tsClient": "1754092800000"},
	}), false)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header; the client's backoff keys off it")
	}
}

// A ClickHouse failure must surface as 5xx so the client keeps the events.
// Returning 200 here would delete them from the outbox with nothing stored.
func TestInsertFailureReturns503AndAcceptsNothing(t *testing.T) {
	h := newTestHandlerFailingInsert(t)
	rec := post(t, h, batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "n", "deviceId": "d1", "tsClient": "1754092800000"},
	}), false)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "accepted") && strings.Contains(rec.Body.String(), "0191f4a2") {
		t.Error("response acknowledged an event that was never stored")
	}
}

// A tenant_id in the body is rejected as an unknown field, never quietly
// dropped and never preferred over the claim. Silently accepting it would let a
// client believe it had set something it had not.
func TestBodyTenantIDIsRejected(t *testing.T) {
	h, sink := newTestHandler(t) // claims tenant is "t1"
	body := []byte(`{"sentAt":"1754092800000","events":[{"eventId":"0191f4a2-1c3d-7000-8000-000000000001","name":"n","deviceId":"d1","tsClient":"1754092800000","tenantId":"attacker-tenant"}]}`)

	rec := post(t, h, body, false)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an unknown body field", rec.Code)
	}
	if len(sink.rows) != 0 {
		t.Errorf("stored %d rows for a rejected batch, want 0", len(sink.rows))
	}
}

// An oversized *uncompressed* body must be a 413 like every other
// too-large batch. MaxBytesReader surfaces its own error type, which is easy to
// let fall through to the generic 400 — and a 400 tells the client to drop the
// batch instead of halving it.
func TestOversizedUncompressedBodyReturns413(t *testing.T) {
	h, _ := newTestHandler(t)

	// One event with a huge string property, sent without gzip.
	big := strings.Repeat("x", limits.MaxBatchBytes+1024)
	body := []byte(`{"sentAt":"1754092800000","events":[{"eventId":"0191f4a2-1c3d-7000-8000-000000000001","name":"n","deviceId":"d1","tsClient":"1754092800000","props":{"blob":{"stringValue":"` + big + `"}}}]}`)

	rec := post(t, h, body, false)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

// Every event carries its own session. One offset applied across the batch
// gives events from a second device or session the wrong corrected timestamp.
func TestOffsetIsResolvedPerSessionNotPerBatch(t *testing.T) {
	h, sink := newTestHandler(t)

	body := batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "n", "deviceId": "d1", "sessionId": "s1", "tsClient": "1754092800000"},
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000002", "name": "n", "deviceId": "d2", "sessionId": "s2", "tsClient": "1754092800000"},
	})

	if rec := post(t, h, body, false); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(sink.offsetKeys) != 2 {
		t.Errorf("resolved %d session offsets, want one per distinct (device, session): %v",
			len(sink.offsetKeys), sink.offsetKeys)
	}
}

// An offset the store cannot resolve must not be silently replaced with zero:
// a replay would then land at a different ts, move under the sort key, and stop
// deduplicating. Fail the batch instead — the client still holds the events.
func TestOffsetStoreFailureReturns503(t *testing.T) {
	h := newTestHandlerFailingOffsets(t)
	rec := post(t, h, batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-000000000001", "name": "n", "deviceId": "d1", "sessionId": "s1", "tsClient": "1754092800000"},
	}), false)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

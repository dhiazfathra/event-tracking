//go:build e2e

package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
)

// testTSClient is a fixed offset from "now" rather than a hardcoded epoch
// constant, so this test doesn't rot into a stale, unrelated failure (the
// skew clamp) as real calendar time moves past a hardcoded date.
func testTSClient() string {
	return fmt.Sprintf("%d", time.Now().Add(-time.Hour).UnixMilli())
}

// Exchange a token, post a batch, read it back out of a real ClickHouse.
// A green unit suite can still ship a service that has never written a row.
func TestExchangeThenIngestThenQuery(t *testing.T) {
	ctx := context.Background()
	env := startFullStack(t) // ClickHouse + Postgres + Redis + wired handlers

	tokenRec := postJSON(t, env.Token, "/v1/auth/token",
		`{"clientId":"pk_live_test","platform":"android","deviceHint":"e2e-device"}`)
	if tokenRec.Code != http.StatusOK {
		t.Fatalf("token exchange: %d %s", tokenRec.Code, tokenRec.Body)
	}
	var tok trackingv1.TokenResponse
	decode(t, tokenRec.Body.Bytes(), &tok)
	if tok.TrustTier != 1 {
		t.Errorf("trust_tier = %d, want 1 (Noop attestor)", tok.TrustTier)
	}

	body := batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-0000000000e1", "name": "checkout", "deviceId": "d1", "sessionId": "s1",
			"userId": "u1", "tsClient": testTSClient(),
			"props": map[string]any{"amount": map[string]any{"numberValue": 19.99}}},
	})
	rec := postWithToken(t, env.Batch, body, tok.AccessToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("batch: %d %s", rec.Code, rec.Body)
	}

	var n uint64
	err := env.CH.QueryRow(ctx,
		`SELECT count() FROM events WHERE tenant_id = ? AND event_id = ?`,
		"t-test", "0191f4a2-1c3d-7000-8000-0000000000e1").Scan(&n)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Errorf("stored rows = %d, want 1", n)
	}

	// Trust tier must be persisted on the row, not just in the token.
	var tier uint8
	if err := env.CH.QueryRow(ctx,
		`SELECT trust_tier FROM events WHERE event_id = ?`,
		"0191f4a2-1c3d-7000-8000-0000000000e1").Scan(&tier); err != nil {
		t.Fatalf("read tier: %v", err)
	}
	if tier != 1 {
		t.Errorf("stored trust_tier = %d, want 1", tier)
	}
}

// Replaying the identical batch must be a no-op from the client's perspective:
// the same IDs come back accepted, and after a merge there is still one row.
func TestReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	env := startFullStack(t)
	token := exchangeToken(t, env)

	body := batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-0000000000e2", "name": "checkout", "deviceId": "d1", "sessionId": "s1",
			"tsClient": testTSClient()},
	})

	for i := 0; i < 3; i++ {
		rec := postWithToken(t, env.Batch, body, token)
		if rec.Code != http.StatusOK {
			t.Fatalf("attempt %d: %d %s", i, rec.Code, rec.Body)
		}
		var resp trackingv1.BatchResponse
		decode(t, rec.Body.Bytes(), &resp)
		if len(resp.Accepted) != 1 {
			t.Fatalf("attempt %d accepted = %v, want 1", i, resp.Accepted)
		}
	}

	if err := env.CH.Exec(ctx, `OPTIMIZE TABLE events FINAL`); err != nil {
		t.Fatalf("optimize: %v", err)
	}
	var n uint64
	if err := env.CH.QueryRow(ctx, `SELECT count() FROM events WHERE event_id = ?`,
		"0191f4a2-1c3d-7000-8000-0000000000e2").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("rows after merge = %d, want 1 — the ts must be stable across replays", n)
	}
}

// The exchange and the batch endpoint must agree on one key source. An
// ephemeral in-process signing key with an externally-fetched JWKS means every
// token the service issues is rejected by the service itself — and no unit test
// catches it, because each side passes in isolation.
func TestExchangedTokenVerifiesAtBatch(t *testing.T) {
	env := startFullStack(t)

	token := exchangeToken(t, env)
	rec := postWithToken(t, env.Batch, batchJSON(t, []map[string]any{
		{"eventId": "0191f4a2-1c3d-7000-8000-0000000000e3", "name": "checkout",
			"deviceId": "d1", "sessionId": "s1", "tsClient": testTSClient()},
	}), token)

	if rec.Code == http.StatusUnauthorized {
		t.Fatal("a freshly exchanged token was rejected by /v1/batch — the minter and the JWKS disagree")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("batch: %d %s", rec.Code, rec.Body)
	}
}

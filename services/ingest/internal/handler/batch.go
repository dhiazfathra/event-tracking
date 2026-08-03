// Package handler holds the HTTP surface. The status codes it returns are part
// of the public contract — the SDK's entire retry logic keys off them, and the
// SDK cannot be force-upgraded. Changing one is a breaking change.
package handler

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/limits"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/validate"
)

// Deps wires everything the handler needs.
type Deps struct {
	Verifier *tenant.Verifier

	// Legacy resolves pre-token wk_live_ write keys during the cutover. Without
	// it every legacy credential goes to JWT parsing and fails, so the staged
	// deprecation never actually runs.
	Legacy tenant.LegacyResolver

	Offsets   enrich.OffsetStore
	Quota     *quota.Checker
	LimitsFor func(ctx context.Context, tenantID string, tier uint8) (limits.Quota, error)
	Insert    func(ctx context.Context, rows []clickhouse.Row) error
	Now       func() time.Time

	// OnLegacyUse counts legacy credential usage per tenant and SDK version.
	// That count is what tells you when a cutoff is safe.
	OnLegacyUse func(tenantID, sdkVersion string)
}

func NewBatch(d Deps) http.Handler {
	if d.Now == nil {
		d.Now = func() time.Time { return time.Now().UTC() }
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAt := d.Now()

		claims, isLegacy, err := d.Verifier.VerifyOrLegacy(
			r.Context(), r.Header.Get("Authorization"), receivedAt, d.Legacy)
		if err != nil {
			// 401 tells the client to re-exchange for a fresh token and retry
			// once. Any other code here would make an expired token look like
			// a permanent failure and stop the device syncing. A cutoff legacy
			// key lands here too, which is the intended end state.
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}

		body, err := readBody(r)
		if err != nil {
			if errors.Is(err, errTooLarge) {
				httpError(w, http.StatusRequestEntityTooLarge, "batch too large")
				return
			}
			httpError(w, http.StatusBadRequest, "unreadable body")
			return
		}

		var req trackingv1.BatchRequest
		// Strict decoding. DiscardUnknown would accept a body `tenantId` and
		// answer 200, telling the client it had set something it had not — and
		// the contract says a tenant field in the body is rejected as unknown.
		//
		// The cost is that a newer SDK sending a field this build does not know
		// gets a 400. That is survivable precisely because the envelope is
		// governed by `buf breaking` in CI: new fields ship to servers before
		// they ship to clients, never the other way round.
		if err := protojson.Unmarshal(body, &req); err != nil {
			httpError(w, http.StatusBadRequest, "malformed batch envelope")
			return
		}
		if len(req.Events) > limits.MaxEventsPerBatch {
			httpError(w, http.StatusRequestEntityTooLarge, "too many events")
			return
		}

		if isLegacy && d.OnLegacyUse != nil {
			var sdkVersion string
			if len(req.Events) > 0 {
				sdkVersion = req.Events[0].GetContext().GetSdkVersion()
			}
			d.OnLegacyUse(claims.TenantID, sdkVersion)
		}

		resp := &trackingv1.BatchResponse{ReceivedAt: receivedAt.UnixMilli()}

		// Validate before quota: a malformed event never reaches storage, so it
		// must never spend budget either. Otherwise a client bug that spams
		// garbage events burns real quota and 429s the tenant's valid traffic.
		valid := make([]*trackingv1.Event, 0, len(req.Events))
		for _, e := range req.Events {
			if rej := validate.Event(e); rej != nil {
				resp.Rejected = append(resp.Rejected, rej)
				continue
			}
			valid = append(valid, e)
		}

		lim, err := d.LimitsFor(r.Context(), claims.TenantID, claims.TrustTier)
		if err != nil {
			httpError(w, http.StatusServiceUnavailable, "limits unavailable")
			return
		}
		if isLegacy {
			// Below tier 1, deliberately. Deprecation pressure that the SDK
			// already knows how to absorb, since it backs off on 429.
			lim.RPS = lim.LegacyRPS
		}

		// Charge at least 1 unit even when every event failed validation.
		// len(valid)==0 would otherwise read as "nothing to admit" and the Lua
		// script never touches the counters — letting an all-garbage batch burn
		// full parse+validate CPU at zero rate-limit cost, unbounded.
		chargeUnits := len(valid)
		if chargeUnits == 0 {
			chargeUnits = 1
		}
		dec, err := d.Quota.Allow(r.Context(), claims, lim, chargeUnits, receivedAt)
		if err != nil {
			httpError(w, http.StatusServiceUnavailable, "quota unavailable")
			return
		}
		if !dec.Allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(dec.RetryAfter.Seconds())+1))
			httpError(w, http.StatusTooManyRequests, dec.Reason)
			return
		}

		rows := make([]clickhouse.Row, 0, len(valid))
		accepted := make([]string, 0, len(valid))

		// Offsets are memoised per (device, session) within the batch, so a
		// 500-event batch from one session is still one store round-trip.
		offsets := map[enrich.SessionKey]int64{}

		for _, e := range valid {
			key := enrich.SessionKey{
				TenantID:  claims.TenantID,
				DeviceID:  e.GetDeviceId(),
				SessionID: e.GetSessionId(),
			}
			offset, ok := offsets[key]
			if !ok {
				offset, err = d.Offsets.GetOrSet(
					r.Context(), key, enrich.CandidateOffset(req.GetSentAt(), receivedAt))
				if err != nil {
					// Falling back to a zero offset would give this delivery a
					// different ts than its replay — moving the row under the
					// sort key so ReplacingMergeTree never collapses the pair.
					// A 503 keeps the events on the client instead.
					httpError(w, http.StatusServiceUnavailable, "offset store unavailable")
					return
				}
				offsets[key] = offset
			}

			row, _ := enrich.Row(e, claims, offset, receivedAt)
			rows = append(rows, row)
			accepted = append(accepted, e.GetEventId())
		}

		if err := d.Insert(r.Context(), rows); err != nil {
			// The client still holds these events. 503 keeps them there.
			httpError(w, http.StatusServiceUnavailable, "storage unavailable")
			return
		}

		// Only set accepted after the insert returns. wait_for_async_insert=1
		// means this ack is a durability claim, and the client deletes its copy
		// on the strength of it.
		resp.Accepted = accepted

		w.Header().Set("Content-Type", "application/json")
		out, err := protojson.Marshal(resp)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "encode")
			return
		}
		_, _ = w.Write(out)
	})
}

var errTooLarge = errors.New("body too large")

// readBody decompresses and enforces the decompressed size cap. The cap is on
// the decompressed bytes on purpose: a gzip bomb is small on the wire.
func readBody(r *http.Request) ([]byte, error) {
	var src io.Reader = http.MaxBytesReader(nil, r.Body, limits.MaxBatchBytes)

	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(src)
		if err != nil {
			return nil, err
		}
		defer func() { _ = zr.Close() }()
		// +1 so an exactly-at-limit body is distinguishable from an over-limit
		// one after the read.
		src = io.LimitReader(zr, limits.MaxBatchBytes+1)
	}

	b, err := io.ReadAll(src)
	if err != nil {
		// MaxBytesReader reports its own error type rather than returning a
		// short read. Left unmapped it falls through to the generic 400, which
		// tells the client to *drop* the batch instead of halving it — turning
		// a recoverable oversize into silent data loss.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, errTooLarge
		}
		return nil, err
	}
	if len(b) > limits.MaxBatchBytes {
		return nil, errTooLarge
	}
	return b, nil
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

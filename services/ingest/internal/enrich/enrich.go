package enrich

import (
	"encoding/json"
	"time"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/clickhouse"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

// Row builds the storage row from a validated envelope plus verified claims.
//
// Every trust-bearing field comes from claims, never from the envelope. The
// envelope has no tenant field at all (proto §3.1); if one ever appears in a
// request body it is ignored here, not preferred over the claim — otherwise a
// valid token for tenant A could write rows labelled tenant B.
func Row(e *trackingv1.Event, c tenant.Claims, offsetMS int64, receivedAt time.Time) (clickhouse.Row, bool) {
	ts, clamped := Correct(e.GetTsClient(), offsetMS, receivedAt)
	ctx := e.GetContext()

	return clickhouse.Row{
		TenantID:   c.TenantID,
		EventID:    e.GetEventId(),
		Name:       e.GetName(),
		TS:         ts,
		TSClient:   time.UnixMilli(e.GetTsClient()).UTC(),
		TSReceived: receivedAt,

		UserID:      e.GetUserId(),
		AnonymousID: e.GetAnonymousId(),
		DeviceID:    e.GetDeviceId(),
		SessionID:   e.GetSessionId(),
		Seq:         e.GetSeq(),

		AppVersion: ctx.GetAppVersion(),
		SDKVersion: ctx.GetSdkVersion(),
		OS:         ctx.GetOs(),
		OSVersion:  ctx.GetOsVersion(),
		Locale:     ctx.GetLocale(),

		TrustTier: c.TrustTier,
		InstallID: c.InstallID,

		Props: propsJSON(e.GetProps()),
	}, clamped
}

// propsJSON flattens the Value union into a plain JSON object so ClickHouse's
// JSON type produces subcolumns named after the property, not after the union
// arm. {"amount":19.99}, never {"amount":{"numberValue":19.99}}.
func propsJSON(props map[string]*trackingv1.Value) string {
	if len(props) == 0 {
		return "{}"
	}
	flat := make(map[string]any, len(props))
	for k, v := range props {
		switch val := v.GetKind().(type) {
		case *trackingv1.Value_StringValue:
			flat[k] = val.StringValue
		case *trackingv1.Value_NumberValue:
			flat[k] = val.NumberValue
		case *trackingv1.Value_BoolValue:
			flat[k] = val.BoolValue
		}
	}
	b, err := json.Marshal(flat)
	if err != nil {
		// Unreachable: every value is a string, float64, or bool. Falling back
		// to an empty object is better than dropping the event.
		return "{}"
	}
	return string(b)
}

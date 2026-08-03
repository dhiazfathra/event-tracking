package validate_test

import (
	"strings"
	"testing"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/services/ingest/internal/validate"
)

func valid() *trackingv1.Event {
	return &trackingv1.Event{
		EventId:  "0191f4a2-1c3d-7000-8000-000000000001",
		Name:     "checkout_completed",
		TsClient: 1754092800000,
		Seq:      1,
		DeviceId: "device-abc",
		Props: map[string]*trackingv1.Value{
			"amount": {Kind: &trackingv1.Value_NumberValue{NumberValue: 19.99}},
		},
	}
}

func TestValidEventPasses(t *testing.T) {
	if r := validate.Event(valid()); r != nil {
		t.Errorf("valid event rejected: %+v", r)
	}
}

func TestRejectCodes(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*trackingv1.Event)
		want string
	}{
		{"missing event_id", func(e *trackingv1.Event) { e.EventId = "" }, validate.CodeMissingEventID},
		{"non-uuid event_id", func(e *trackingv1.Event) { e.EventId = "not-a-uuid" }, validate.CodeBadEventID},
		{"missing name", func(e *trackingv1.Event) { e.Name = "" }, validate.CodeMissingName},
		{"name too long", func(e *trackingv1.Event) { e.Name = strings.Repeat("x", 256) }, validate.CodeNameTooLong},
		{"missing device_id", func(e *trackingv1.Event) { e.DeviceId = "" }, validate.CodeMissingDeviceID},
		{"too many props", func(e *trackingv1.Event) {
			e.Props = map[string]*trackingv1.Value{}
			for i := 0; i < 51; i++ {
				e.Props[string(rune('a'+i%26))+strings.Repeat("z", i)] =
					&trackingv1.Value{Kind: &trackingv1.Value_BoolValue{BoolValue: true}}
			}
		}, validate.CodeTooManyProps},
		{"prop key too long", func(e *trackingv1.Event) {
			e.Props = map[string]*trackingv1.Value{
				strings.Repeat("k", 129): {Kind: &trackingv1.Value_BoolValue{BoolValue: true}},
			}
		}, validate.CodePropKeyTooLong},
		{"prop with no value set", func(e *trackingv1.Event) {
			e.Props = map[string]*trackingv1.Value{"amount": {}}
		}, validate.CodeInvalidPropertyType},
		{"prop with NaN number", func(e *trackingv1.Event) {
			e.Props = map[string]*trackingv1.Value{
				"amount": {Kind: &trackingv1.Value_NumberValue{NumberValue: nan()}},
			}
		}, validate.CodeInvalidPropertyType},
		{"prop with Inf number", func(e *trackingv1.Event) {
			e.Props = map[string]*trackingv1.Value{
				"amount": {Kind: &trackingv1.Value_NumberValue{NumberValue: inf()}},
			}
		}, validate.CodeInvalidPropertyType},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := valid()
			c.mut(e)
			r := validate.Event(e)
			if r == nil {
				t.Fatalf("expected reject %s, got nil", c.want)
			}
			if r.Code != c.want {
				t.Errorf("code = %q, want %q", r.Code, c.want)
			}
			// The reject must name the event so the client can mark exactly
			// that row dead. A reject without an event_id is unactionable.
			if r.EventId != e.EventId {
				t.Errorf("reject.event_id = %q, want %q", r.EventId, e.EventId)
			}
		})
	}
}

// Exactly 50 props is the boundary and must pass.
func TestFiftyPropsIsAllowed(t *testing.T) {
	e := valid()
	e.Props = map[string]*trackingv1.Value{}
	for i := 0; i < 50; i++ {
		e.Props[strings.Repeat("k", i+1)] = &trackingv1.Value{Kind: &trackingv1.Value_BoolValue{BoolValue: true}}
	}
	if r := validate.Event(e); r != nil {
		t.Errorf("50 props rejected: %+v", r)
	}
}

func TestStringPropIsAllowed(t *testing.T) {
	e := valid()
	e.Props = map[string]*trackingv1.Value{
		"currency": {Kind: &trackingv1.Value_StringValue{StringValue: "IDR"}},
	}
	if r := validate.Event(e); r != nil {
		t.Errorf("string prop rejected: %+v", r)
	}
}

func TestNilEventPropsIsAllowed(t *testing.T) {
	e := valid()
	e.Props = nil
	if r := validate.Event(e); r != nil {
		t.Errorf("nil props rejected: %+v", r)
	}
}

func nan() float64 {
	var zero float64
	return zero / zero
}

func inf() float64 {
	one := 1.0
	zero := 0.0
	return one / zero
}

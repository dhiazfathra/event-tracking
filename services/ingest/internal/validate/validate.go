// Package validate checks one event at a time and returns a Reject describing
// exactly what is wrong with it.
//
// The per-event granularity is the whole point. If one malformed event failed
// the batch with a 4xx, the client would retry that batch forever, the outbox
// would never drain, and every subsequent event would be stuck behind it. A
// single poison event would silently kill a user's entire telemetry stream.
package validate

import (
	"fmt"
	"math"

	"github.com/google/uuid"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/limits"
)

// Reject codes. These strings are in the public contract: the SDK logs them and
// support engineers grep for them. Do not reword an existing one — add a new
// code instead.
const (
	CodeMissingEventID      = "MISSING_EVENT_ID"
	CodeBadEventID          = "BAD_EVENT_ID"
	CodeMissingName         = "MISSING_NAME"
	CodeNameTooLong         = "NAME_TOO_LONG"
	CodeMissingDeviceID     = "MISSING_DEVICE_ID"
	CodeTooManyProps        = "TOO_MANY_PROPS"
	CodePropKeyTooLong      = "PROP_KEY_TOO_LONG"
	CodeInvalidPropertyType = "INVALID_PROPERTY_TYPE"
)

const (
	maxNameLen    = 255
	maxPropKeyLen = 128
)

// Event returns nil if the event is storable, or a Reject naming the first
// problem found. First-problem-only is deliberate: a client cannot act on a
// list of eight complaints any differently than on one.
func Event(e *trackingv1.Event) *trackingv1.Reject {
	reject := func(code, msg string) *trackingv1.Reject {
		return &trackingv1.Reject{EventId: e.GetEventId(), Code: code, Message: msg}
	}

	if e.GetEventId() == "" {
		return reject(CodeMissingEventID, "event_id is required")
	}
	if _, err := uuid.Parse(e.GetEventId()); err != nil {
		return reject(CodeBadEventID, "event_id must be a UUID")
	}
	if e.GetName() == "" {
		return reject(CodeMissingName, "name is required")
	}
	if len(e.GetName()) > maxNameLen {
		return reject(CodeNameTooLong, fmt.Sprintf("name exceeds %d bytes", maxNameLen))
	}
	if e.GetDeviceId() == "" {
		return reject(CodeMissingDeviceID, "device_id is required")
	}

	// Bounds the ClickHouse JSON subcolumn explosion. The limit is a stated
	// guess (spec §7.5), not a measured one — but an unbounded property space
	// is a per-tenant denial of service on the whole table.
	if len(e.GetProps()) > limits.MaxPropsPerEvent {
		return reject(CodeTooManyProps,
			fmt.Sprintf("props: %d exceeds limit of %d", len(e.GetProps()), limits.MaxPropsPerEvent))
	}

	for k, v := range e.GetProps() {
		if len(k) > maxPropKeyLen {
			return reject(CodePropKeyTooLong, fmt.Sprintf("props.%s: key exceeds %d bytes", k, maxPropKeyLen))
		}
		switch val := v.GetKind().(type) {
		case *trackingv1.Value_StringValue, *trackingv1.Value_BoolValue:
			// fine
		case *trackingv1.Value_NumberValue:
			// NaN and Inf round-trip through JSON as nulls or errors depending
			// on the decoder. Reject them here rather than storing garbage.
			if math.IsNaN(val.NumberValue) || math.IsInf(val.NumberValue, 0) {
				return reject(CodeInvalidPropertyType, fmt.Sprintf("props.%s: must be a finite number", k))
			}
		default:
			return reject(CodeInvalidPropertyType, fmt.Sprintf("props.%s: no value set", k))
		}
	}

	return nil
}

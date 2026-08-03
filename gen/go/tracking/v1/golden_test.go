package trackingv1_test

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/google/go-cmp/cmp"

	trackingv1 "github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"
	"github.com/dhiazfathra/event-tracking/pkg/testsupport"
)

func TestEventFullGoldenRoundTrip(t *testing.T) {
	raw := testsupport.LoadGolden(t, "event_full.json")

	var got trackingv1.Event
	if err := protojson.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	want := &trackingv1.Event{
		EventId:     "0191f4a2-1c3d-7000-8000-000000000001",
		Name:        "checkout_completed",
		TsClient:    1754092800000,
		Seq:         42,
		DeviceId:    "device-abc",
		SessionId:   "session-xyz",
		AnonymousId: "anon-1",
		UserId:      "user-9",
		Props: map[string]*trackingv1.Value{
			"amount":   {Kind: &trackingv1.Value_NumberValue{NumberValue: 19.99}},
			"currency": {Kind: &trackingv1.Value_StringValue{StringValue: "IDR"}},
			"first":    {Kind: &trackingv1.Value_BoolValue{BoolValue: true}},
		},
		Context: &trackingv1.Context{
			SdkVersion: "0.1.0",
			AppVersion: "2.3.4",
			Os:         "android",
			OsVersion:  "14",
			Locale:     "id-ID",
		},
	}

	if diff := cmp.Diff(want, &got, protocmp.Transform()); diff != "" {
		t.Errorf("decoded event mismatch (-want +got):\n%s", diff)
	}
}

// 64-bit fields MUST be strings on the wire. If this ever passes with numbers,
// the Dart client and the Go server have silently diverged.
func TestSixtyFourBitFieldsAreJSONStrings(t *testing.T) {
	raw := testsupport.LoadGolden(t, "event_full.json")
	for _, frag := range []string{`"tsClient": "1754092800000"`, `"seq": "42"`} {
		if !bytes.Contains(raw, []byte(frag)) {
			t.Errorf("golden must encode 64-bit field as a JSON string; missing %s", frag)
		}
	}
}

func TestBatchResponsePartialGolden(t *testing.T) {
	raw := testsupport.LoadGolden(t, "batch_response_partial.json")

	var got trackingv1.BatchResponse
	if err := protojson.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Accepted) != 2 {
		t.Errorf("accepted = %d, want 2", len(got.Accepted))
	}
	if len(got.Rejected) != 1 {
		t.Fatalf("rejected = %d, want 1", len(got.Rejected))
	}
	if got.Rejected[0].Code != "INVALID_PROPERTY_TYPE" {
		t.Errorf("reject code = %q, want INVALID_PROPERTY_TYPE", got.Rejected[0].Code)
	}
}

package limits_test

import (
	"testing"

	"github.com/dhiazfathra/event-tracking/pkg/limits"
)

// These values are part of the public contract: the SDK, the ingest validator,
// and the docs all quote them. A change here is a spec change.
func TestContractValues(t *testing.T) {
	cases := []struct {
		name string
		got  int
		want int
	}{
		{"MaxEventsPerBatch", limits.MaxEventsPerBatch, 500},
		{"MaxBatchBytes", limits.MaxBatchBytes, 1048576},
		{"MaxPropsPerEvent", limits.MaxPropsPerEvent, 50},
		{"MaxOutboxRows", limits.MaxOutboxRows, 10000},
		{"MaxRetryAttempts", limits.MaxRetryAttempts, 20},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

package main

import (
	"reflect"
	"testing"
)

func TestMergedImports(t *testing.T) {
	p := pkgInfo{
		Imports:      []string{"a"},
		TestImports:  []string{"b"},
		XTestImports: []string{"c"},
	}
	got := mergedImports(p)
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("mergedImports = %v, want %v", got, want)
	}
	if len(p.Imports) != 1 {
		t.Errorf("mergedImports mutated source Imports slice: %v", p.Imports)
	}
}

func TestViolations(t *testing.T) {
	cases := []struct {
		name     string
		pkg      string
		imports  []string
		wantMsgs int
	}{
		{
			name:     "service importing another service",
			pkg:      "github.com/dhiazfathra/event-tracking/services/query/internal/handler",
			imports:  []string{"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"},
			wantMsgs: 1,
		},
		{
			name:     "pkg importing a service",
			pkg:      "github.com/dhiazfathra/event-tracking/pkg/tenant",
			imports:  []string{"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"},
			wantMsgs: 1,
		},
		{
			name:     "service importing pkg is fine",
			pkg:      "github.com/dhiazfathra/event-tracking/services/ingest/internal/handler",
			imports:  []string{"github.com/dhiazfathra/event-tracking/pkg/clickhouse"},
			wantMsgs: 0,
		},
		{
			name:     "service importing its own internals is fine",
			pkg:      "github.com/dhiazfathra/event-tracking/services/ingest/cmd",
			imports:  []string{"github.com/dhiazfathra/event-tracking/services/ingest/internal/handler"},
			wantMsgs: 0,
		},
		{
			name:     "service importing gen is fine",
			pkg:      "github.com/dhiazfathra/event-tracking/services/query/internal/compile",
			imports:  []string{"github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"},
			wantMsgs: 0,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := violations(c.pkg, c.imports)
			if len(got) != c.wantMsgs {
				t.Errorf("violations = %v (%d), want %d", got, len(got), c.wantMsgs)
			}
		})
	}
}

# Contracts & Repo Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the protobuf contracts, committed Go/Dart codegen, Go workspace, shared golden fixtures, and the CI checks that keep module boundaries and wire compatibility from rotting.

**Architecture:** Protobuf in `proto/` is the single source of truth for the event envelope and the query DSL, but the wire format is **JSON, not protobuf binary** (spec §2). `buf generate` emits Go and Dart types into `gen/`, which is committed so neither a Go build nor `flutter pub get` needs the proto toolchain. Golden JSON fixtures in `testdata/golden/` are parsed by both the Go and Dart test suites so a proto-JSON encoding divergence fails CI on both sides.

**Tech Stack:** Go 1.23+, `buf` CLI v1.47+, `protoc-gen-go`, `protoc-gen-dart`, Dart 3.5+, `google/go-cmp`, GitHub Actions.

## Global Constraints

Copied verbatim from the spec. Every task's requirements implicitly include this section.

- Wire format is **JSON, not protobuf binary**. Proto is used for schema definition and codegen only.
- `gen/` is **committed to git**. CI runs `make gen && git diff --exit-code`.
- `services/*` may not import each other. They share only `pkg/*` and `gen/go`.
- `clients/flutter_sdk` and `services/*` share exactly one thing: `gen/`.
- `pkg/*` may not import `services/*`. Dependency direction is one-way.
- `buf breaking` runs in CI against the previous release tag.
- `tenant_id` is deliberately absent from the `Event` message. A client-supplied tenant field is a cross-tenant write primitive.
- `event_id` is a client-generated UUID v7.
- 64-bit ints (`int64 ts_client`, `uint64 seq`) encode as **JSON strings** under proto-JSON. Golden fixtures pin this.
- Batch limits: ≤500 events, ≤1 MB decompressed.
- Events with >50 properties are rejected at ingest (limit is a constant defined here, enforced in plan 2).

---

## File Structure

| File | Responsibility |
|---|---|
| `buf.yaml` | Module config, lint + breaking rules |
| `buf.gen.yaml` | Codegen plugin config for Go and Dart |
| `proto/tracking/v1/event.proto` | `Event` envelope, `Value` property union, `Context` |
| `proto/tracking/v1/ingest.proto` | `BatchRequest`, `BatchResponse`, `Reject`, `TokenRequest`, `TokenResponse` |
| `proto/tracking/v1/query.proto` | Query DSL AST and result shapes |
| `gen/go/tracking/v1/*.pb.go` | Generated Go types (committed) |
| `gen/dart/tracking/v1/*.pb.dart` | Generated Dart types (committed) |
| `go.work` | Workspace over `services/*`, `pkg/*`, `gen/go` |
| `pkg/limits/limits.go` | Shared numeric constants (batch size, prop count, byte caps) |
| `pkg/testsupport/golden.go` | Golden fixture loader for Go tests |
| `testdata/golden/*.json` | Shared fixtures read by Go **and** Dart tests |
| `Makefile` | `gen`, `test`, `lint`, `check-boundaries` targets |
| `tools/checkboundaries/main.go` | Import-graph enforcement of the three module rules |
| `.github/workflows/ci.yml` | Runs gen-drift, boundaries, buf lint/breaking, tests |

Note: `.gitignore` currently ignores `go.work` and `go.work.sum` (Go template default). Task 3 removes that ignore — the workspace file is load-bearing for this monorepo and must be committed.

---

### Task 1: Proto module scaffolding and the event envelope

**Files:**
- Create: `buf.yaml`
- Create: `buf.gen.yaml`
- Create: `proto/tracking/v1/event.proto`
- Test: `make proto-lint` (buf's own lint is the test at this stage)

**Interfaces:**
- Consumes: nothing.
- Produces: proto package `tracking.v1`; messages `Event`, `Value`, `Context`. Go import path `github.com/dhiazfathra/event-tracking/gen/go/tracking/v1`, Go package alias `trackingv1`.

- [ ] **Step 1: Write `buf.yaml`**

```yaml
version: v2
modules:
  - path: proto
lint:
  use:
    - STANDARD
  except:
    - PACKAGE_VERSION_SUFFIX
breaking:
  use:
    - FILE
    - WIRE_JSON
```

`WIRE_JSON` matters more than `WIRE` here: the wire format is JSON, so a field
rename is breaking even though the field number is unchanged.

- [ ] **Step 2: Write `buf.gen.yaml`**

```yaml
version: v2
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/dhiazfathra/event-tracking/gen/go
plugins:
  - remote: buf.build/protocolbuffers/go:v1.35.2
    out: gen/go
    opt: paths=source_relative
  - remote: buf.build/protocolbuffers/dart:v21.1.2
    out: gen/dart
```

- [ ] **Step 3: Write `proto/tracking/v1/event.proto`**

```protobuf
syntax = "proto3";

package tracking.v1;

// Value is the property union. Deliberately closed: props are analytics
// dimensions, not arbitrary documents. Nested objects would make the
// ClickHouse JSON subcolumn explosion unbounded.
message Value {
  oneof kind {
    string string_value = 1;
    double number_value = 2;
    bool   bool_value   = 3;
  }
}

// Context is SDK-populated, not host-app populated.
message Context {
  string sdk_version = 1;
  string app_version = 2;
  string os          = 3;
  string os_version  = 4;
  string locale      = 5;
}

// Event is the ingestion envelope.
//
// tenant_id is deliberately absent. It is read server-side from the verified
// token's tenant_id claim. A client-supplied tenant field is a cross-tenant
// write primitive.
message Event {
  string event_id     = 1;  // UUID v7, client-generated. Dedup key + rough time ordering.
  string name         = 2;
  int64  ts_client    = 3;  // client wall clock, millis. Untrusted.
  uint64 seq          = 4;  // per-device monotonic. Gap detection only.
  string device_id    = 5;
  string session_id   = 6;
  string anonymous_id = 7;
  string user_id      = 8;  // empty until identify()
  map<string, Value> props = 9;
  Context context     = 10;
}
```

- [ ] **Step 4: Run buf lint to verify it passes**

Run: `buf lint`
Expected: no output, exit 0. If it complains about `Value`/`Context` naming, the
`except: PACKAGE_VERSION_SUFFIX` line is not the issue — read the message and fix
the proto, not the config.

- [ ] **Step 5: Commit**

```bash
git add buf.yaml buf.gen.yaml proto/tracking/v1/event.proto
git commit -m "feat(proto): add tracking.v1 event envelope and buf config"
```

---

### Task 2: Ingest and query protos

**Files:**
- Create: `proto/tracking/v1/ingest.proto`
- Create: `proto/tracking/v1/query.proto`
- Test: `buf lint`

**Interfaces:**
- Consumes: `tracking.v1.Event` from Task 1.
- Produces: `BatchRequest`, `BatchResponse`, `Reject`, `TokenRequest`, `TokenResponse`, `TimeseriesRequest`, `TimeseriesResponse`, `Filter`, `Series`, `Point`.

- [ ] **Step 1: Write `proto/tracking/v1/ingest.proto`**

```protobuf
syntax = "proto3";

package tracking.v1;

import "tracking/v1/event.proto";

message BatchRequest {
  int64 sent_at = 1;              // client wall clock at send, millis
  repeated Event events = 2;      // <= 500, <= 1 MB decompressed
}

// Reject codes. Stable strings — the SDK logs them and support reads them.
message Reject {
  string event_id = 1;
  string code     = 2;   // INVALID_PROPERTY_TYPE | MISSING_EVENT_ID | ...
  string message  = 3;
}

// Always partial-success shaped. A single malformed event must never fail the
// whole batch: the client would retry forever and the outbox would never drain.
message BatchResponse {
  int64 received_at = 1;
  repeated string accepted = 2;
  repeated Reject rejected = 3;
}

message TokenRequest {
  string client_id   = 1;   // public, embedded in the SDK, authorizes nothing
  string platform    = 2;   // "ios" | "android"
  string attestation = 3;   // App Attest / Play Integrity blob; may be empty
}

message TokenResponse {
  string access_token = 1;  // EdDSA JWT, typ=at+jwt
  int64  expires_in   = 2;  // seconds; 1800-3600
  uint32 trust_tier   = 3;  // 0 = attested, 1 = attestation unavailable
}
```

- [ ] **Step 2: Write `proto/tracking/v1/query.proto`**

```protobuf
syntax = "proto3";

package tracking.v1;

// The query DSL. Constrained on purpose: compiled to parameterized SQL,
// never passed through as SQL. tenant_id appears nowhere — the query service
// injects it server-side from the verified read key.

enum Op {
  OP_UNSPECIFIED = 0;
  OP_EQ          = 1;
  OP_NEQ         = 2;
  OP_IN          = 3;
  OP_GT          = 4;
  OP_LT          = 5;
}

message Filter {
  string field = 1;            // allowlisted column or "props.<key>"
  Op     op    = 2;
  repeated string values = 3;  // always strings on the wire; cast at compile time
}

enum Interval {
  INTERVAL_UNSPECIFIED = 0;
  INTERVAL_HOUR        = 1;
  INTERVAL_DAY         = 2;
  INTERVAL_WEEK        = 3;
}

enum Metric {
  METRIC_UNSPECIFIED = 0;
  METRIC_EVENTS      = 1;  // uniqExact(event_id)
  METRIC_USERS       = 2;  // uniqExact(user_id)
}

message TimeseriesRequest {
  string   event_name = 1;
  int64    from_ms    = 2;
  int64    to_ms      = 3;
  Interval interval   = 4;
  Metric   metric     = 5;
  repeated Filter filters = 6;
  repeated string group_by = 7;   // allowlisted dimensions, max 2
  bool approximate = 8;           // uniq() HLL instead of uniqExact()
}

message Point {
  int64  bucket_ms = 1;
  uint64 value     = 2;
}

message Series {
  map<string, string> group = 1;  // empty when group_by is empty
  repeated Point points = 2;
}

message TimeseriesResponse {
  repeated Series series = 1;
  string source      = 2;   // "raw" | "rollup" — never conflate the two
  bool   approximate = 3;
  int64  computed_at = 4;
  string etag        = 5;
}
```

- [ ] **Step 3: Run buf lint**

Run: `buf lint`
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add proto/tracking/v1/ingest.proto proto/tracking/v1/query.proto
git commit -m "feat(proto): add ingest and query DSL contracts"
```

---

### Task 3: Generate and commit Go + Dart code, wire up the Go workspace

**Files:**
- Create: `Makefile`
- Create: `go.work`
- Create: `gen/go/tracking/v1/{event,ingest,query}.pb.go` (generated)
- Create: `gen/dart/tracking/v1/*.pb.dart` (generated)
- Create: `gen/go/go.mod`
- Modify: `.gitignore` — remove the `go.work` / `go.work.sum` ignore lines
- Test: `go build ./...`

**Interfaces:**
- Consumes: protos from Tasks 1–2.
- Produces: Go package `trackingv1` importable as `github.com/dhiazfathra/event-tracking/gen/go/tracking/v1`; `make gen` target.

- [ ] **Step 1: Write the `Makefile`**

```makefile
.PHONY: gen gen-check lint test check-boundaries

gen:
	buf generate

# CI gate: generated code must match the protos exactly.
gen-check: gen
	git diff --exit-code -- gen/

lint:
	buf lint
	golangci-lint run ./...

check-boundaries:
	go run ./tools/checkboundaries

test:
	go test ./...
```

- [ ] **Step 2: Remove the `go.work` ignore from `.gitignore`**

Delete these three lines:

```
# Go workspace file
go.work
go.work.sum
```

The workspace file is load-bearing in a monorepo with `services/*`, `pkg/*`, and
`gen/go` as separate modules. Ignoring it is a stock-template default that is
wrong here.

- [ ] **Step 3: Create the `gen/go` module and generate**

```bash
mkdir -p gen/go
cd gen/go && go mod init github.com/dhiazfathra/event-tracking/gen/go && go get google.golang.org/protobuf@v1.35.2 && cd ../..
make gen
```

- [ ] **Step 4: Write `go.work`**

```
go 1.23

use (
	./gen/go
	./pkg/limits
	./pkg/testsupport
)
```

Later plans append their own modules (`./services/ingest`, `./services/query`,
`./pkg/clickhouse`, `./pkg/tenant`, `./pkg/querydsl`) as they are created.

- [ ] **Step 5: Run the build to verify generated Go compiles**

Run: `go build ./...`
Expected: exit 0, no output. (`pkg/limits` and `pkg/testsupport` do not exist yet
— create them in Task 4 and re-run; until then, list only `./gen/go` in `go.work`.)

- [ ] **Step 6: Commit**

```bash
git add Makefile go.work .gitignore gen/
git commit -m "build: add buf codegen, committed gen/, and go workspace"
```

---

### Task 4: Shared limits and the golden fixture loader

**Files:**
- Create: `pkg/limits/go.mod`
- Create: `pkg/limits/limits.go`
- Create: `pkg/testsupport/go.mod`
- Create: `pkg/testsupport/golden.go`
- Test: `pkg/limits/limits_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `limits.MaxEventsPerBatch = 500` (int)
  - `limits.MaxBatchBytes = 1 << 20` (int)
  - `limits.MaxPropsPerEvent = 50` (int)
  - `limits.MaxOutboxRows = 10_000` (int)
  - `limits.MaxRetryAttempts = 20` (int)
  - `testsupport.LoadGolden(t *testing.T, name string) []byte`
  - `testsupport.GoldenPath(name string) string`

- [ ] **Step 1: Write the failing test**

`pkg/limits/limits_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg/limits && go test ./...`
Expected: FAIL — `package limits is not in std` or `undefined: limits.MaxEventsPerBatch`.

- [ ] **Step 3: Write the implementation**

```bash
mkdir -p pkg/limits && cd pkg/limits && go mod init github.com/dhiazfathra/event-tracking/pkg/limits && cd ../..
```

`pkg/limits/limits.go`:

```go
// Package limits holds the numeric constants that appear in the public
// contract. They are shared so the ingest validator and the SDK cannot drift.
package limits

const (
	// MaxEventsPerBatch caps a single POST /v1/batch. Over this: 413.
	MaxEventsPerBatch = 500

	// MaxBatchBytes caps the decompressed batch body. Over this: 413.
	MaxBatchBytes = 1 << 20 // 1 MiB

	// MaxPropsPerEvent bounds ClickHouse JSON subcolumn explosion.
	// Per-event reject, not a batch failure.
	MaxPropsPerEvent = 50

	// MaxOutboxRows caps the client outbox across every state. Overflow evicts
	// the oldest pending rows and increments dropped_events.
	MaxOutboxRows = 10_000

	// MaxRetryAttempts is when the client gives up and marks an event dead.
	MaxRetryAttempts = 20
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd pkg/limits && go test ./...`
Expected: PASS.

- [ ] **Step 5: Write the golden loader**

```bash
mkdir -p pkg/testsupport && cd pkg/testsupport && go mod init github.com/dhiazfathra/event-tracking/pkg/testsupport && cd ../..
```

`pkg/testsupport/golden.go`:

```go
// Package testsupport holds test-only helpers shared across Go modules.
package testsupport

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// GoldenPath resolves a fixture name to its absolute path in testdata/golden.
// Resolution is relative to this source file, not the caller's working
// directory, so it works from any module in the workspace.
func GoldenPath(name string) string {
	_, self, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(self))) // pkg/testsupport -> pkg -> root
	return filepath.Join(repoRoot, "testdata", "golden", name)
}

// LoadGolden reads a shared fixture. The same files are parsed by the Dart test
// suite, so an encoding divergence between Go and Dart fails CI on both sides.
func LoadGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(GoldenPath(name))
	if err != nil {
		t.Fatalf("load golden %q: %v", name, err)
	}
	return b
}
```

- [ ] **Step 6: Add both modules to `go.work` and build**

Append to `go.work` `use (...)`: `./pkg/limits` and `./pkg/testsupport` (already
listed in Task 3 Step 4 — verify they resolve now).

Run: `go build ./... && go test ./...`
Expected: exit 0, `pkg/limits` tests PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/limits pkg/testsupport go.work
git commit -m "feat(pkg): add shared contract limits and golden fixture loader"
```

---

### Task 5: Golden JSON fixtures pinning proto-JSON encoding

**Files:**
- Create: `testdata/golden/event_full.json`
- Create: `testdata/golden/batch_response_partial.json`
- Test: `gen/go/tracking/v1/golden_test.go`

**Interfaces:**
- Consumes: `trackingv1.Event`, `trackingv1.BatchResponse`, `testsupport.LoadGolden`.
- Produces: fixture names `event_full.json` and `batch_response_partial.json`, consumed unchanged by the Dart test suite in plan 4.

The point of this task: proto-JSON encodes `int64` and `uint64` as **strings**. A
hand-written Dart encoder that emits them as numbers will parse fine locally and
break the server. These fixtures make that failure loud and immediate.

- [ ] **Step 1: Write the failing test**

`gen/go/tracking/v1/golden_test.go`:

```go
package trackingv1_test

import (
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
		if !contains(raw, frag) {
			t.Errorf("golden must encode 64-bit field as a JSON string; missing %s", frag)
		}
	}
}

func contains(hay []byte, needle string) bool {
	return len(needle) > 0 && len(hay) >= len(needle) &&
		string(hay) != "" && indexOf(string(hay), needle) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd gen/go && go test ./tracking/v1/...`
Expected: FAIL — `load golden "event_full.json": no such file or directory`.

- [ ] **Step 3: Write the fixtures**

`testdata/golden/event_full.json`:

```json
{
  "eventId": "0191f4a2-1c3d-7000-8000-000000000001",
  "name": "checkout_completed",
  "tsClient": "1754092800000",
  "seq": "42",
  "deviceId": "device-abc",
  "sessionId": "session-xyz",
  "anonymousId": "anon-1",
  "userId": "user-9",
  "props": {
    "amount": { "numberValue": 19.99 },
    "currency": { "stringValue": "IDR" },
    "first": { "boolValue": true }
  },
  "context": {
    "sdkVersion": "0.1.0",
    "appVersion": "2.3.4",
    "os": "android",
    "osVersion": "14",
    "locale": "id-ID"
  }
}
```

`testdata/golden/batch_response_partial.json`:

```json
{
  "receivedAt": "1754092800412",
  "accepted": [
    "0191f4a2-1c3d-7000-8000-000000000001",
    "0191f4a2-1c3d-7000-8000-000000000002"
  ],
  "rejected": [
    {
      "eventId": "0191f4a2-1c3d-7000-8000-000000000003",
      "code": "INVALID_PROPERTY_TYPE",
      "message": "props.amount: expected number"
    }
  ]
}
```

- [ ] **Step 4: Add the test dependency and run**

Run: `cd gen/go && go get github.com/google/go-cmp@v0.6.0 && go test ./tracking/v1/...`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add testdata/golden gen/go
git commit -m "test(contract): pin proto-JSON encoding with shared golden fixtures"
```

---

### Task 6: Module boundary enforcement

**Files:**
- Create: `tools/go.mod`
- Create: `tools/checkboundaries/main.go`
- Test: `tools/checkboundaries/main_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `go run ./tools/checkboundaries` — exits non-zero listing every violation. Wired into `make check-boundaries` and CI.

The three rules exist because they are cheap to state and expensive to recover
from. A `services/query` that imports `services/ingest` is a deploy coupling that
nobody notices until the first independent release.

- [ ] **Step 1: Write the failing test**

`tools/checkboundaries/main_test.go`:

```go
package main

import "testing"

func TestViolations(t *testing.T) {
	cases := []struct {
		name     string
		pkg      string
		imports  []string
		wantMsgs int
	}{
		{
			name:    "service importing another service",
			pkg:     "github.com/dhiazfathra/event-tracking/services/query/internal/handler",
			imports: []string{"github.com/dhiazfathra/event-tracking/services/ingest/internal/quota"},
			wantMsgs: 1,
		},
		{
			name:    "pkg importing a service",
			pkg:     "github.com/dhiazfathra/event-tracking/pkg/tenant",
			imports: []string{"github.com/dhiazfathra/event-tracking/services/ingest/internal/enrich"},
			wantMsgs: 1,
		},
		{
			name:    "service importing pkg is fine",
			pkg:     "github.com/dhiazfathra/event-tracking/services/ingest/internal/handler",
			imports: []string{"github.com/dhiazfathra/event-tracking/pkg/clickhouse"},
			wantMsgs: 0,
		},
		{
			name:    "service importing its own internals is fine",
			pkg:     "github.com/dhiazfathra/event-tracking/services/ingest/cmd",
			imports: []string{"github.com/dhiazfathra/event-tracking/services/ingest/internal/handler"},
			wantMsgs: 0,
		},
		{
			name:    "service importing gen is fine",
			pkg:     "github.com/dhiazfathra/event-tracking/services/query/internal/compile",
			imports: []string{"github.com/dhiazfathra/event-tracking/gen/go/tracking/v1"},
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools && go test ./checkboundaries/...`
Expected: FAIL — `undefined: violations`.

- [ ] **Step 3: Write the implementation**

```bash
mkdir -p tools/checkboundaries && cd tools && go mod init github.com/dhiazfathra/event-tracking/tools && cd ..
```

`tools/checkboundaries/main.go`:

```go
// Command checkboundaries enforces the monorepo's three module rules:
//
//  1. services/* may not import each other.
//  2. pkg/* may not import services/*.
//  3. (rule 3 — clients share only gen/ — is a Dart-side concern and is
//     enforced by the Flutter SDK having no path dependency on Go modules.)
//
// It shells out to `go list` over the workspace rather than parsing files, so
// build tags and generated code are handled the same way the compiler does.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const modulePrefix = "github.com/dhiazfathra/event-tracking/"

type pkgInfo struct {
	ImportPath string
	Imports    []string
}

func main() {
	pkgs, err := listPackages()
	if err != nil {
		fmt.Fprintf(os.Stderr, "go list failed: %v\n", err)
		os.Exit(2)
	}

	var all []string
	for _, p := range pkgs {
		all = append(all, violations(p.ImportPath, p.Imports)...)
	}

	if len(all) > 0 {
		fmt.Fprintln(os.Stderr, "module boundary violations:")
		for _, v := range all {
			fmt.Fprintf(os.Stderr, "  %s\n", v)
		}
		os.Exit(1)
	}
	fmt.Println("module boundaries OK")
}

func listPackages() ([]pkgInfo, error) {
	cmd := exec.Command("go", "list", "-json", "./...")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var pkgs []pkgInfo
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for dec.More() {
		var p pkgInfo
		if err := dec.Decode(&p); err != nil {
			return nil, err
		}
		pkgs = append(pkgs, p)
	}
	return pkgs, nil
}

// serviceOf returns the service name for an import path under services/, or "".
func serviceOf(path string) string {
	rest, ok := strings.CutPrefix(path, modulePrefix+"services/")
	if !ok {
		return ""
	}
	return strings.SplitN(rest, "/", 2)[0]
}

func isPkg(path string) bool {
	return strings.HasPrefix(path, modulePrefix+"pkg/")
}

// violations reports rule breaches for one package's import list.
func violations(pkg string, imports []string) []string {
	var out []string
	self := serviceOf(pkg)

	for _, imp := range imports {
		other := serviceOf(imp)

		// Rule 1: cross-service import.
		if self != "" && other != "" && other != self {
			out = append(out, fmt.Sprintf("%s imports %s (services/* may not import each other)", pkg, imp))
		}

		// Rule 2: pkg reaching into a service.
		if isPkg(pkg) && other != "" {
			out = append(out, fmt.Sprintf("%s imports %s (pkg/* may not import services/*)", pkg, imp))
		}
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools && go test ./checkboundaries/...`
Expected: PASS, 5 subtests.

- [ ] **Step 5: Run it against the real workspace**

Run: `go run ./tools/checkboundaries`
Expected: `module boundaries OK` (no services exist yet — this proves the tool runs
clean rather than proving anything about the code).

- [ ] **Step 6: Commit**

```bash
git add tools go.work
git commit -m "build: enforce monorepo module boundaries in CI"
```

---

### Task 7: CI pipeline

**Files:**
- Create: `.github/workflows/ci.yml`
- Test: push the branch and read the Actions run

**Interfaces:**
- Consumes: `make gen-check`, `make check-boundaries`, `make lint`, `make test`, `buf breaking`.
- Produces: a required-check surface for every later plan. Plans 2–4 add their own jobs to this file.

- [ ] **Step 1: Write the workflow**

`.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [master]
  pull_request:

jobs:
  contracts:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # buf breaking needs history
      - uses: actions/setup-go@v5
        with:
          go-version: '1.23'
      - uses: bufbuild/buf-setup-action@v1
        with:
          version: 1.47.2

      - name: Lint protos
        run: buf lint

      # An SDK deployed into third-party apps cannot be force-upgraded, so a
      # breaking envelope change is unfixable once shipped.
      - name: Breaking change check
        run: buf breaking --against '.git#tag=$(git describe --tags --abbrev=0 2>/dev/null || echo HEAD~1)'
        continue-on-error: false

      - name: Generated code is up to date
        run: make gen-check

      - name: Module boundaries
        run: make check-boundaries

      - name: Go tests
        run: make test
```

- [ ] **Step 2: Verify the gen-drift gate actually catches drift**

```bash
sed -i.bak 's/string locale    = 5;/string locale    = 5;\n  string timezone   = 6;/' proto/tracking/v1/event.proto
make gen-check
```

Expected: FAIL — `git diff --exit-code` reports changes under `gen/`.

Then revert:

```bash
mv proto/tracking/v1/event.proto.bak proto/tracking/v1/event.proto
make gen && git diff --exit-code -- gen/
```

Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: lint protos, gate gen drift, breaking changes, and boundaries"
```

---

### Task 8: Foundation README

**Files:**
- Modify: `README.md`
- Create: `docs/decisions/README.md` (ADR index, if absent)

**Interfaces:**
- Consumes: everything above.
- Produces: contributor entry point.

- [ ] **Step 1: Add a Development section to `README.md`**

Append:

````markdown
## Development

### Prerequisites

- Go 1.23+
- [`buf`](https://buf.build/docs/installation) 1.47+
- Dart 3.5+ / Flutter 3.24+ (only for `clients/flutter_sdk`)

### Common commands

```bash
make gen               # regenerate gen/ from proto/ — commit the result
make gen-check         # CI gate: fails if gen/ is stale
make lint              # buf lint + golangci-lint
make check-boundaries  # enforce the three module rules
make test              # go test ./...
```

### Module rules

1. `services/*` may not import each other — shared logic moves to `pkg/`.
2. `clients/flutter_sdk` and `services/*` share exactly one thing: `gen/`.
3. `pkg/*` may not import `services/*`.

Enforced by `tools/checkboundaries` in CI, not by convention.

### Why `gen/` is committed

So neither a Go build nor `flutter pub get` requires the proto toolchain. The
cost is merge noise, paid for by the `make gen-check` CI gate.

### Wire format

JSON, not protobuf binary. Proto defines the schema and drives codegen only.
Note that proto-JSON encodes 64-bit integers (`ts_client`, `seq`, `received_at`)
as **strings** — `testdata/golden/` pins this, and both the Go and Dart test
suites read those same files.
````

- [ ] **Step 2: Verify the commands in the README actually run**

Run: `make gen-check && make check-boundaries && make test`
Expected: all exit 0.

- [ ] **Step 3: Commit**

```bash
git add README.md docs/decisions/README.md
git commit -m "docs: document build, module rules, and wire format"
```

---

## Completion criteria

- `make gen-check`, `make check-boundaries`, `make lint`, `make test` all pass.
- `gen/go` and `gen/dart` are committed and importable.
- `testdata/golden/` fixtures parse in Go; plan 4 asserts they parse identically in Dart.
- CI runs `buf breaking` against the previous tag.

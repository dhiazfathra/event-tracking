// Package enrich turns a validated wire envelope into a storage row: server
// timestamps, trust tier, install id, and skew-corrected event time.
package enrich

import (
	"context"
	"sync"
	"time"
)

const (
	// MaxFutureSkew and MaxPastSkew bound the corrected timestamp. Beyond them
	// the timestamp is clamped to received_at and flagged. Without this, one
	// device with a broken clock creates partitions years out and wrecks the
	// partition count for everybody.
	MaxFutureSkew = 24 * time.Hour
	MaxPastSkew   = 30 * 24 * time.Hour
)

// SessionKey scopes a clock offset. Per-session, never per-request.
type SessionKey struct {
	TenantID  string
	DeviceID  string
	SessionID string
}

// OffsetStore persists the first-contact offset for a session.
//
// GetOrSet returns the stored offset if one exists and stores+returns the
// candidate otherwise. It must be atomic: two concurrent first requests for the
// same session must agree on one value, or the two events land at different
// ts and a retry of either stops deduplicating.
type OffsetStore interface {
	GetOrSet(ctx context.Context, k SessionKey, candidate int64) (int64, error)
}

// Correct applies the session offset and clamps the result.
//
// Returns the corrected timestamp and whether it was clamped. Clamped rows are
// still stored — ts_client keeps the raw device value for forensics — but the
// flag lets ingest count how much of the corpus has an untrustworthy clock.
func Correct(tsClientMS, offsetMS int64, receivedAt time.Time) (time.Time, bool) {
	ts := time.UnixMilli(tsClientMS + offsetMS).UTC()

	if ts.Sub(receivedAt) > MaxFutureSkew {
		return receivedAt, true
	}
	if receivedAt.Sub(ts) > MaxPastSkew {
		return receivedAt, true
	}
	return ts, false
}

// CandidateOffset is the first-contact estimate: how far behind the server the
// device's clock appeared to be at send time.
func CandidateOffset(sentAtMS int64, receivedAt time.Time) int64 {
	if sentAtMS <= 0 {
		// Absent or nonsensical sent_at. A zero offset is recoverable; a
		// 55-year offset is pinned for the life of the session.
		return 0
	}
	offset := receivedAt.UnixMilli() - sentAtMS
	if offset > MaxPastSkew.Milliseconds() || offset < -MaxFutureSkew.Milliseconds() {
		return 0
	}
	return offset
}

// MemoryOffsetStore is the in-process implementation. Production wires the
// Postgres-backed store (session_offsets table) instead; this one exists so the
// skew logic is testable without a database and so a single-pod dev setup runs.
type MemoryOffsetStore struct {
	mu sync.Mutex
	m  map[SessionKey]int64
}

func NewMemoryOffsetStore() *MemoryOffsetStore {
	return &MemoryOffsetStore{m: make(map[SessionKey]int64)}
}

func (s *MemoryOffsetStore) GetOrSet(_ context.Context, k SessionKey, candidate int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.m[k]; ok {
		return v, nil
	}
	s.m[k] = candidate
	return candidate, nil
}

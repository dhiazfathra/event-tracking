// Package attest verifies platform attestations at the token exchange.
//
// Deliberately not on the ingest hot path: ingest only verifies a signature.
// Attestation is a per-app-start cost, not a per-batch cost.
package attest

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Attestor verifies a platform attestation blob and returns a stable subject
// identifying the app install, plus whether verification succeeded.
//
// A false result is not an error condition — it assigns Tier 1.
//
// challenge is the server-issued nonce the client embedded in the attestation.
// Both App Attest and Play Integrity bind a caller-supplied nonce into the
// signed payload; verifying it is what stops one captured attestation from
// being replayed for Tier 0 tokens indefinitely.
type Attestor interface {
	Verify(ctx context.Context, platform, blob, challenge string) (subject string, ok bool)
}

// Noop always reports failure, so every install lands at Tier 1. This is the
// correct local-dev and simulator behaviour: simulators cannot attest, and the
// design already treats attestation-unavailable as a tier, not a rejection.
type Noop struct{}

func (Noop) Verify(context.Context, string, string, string) (string, bool) {
	return "", false
}

// ChallengeStore issues and consumes one-time attestation nonces.
type ChallengeStore interface {
	Issue(ctx context.Context, clientID, platform string) (string, error)
	Consume(ctx context.Context, clientID, platform, challenge string) bool
}

// RedisChallenges is the production ChallengeStore.
//
// The nonce is bound to the client ID and platform that requested it, and
// consumed atomically via GETDEL so two concurrent exchanges cannot both spend
// the same one. The key is scoped to client+platform only (the nonce is the
// *value*, not part of the key), so a new Issue call overwrites any prior
// outstanding challenge for that client+platform instead of accumulating a
// fresh Redis key per call. That keeps outstanding challenges bounded to at
// most one per client+platform regardless of call volume, with no separate
// rate limiter needed for this endpoint.
type RedisChallenges struct {
	RDB *redis.Client
	TTL time.Duration // 5 minutes is ample for an app-start round trip
}

func (c RedisChallenges) Issue(ctx context.Context, clientID, platform string) (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("challenge entropy: %w", err)
	}
	nonce := base64.RawURLEncoding.EncodeToString(raw)

	// SET (not SETNX): a repeat call for the same client+platform replaces the
	// previous nonce rather than adding a new key, so outstanding challenges
	// never grow unbounded.
	if err := c.RDB.Set(ctx, c.key(clientID, platform), nonce, c.TTL).Err(); err != nil {
		return "", fmt.Errorf("store challenge: %w", err)
	}
	return nonce, nil
}

func (c RedisChallenges) Consume(ctx context.Context, clientID, platform, challenge string) bool {
	if challenge == "" {
		return false
	}
	// GETDEL: present-and-removed in one round trip. A second attempt with the
	// same nonce finds nothing, and a stale nonce (superseded by a later Issue)
	// never matches because it was overwritten, not appended.
	got, err := c.RDB.GetDel(ctx, c.key(clientID, platform)).Result()
	return err == nil && got == challenge
}

func (c RedisChallenges) key(clientID, platform string) string {
	return fmt.Sprintf("att:{%s}:%s", clientID, platform)
}

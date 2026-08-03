// Package quota enforces per-tenant daily budgets and per-install rate limits
// with Redis counters.
//
// Ordering of the buckets is deliberate: install_id first, tenant second, IP
// last (IP is not enforced here at all — it survives only as a coarse anomaly
// signal upstream). IP-primary limiting is actively harmful in this market:
// Indonesian carriers CGNAT aggressively, so one Telkomsel egress IP is
// thousands of real users.
package quota

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dhiazfathra/event-tracking/pkg/limits"
	"github.com/dhiazfathra/event-tracking/pkg/tenant"
)

type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	Reason     string // "" | "rate_limit" | "daily_quota"
}

type Checker struct {
	rdb *redis.Client
}

func NewChecker(rdb *redis.Client) *Checker {
	return &Checker{rdb: rdb}
}

// allowScript is a single atomic check-and-consume across both counters.
//
// The obvious implementation — increment, compare, DECRBY on denial — is
// wrong under concurrency: a denied request's rollback can land after another
// pod's increment, refunding budget that was legitimately spent. Nothing is
// consumed here unless the whole batch is admitted, so there is no rollback to
// race in the first place.
//
// Both keys use the same {tenant} hash tag, so they land on one slot and the
// script stays valid under Redis Cluster.
var allowScript = redis.NewScript(`
local rate_key   = KEYS[1]
local day_key    = KEYS[2]
local n          = tonumber(ARGV[1])
local rps        = tonumber(ARGV[2])
local daily      = tonumber(ARGV[3])
local rate_ttl   = tonumber(ARGV[4])
local day_ttl    = tonumber(ARGV[5])

local rate = tonumber(redis.call('GET', rate_key) or '0')
if rps > 0 and rate + n > rps then
  return {0, 'rate_limit'}
end

local day = tonumber(redis.call('GET', day_key) or '0')
if daily > 0 and day + n > daily then
  return {0, 'daily_quota'}
end

redis.call('INCRBY', rate_key, n)
redis.call('EXPIRE', rate_key, rate_ttl)
redis.call('INCRBY', day_key, n)
redis.call('EXPIRE', day_key, day_ttl)
return {1, ''}
`)

// Allow accounts n events and reports whether the batch may proceed.
//
// The rate-limit bucket keys on install_id first, tenant second — never IP.
// Two installs under the same tenant get independent per-second budgets, and
// no single install can spend the whole tenant's RPS in one second. IP is not
// part of this key at all: CGNAT means one carrier egress IP fronts thousands
// of distinct real users, so IP-primary limiting would punish all of them for
// one install's burst.
//
// Consumption is all-or-nothing: a denied batch consumes no budget at all.
func (c *Checker) Allow(ctx context.Context, cl tenant.Claims, lim limits.Quota, n int, now time.Time) (Decision, error) {
	rateKey := fmt.Sprintf("rl:{%s}:%s:%d", cl.TenantID, cl.InstallID, now.Unix())
	dayKey := fmt.Sprintf("q:{%s}:%s", cl.TenantID, now.UTC().Format("2006-01-02"))

	res, err := allowScript.Run(ctx, c.rdb,
		[]string{rateKey, dayKey},
		n, lim.RPS, lim.DailyEvents, 2, int((48 * time.Hour).Seconds()),
	).Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("quota check: %w", err)
	}

	allowed, _ := res[0].(int64)
	reason, _ := res[1].(string)
	if allowed == 1 {
		return Decision{Allowed: true}, nil
	}

	retryAfter := time.Second
	if reason == "daily_quota" {
		retryAfter = untilNextUTCDay(now)
	}
	return Decision{Allowed: false, RetryAfter: retryAfter, Reason: reason}, nil
}

// AllowClient is a coarse per-key rate limit for endpoints that have no
// install_id yet — namely POST /v1/auth/token, which mints the install_id
// rather than receiving one. It is deliberately separate from Allow: sharing
// Allow's tenant-scoped daily-quota key would make token minting spend the
// same budget as event ingestion, which is not the intent here. This exists
// so an unauthenticated client cannot mint unbounded installs rows by
// rotating device_hint on every call; it is not the security boundary, the
// tenant-wide ingest quota is.
//
// Plain INCR+EXPIRE, not a Lua script: this is a simple ceiling, not an
// all-or-nothing multi-counter admission decision, so the script's atomicity
// guarantees do not apply here.
func (c *Checker) AllowClient(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	n, err := c.rdb.Incr(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("client rate check: %w", err)
	}
	if n == 1 {
		if err := c.rdb.Expire(ctx, key, window).Err(); err != nil {
			return false, fmt.Errorf("client rate check: %w", err)
		}
	}
	return n <= int64(limit), nil
}

// untilNextUTCDay is the honest Retry-After for an exhausted daily budget:
// nothing the client does before midnight UTC will succeed.
func untilNextUTCDay(now time.Time) time.Duration {
	next := now.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	return next.Sub(now.UTC())
}

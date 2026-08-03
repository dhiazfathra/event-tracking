package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Row is the storage shape of an event. It is deliberately not the proto type:
// the wire envelope has no tenant_id, no trust_tier, and no install_id, and
// conflating the two is how a client-supplied tenant field sneaks into storage.
type Row struct {
	TenantID   string
	EventID    string
	Name       string
	TS         time.Time
	TSClient   time.Time
	TSReceived time.Time

	UserID      string
	AnonymousID string
	DeviceID    string
	SessionID   string
	Seq         uint64

	AppVersion string
	SDKVersion string
	OS         string
	OSVersion  string
	Locale     string

	TrustTier uint8
	InstallID string

	Props string // JSON object
}

// InsertEvents writes rows and does not return until ClickHouse has flushed
// them (wait_for_async_insert=1 on the connection). There is deliberately no
// in-process accumulator: a Go-side buffer would create a window where the
// service has acked events it has not written, losing them on a pod restart
// after the client already deleted its copy.
func InsertEvents(ctx context.Context, conn driver.Conn, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO events (
		tenant_id, event_id, name, ts, ts_client, ts_received,
		user_id, anonymous_id, device_id, session_id, seq,
		app_version, sdk_version, os, os_version, locale,
		trust_tier, install_id, props)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	// Abort() releases the reserved connection back to the pool; it is a
	// safe no-op after Send() already did so (returns ErrBatchAlreadySent).
	defer batch.Abort()

	for _, r := range rows {
		if err := batch.Append(
			r.TenantID, r.EventID, r.Name, r.TS, r.TSClient, r.TSReceived,
			r.UserID, r.AnonymousID, r.DeviceID, r.SessionID, r.Seq,
			r.AppVersion, r.SDKVersion, r.OS, r.OSVersion, r.Locale,
			r.TrustTier, r.InstallID, r.Props,
		); err != nil {
			return fmt.Errorf("append %s: %w", r.EventID, err)
		}
	}
	return batch.Send()
}

package clickhouse

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Config struct {
	Addrs    []string
	Database string
	Username string
	Password string
}

// Open returns a pooled connection configured for the ingest write path.
//
// The async-insert settings are the load-bearing part. Many stateless ingest
// pods each writing small batches would otherwise produce thousands of tiny
// parts per minute and push the table into "too many parts". ClickHouse
// coalesces them server-side instead.
//
// wait_for_async_insert=1 costs up to ~1s of p99 latency and buys the right to
// say a 200 means the data is flushed. The client deletes its outbox row on
// 200, so acking before the flush would be silent data loss.
func Open(ctx context.Context, cfg Config) (driver.Conn, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: cfg.Addrs,
		Auth: ch.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: ch.Settings{
			"async_insert":                 1,
			"wait_for_async_insert":        1,
			"async_insert_busy_timeout_ms": 1000,
			"async_insert_max_data_size":   10000000,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    32,
		MaxIdleConns:    8,
		ConnMaxLifetime: 10 * time.Minute,
		Compression:     &ch.Compression{Method: ch.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		// ch.Open already handed back a live pool. Returning without closing it
		// leaks connections on every failed start — which is exactly the path a
		// crash-looping pod takes.
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return conn, nil
}

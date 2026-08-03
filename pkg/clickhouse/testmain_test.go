package clickhouse_test

import (
	"context"
	"testing"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/testcontainers/testcontainers-go"
	tcch "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// startClickHouse boots a throwaway ClickHouse and returns a live connection.
// One container per test: these are fast, and shared state between schema tests
// is how you get a green suite that passes only in the order you happened to
// run it.
func startClickHouse(t *testing.T) driver.Conn {
	t.Helper()
	ctx := context.Background()

	const password = "test"
	container, err := tcch.Run(ctx, "clickhouse/clickhouse-server:24.8-alpine",
		tcch.WithUsername("default"),
		tcch.WithPassword(password),
		tcch.WithDatabase("tracking_test"),
	)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	host, err := container.ConnectionHost(ctx)
	if err != nil {
		t.Fatalf("connection host: %v", err)
	}

	conn, err := ch.Open(&ch.Options{
		Addr: []string{host},
		Auth: ch.Auth{Database: "tracking_test", Username: "default", Password: password},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return conn
}

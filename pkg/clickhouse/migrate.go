// Package clickhouse holds the shared ClickHouse pool, migration runner, and
// insert path. Forward-only migrations: there is no down-migration story,
// because rolling a schema backwards on a 2.6B-row/month table is a fantasy.
package clickhouse

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed all:sql
var embedded embed.FS

// Migrations is the embedded migration set. It is a package-level var so tests
// can substitute a fixture FS.
var Migrations fs.FS = mustSub(embedded, "sql")

func mustSub(f embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

// schemaMigrationsDDL tracks which migration files have been applied.
//
// Relying on CREATE TABLE IF NOT EXISTS for idempotency only works while every
// migration happens to be idempotent. The first ALTER, backfill, or data
// migration breaks that silently — re-running it either errors or, worse,
// applies twice. A ledger costs one small table and removes the whole class.
const schemaMigrationsDDL = `
CREATE TABLE IF NOT EXISTS schema_migrations
(
    name       String,
    applied_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY name`

// Migrate applies each unapplied .sql file in dir, in lexical order, exactly
// once. Each file may hold multiple statements separated by ";".
//
// A file is recorded only after all of its statements succeed, so a partially
// applied file is retried on the next run rather than being skipped. That
// makes each individual migration's own statements the thing that must be
// safe to re-run — ClickHouse has no transactional DDL to lean on.
func Migrate(ctx context.Context, conn driver.Conn, dir fs.FS) error {
	if err := conn.Exec(ctx, schemaMigrationsDDL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := appliedMigrations(ctx, conn)
	if err != nil {
		return err
	}

	entries, err := fs.ReadDir(dir, ".")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		if _, done := applied[name]; done {
			continue
		}

		body, err := fs.ReadFile(dir, name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		for _, stmt := range strings.Split(string(body), ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}

		if err := conn.Exec(ctx,
			`INSERT INTO schema_migrations (name) VALUES (?)`, name); err != nil {
			return fmt.Errorf("record %s: %w", name, err)
		}
	}
	return nil
}

func appliedMigrations(ctx context.Context, conn driver.Conn) (map[string]struct{}, error) {
	rows, err := conn.Query(ctx, `SELECT DISTINCT name FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	applied := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan schema_migrations: %w", err)
		}
		applied[name] = struct{}{}
	}
	return applied, rows.Err()
}

.PHONY: gen gen-check lint test check-boundaries sync-migrations

gen:
	buf generate

# CI gate: generated code must match the protos exactly.
#
# The second line is not redundant: `git diff` says nothing about untracked
# files, so a brand-new generated file would sit uncommitted while this check
# passed.
gen-check: gen
	git diff --exit-code -- gen/
	test -z "$$(git ls-files --others --exclude-standard -- gen/)"

lint:
	buf lint
	@for d in $$(go list -m -f '{{.Dir}}'); do (cd "$$d" && golangci-lint run ./...) || exit 1; done

check-boundaries:
	go run ./tools/checkboundaries

# The repo root is not itself a Go module, only a go.work pointing at member
# modules, so `./...` (and `all`, which pulls in every dependency of every
# workspace module) can't be run from here. Loop over each workspace
# module's own directory instead — scoped to just this repo's code.
test:
	@for d in $$(go list -m -f '{{.Dir}}'); do (cd "$$d" && go test ./...) || exit 1; done

# The embed in pkg/clickhouse/migrate.go reads from pkg/clickhouse/sql, copied
# from migrations/clickhouse. This rule keeps the copy from going stale.
sync-migrations:
	rm -rf pkg/clickhouse/sql && mkdir -p pkg/clickhouse/sql
	cp migrations/clickhouse/*.sql pkg/clickhouse/sql/
	git diff --exit-code -- pkg/clickhouse/sql

.PHONY: gen gen-check lint test check-boundaries

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
	golangci-lint run ./...

check-boundaries:
	go run ./tools/checkboundaries

test:
	go test ./...

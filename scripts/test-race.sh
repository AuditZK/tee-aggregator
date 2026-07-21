#!/usr/bin/env bash
# test-race.sh — run the test suite under the race detector.
#
# The race detector is ThreadSanitizer, a C runtime, so -race needs cgo and a C
# toolchain. A Windows dev box has neither, and `go test -race` there dies with
# `cgo: C compiler "gcc" not found` before running a single test. This runs the
# same detector every other machine uses inside the Go image, without
# installing a compiler on the host.
#
# Usage:
#   scripts/test-race.sh                       # ./internal/... ./pkg/...
#   scripts/test-race.sh ./internal/connector/ # one package
#   COUNT=50 scripts/test-race.sh ./internal/connector/   # stress a flake
#
# Extra `go test` flags pass through:
#   scripts/test-race.sh ./internal/connector/ -run TestIG -v
#
# On Linux with gcc present, skip this and run the command directly:
#   CGO_ENABLED=1 go test -race ./internal/... -count=1
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_IMAGE="${GO_IMAGE:-golang:1.26}"
COUNT="${COUNT:-1}"

if ! docker info >/dev/null 2>&1; then
	echo "docker is not reachable — start Docker Desktop, or run on a host with gcc:" >&2
	echo "  CGO_ENABLED=1 go test -race ./internal/... -count=1" >&2
	exit 1
fi

if [ "$#" -eq 0 ]; then
	set -- ./internal/... ./pkg/...
fi

# MSYS_NO_PATHCONV stops Git Bash rewriting /src into a Windows path.
# buildvcs is off because the mounted .git belongs to another uid inside the
# container, which otherwise fails the build outright.
MSYS_NO_PATHCONV=1 exec docker run --rm \
	-v "${REPO_ROOT}:/src" \
	-v zk-gomod:/go/pkg/mod \
	-w /src \
	-e CGO_ENABLED=1 \
	-e GOFLAGS=-buildvcs=false \
	"${GO_IMAGE}" \
	go test -race -count="${COUNT}" "$@"

#!/usr/bin/env bash
# test-db.sh — run the test suite with the database-backed tests enabled.
#
# Tests built on internal/testdb skip themselves unless TEST_DATABASE_URL is
# set. This starts a throwaway postgres:15-alpine (the version production
# runs), points TEST_DATABASE_URL at it, runs `go test`, and removes it.
#
# Usage:
#   scripts/test-db.sh                         # ./internal/... ./pkg/...
#   scripts/test-db.sh ./internal/service/ -run TestDB -v
#   RACE=1 scripts/test-db.sh ./internal/service/ -run TestDB
#
# RACE=1 runs the tests under the race detector inside the Go image, for the
# same reason as scripts/test-race.sh: -race needs a C toolchain.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_IMAGE="${PG_IMAGE:-postgres:15-alpine}"
GO_IMAGE="${GO_IMAGE:-golang:1.26}"

if ! docker info >/dev/null 2>&1; then
	echo "docker is not reachable — start Docker Desktop" >&2
	exit 1
fi

if [ "$#" -eq 0 ]; then
	set -- ./internal/... ./pkg/...
fi

name="zka-testdb-$$"
net="zka-testdb-net-$$"
docker network create "$net" >/dev/null
trap 'docker rm -f "$name" >/dev/null 2>&1 || true; docker network rm "$net" >/dev/null 2>&1 || true' EXIT
docker run -d --rm --name "$name" --network "$net" -e POSTGRES_PASSWORD=test -p 127.0.0.1::5432 "$PG_IMAGE" >/dev/null

# The image's init phase runs a server with TCP off, so a TCP probe only
# succeeds once the real server is up.
for _ in $(seq 1 60); do
	if docker exec "$name" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then
		break
	fi
	sleep 1
done

if [ "${RACE:-0}" = "1" ]; then
	MSYS_NO_PATHCONV=1 docker run --rm --network "$net" \
		-v "${REPO_ROOT}:/src" \
		-v zk-gomod:/go/pkg/mod \
		-w /src \
		-e CGO_ENABLED=1 \
		-e GOFLAGS=-buildvcs=false \
		-e TEST_DATABASE_URL="postgres://postgres:test@${name}:5432/postgres?sslmode=disable" \
		"${GO_IMAGE}" \
		go test -race -count=1 "$@"
	exit $?
fi

port="$(docker port "$name" 5432/tcp | head -n1 | sed 's/.*://')"
export TEST_DATABASE_URL="postgres://postgres:test@127.0.0.1:${port}/postgres?sslmode=disable"

go test -count=1 "$@"

#!/usr/bin/env bash
# build-enclave.sh — the one build of the enclave binary, used for production
# deploys and by anyone checking that production runs what main says.
#
# Two flags make the result depend on the source and not on the machine:
#   -trimpath        no build-host paths in the binary
#   -buildvcs=false  no git stamps; the production image builds from a context
#                    without .git, and on a checkout a single untracked file
#                    flips vcs.modified and changes the hash of identical code
# go.mod pins the toolchain, so `go` fetches that exact version wherever this
# runs, and the host OS does not matter for a linux/amd64 target.
#
# It refuses a tree with uncommitted or untracked changes. Not because the
# hash would move, it would not, but because a binary built from them matches
# no commit anyone can check out, and then the hash proves nothing.
#
# Usage:
#   scripts/build-enclave.sh                 # -> enclave_linux, prints commit + sha256
#   scripts/build-enclave.sh --allow-dirty   # local experiments, never for a deploy
set -euo pipefail

cd "$(dirname "$0")/.."

allow_dirty=false
if [[ "${1:-}" == "--allow-dirty" ]]; then
  allow_dirty=true
fi

if [[ "$allow_dirty" == false ]] && [[ -n "$(git status --porcelain)" ]]; then
  echo "refusing to build from a dirty tree; commit or stash first, or pass --allow-dirty:" >&2
  git status --short >&2
  exit 1
fi

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath -buildvcs=false -ldflags="-w -s" \
  -o enclave_linux ./cmd/enclave/

suffix=""
if [[ "$allow_dirty" == true ]]; then
  suffix="  (dirty tree: this hash matches no commit)"
fi
echo "commit  $(git rev-parse HEAD)${suffix}"
echo "sha256  $(sha256sum enclave_linux | cut -d' ' -f1)"

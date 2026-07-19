# syntax=docker/dockerfile:1.7

# Go build stage.
# VULN-001: pinned to 1.26.4-alpine (matches go.mod) so stdlib CVEs through
# 1.26.4 — incl. GO-2026-5037 (crypto/x509) and GO-2026-5039 (net/textproto)
# reachable at this commit — are picked up deterministically. Pinned by digest
# (SUP-02); bump it when bumping the tag via
# `docker buildx imagetools inspect golang:1.26.4-alpine`.
FROM golang:1.26.5-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Copy go mod files
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# Copy source
COPY cmd ./cmd
COPY internal ./internal
COPY api ./api
COPY migrations ./migrations

# Build with -trimpath for reproducible builds
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-w -s" \
    -o /enclave \
    ./cmd/enclave

# Runtime stage.
# Pinned by digest (SUP-02). 3.20 receives security updates for the life of the
# branch; bump the digest for the latest patch (or pin the exact patch tag, e.g.
# 3.20.3, for strict reproducibility).
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata wget

# Copy Go binary
COPY --from=builder /enclave /app/enclave

# Copy migrations
COPY --from=builder /app/migrations /app/migrations

# Create non-root user and cache directory for VCEK certs
# NOTE: For SEV-SNP /dev/sev-guest access, you may need to run as root
# or configure device permissions. Comment out USER line if needed.
RUN adduser -D -g '' enclave && \
    mkdir -p /var/cache/enclave/certs && \
    chown enclave:enclave /var/cache/enclave/certs
USER enclave

# Expose ports: REST, gRPC, Log Stream, Metrics
EXPOSE 8080 50051 50052 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-check-certificate -qO/dev/null https://localhost:8080/health || exit 1

# Run
ENTRYPOINT ["/app/enclave"]

# syntax=docker/dockerfile:1.7

# Go build stage.
# VULN-001: pinned to 1.26.2-alpine so stdlib CVEs GO-2026-4947/4946/4870/
# 4866/4865/4603/4602/4601/4600/4599 (all fixed in 1.26.1 / 1.26.2) are
# picked up deterministically. For fully reproducible builds the next step
# is a digest pin (`golang:1.26.2-alpine@sha256:<digest>`); do that once CI
# resolves the upstream digest via `docker pull + inspect` and mirrors the
# result here. Same applies to the alpine runtime tag below.
FROM golang:1.26.2-alpine AS builder

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
# Same note as the builder: pin by digest in CI. 3.20 receives security updates
# for the life of the branch; a future audit should also pin the exact patch
# version (e.g. 3.20.3) for strict reproducibility.
FROM alpine:3.20

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

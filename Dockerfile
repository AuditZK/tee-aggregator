# Go build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build with -trimpath for reproducible builds
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-w -s" \
    -o /enclave \
    ./cmd/enclave

# Runtime stage
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
    CMD wget -qO/dev/null http://localhost:8080/health || exit 1

# Run
ENTRYPOINT ["/app/enclave"]

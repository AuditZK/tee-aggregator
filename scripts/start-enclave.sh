#!/bin/bash
#############################################################################
# Start Go Enclave with GCP Metadata
#
# Reads secrets from GCP VM instance metadata and starts the enclave.
# This is the ONLY way to start the enclave in production - no .env files.
#
# Prerequisites:
#   1. GCP VM with metadata configured (see below)
#   2. Docker installed
#   3. Pre-built enclave-go:prod image
#
# GCP Metadata keys required:
#   - database-url        (PostgreSQL connection string)
#   - encryption-key      (AES-256 key, 64 hex chars)
#   - dek-override        (DEK from TS enclave migration, 64 hex chars)
#   - benchmark-service-url (VPS benchmark API URL)
#
# Optional metadata keys:
#   - log-level, ctrader-client-id, ctrader-client-secret
#   - log-server-api-key, enclave-domain, exchange-http-proxy
#   - MT_BRIDGE_URL, MT_BRIDGE_HMAC_SECRET
#
# Usage:
#   ./scripts/start-enclave.sh [--restart] [--build]
#############################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

METADATA_URL="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
METADATA_HEADER="Metadata-Flavor: Google"
CONTAINER_NAME="enclave_go_prod"
IMAGE_NAME="enclave-go:prod"
NETWORK="zero-knowledge-aggregator_enclave-network"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Track Record Enclave (Go) - Production${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Check if running on GCP
if ! curl -sf -H "$METADATA_HEADER" "$METADATA_URL/" > /dev/null 2>&1; then
  echo -e "${RED}ERROR: Not running on GCP or metadata server not accessible${NC}"
  echo -e "${RED}This script must be run on a GCP VM with metadata configured${NC}"
  exit 1
fi

echo -e "${GREEN}Loading secrets from GCP metadata...${NC}"

# Function to get metadata value
get_metadata() {
  local key="$1"
  local value
  value=$(curl -sf -H "$METADATA_HEADER" "$METADATA_URL/$key" 2>/dev/null) || echo ""
  echo "$value"
}

# Load required secrets
DATABASE_URL=$(get_metadata "database-url")
ENCRYPTION_KEY=$(get_metadata "encryption-key")
DEK_OVERRIDE=$(get_metadata "dek-override")

# Load optional config
BENCHMARK_SERVICE_URL=$(get_metadata "benchmark-service-url")
LOG_LEVEL=$(get_metadata "log-level")
CTRADER_CLIENT_ID=$(get_metadata "ctrader-client-id")
CTRADER_CLIENT_SECRET=$(get_metadata "ctrader-client-secret")
LOG_SERVER_API_KEY=$(get_metadata "log-server-api-key")
ENCLAVE_DOMAIN=$(get_metadata "enclave-domain")
EXCHANGE_HTTP_PROXY=$(get_metadata "exchange-http-proxy")
MT_BRIDGE_URL=$(get_metadata "MT_BRIDGE_URL")
MT_BRIDGE_HMAC_SECRET=$(get_metadata "MT_BRIDGE_HMAC_SECRET")

# Validate required secrets
MISSING=""
[[ -z "$DATABASE_URL" ]] && MISSING="$MISSING database-url"
[[ -z "$ENCRYPTION_KEY" ]] && MISSING="$MISSING encryption-key"

if [[ -n "$MISSING" ]]; then
  echo -e "${RED}ERROR: Missing required GCP metadata keys:${MISSING}${NC}"
  echo ""
  echo "Add them with:"
  echo "  gcloud compute instances add-metadata \$(hostname) \\"
  echo "    --zone=\$(curl -sH 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone | cut -d'/' -f4) \\"
  echo "    --metadata=database-url=postgresql://... \\"
  echo "    --metadata=encryption-key=... \\"
  echo "    --metadata=dek-override=..."
  exit 1
fi

# Determine TLS cert paths
TLS_CERT_PATH="/etc/letsencrypt/live/${ENCLAVE_DOMAIN:-enclave.auditzk.com}/fullchain.pem"
TLS_KEY_PATH="/etc/letsencrypt/live/${ENCLAVE_DOMAIN:-enclave.auditzk.com}/privkey.pem"

echo -e "${GREEN}Loaded from GCP metadata:${NC}"
echo "  - DATABASE_URL: postgresql://...@[redacted]"
echo "  - ENCRYPTION_KEY: [redacted, ${#ENCRYPTION_KEY} chars]"
echo "  - DEK_OVERRIDE: [redacted, ${#DEK_OVERRIDE} chars]"
echo "  - BENCHMARK_SERVICE_URL: ${BENCHMARK_SERVICE_URL:-<not set>}"
echo "  - ENCLAVE_DOMAIN: ${ENCLAVE_DOMAIN:-<not set>}"
echo "  - LOG_LEVEL: ${LOG_LEVEL:-info}"
echo "  - TLS_CERT_PATH: ${TLS_CERT_PATH}"
echo ""

# Parse flags
RESTART_FLAG=""
BUILD_FLAG=""
for arg in "$@"; do
  case $arg in
    --restart) RESTART_FLAG="true" ;;
    --build) BUILD_FLAG="true" ;;
  esac
done

# Stop existing container if restart
if [[ "$RESTART_FLAG" == "true" ]] || docker ps -q -f name="$CONTAINER_NAME" | grep -q .; then
  echo -e "${YELLOW}Stopping existing container...${NC}"
  docker stop "$CONTAINER_NAME" 2>/dev/null || true
  docker rm "$CONTAINER_NAME" 2>/dev/null || true
fi

# Build if requested
if [[ "$BUILD_FLAG" == "true" ]]; then
  echo -e "${GREEN}Building enclave image...${NC}"
  cd "$PROJECT_DIR"
  docker build --no-cache -f Dockerfile.prebuilt -t "$IMAGE_NAME" .
fi

# Start enclave with all env vars from metadata (NO .env file)
echo -e "${GREEN}Starting enclave...${NC}"
docker run -d \
  --name "$CONTAINER_NAME" \
  --network "$NETWORK" \
  --user root \
  --device /dev/sev-guest:/dev/sev-guest \
  -v ~/tee-aggregator-go/certs:/etc/enclave:ro \
  -v zero-knowledge-aggregator_vcek_cache:/var/cache/enclave/certs \
  -v /etc/letsencrypt:/etc/letsencrypt:ro \
  -p 3050:3050 \
  -p 50051:50051 \
  -p 50052:50052 \
  --restart unless-stopped \
  -e ENV=production \
  -e DATABASE_URL="$DATABASE_URL" \
  -e ENCRYPTION_KEY="$ENCRYPTION_KEY" \
  -e DEK_OVERRIDE="$DEK_OVERRIDE" \
  -e GRPC_PORT=50051 \
  -e GRPC_INSECURE=false \
  -e REST_PORT=3050 \
  -e LOG_LEVEL="${LOG_LEVEL:-info}" \
  -e AUTO_MIGRATE=false \
  -e ENABLE_DAILY_SYNC=true \
  -e TLS_CERT_PATH="$TLS_CERT_PATH" \
  -e TLS_KEY_PATH="$TLS_KEY_PATH" \
  -e TLS_CA_CERT=/etc/enclave/ca.crt \
  -e TLS_SERVER_CERT=/etc/enclave/server.crt \
  -e TLS_SERVER_KEY=/etc/enclave/server.key \
  -e BENCHMARK_SERVICE_URL="${BENCHMARK_SERVICE_URL:-}" \
  -e EXCHANGE_HTTP_PROXY="${EXCHANGE_HTTP_PROXY:-}" \
  -e CTRADER_CLIENT_ID="${CTRADER_CLIENT_ID:-}" \
  -e CTRADER_CLIENT_SECRET="${CTRADER_CLIENT_SECRET:-}" \
  -e MT_BRIDGE_URL="${MT_BRIDGE_URL:-}" \
  -e MT_BRIDGE_HMAC_SECRET="${MT_BRIDGE_HMAC_SECRET:-}" \
  -e LOG_SERVER_API_KEY="${LOG_SERVER_API_KEY:-}" \
  "$IMAGE_NAME"

echo ""
echo -e "${GREEN}Waiting for enclave to start...${NC}"
sleep 5

# Check status
if docker ps | grep -q "$CONTAINER_NAME"; then
  echo -e "${GREEN}========================================${NC}"
  echo -e "${GREEN}Enclave (Go) started successfully!${NC}"
  echo -e "${GREEN}========================================${NC}"
  echo ""
  docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep "$CONTAINER_NAME"
  echo ""
  echo "Health: curl https://${ENCLAVE_DOMAIN:-localhost}:3050/health"
  echo "Logs:   docker logs -f $CONTAINER_NAME"
  echo ""
  echo -e "${GREEN}No .env file used - all secrets from GCP metadata${NC}"
else
  echo -e "${RED}ERROR: Enclave failed to start${NC}"
  echo "Check logs: docker logs $CONTAINER_NAME"
  exit 1
fi

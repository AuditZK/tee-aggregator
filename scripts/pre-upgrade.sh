#!/usr/bin/env bash
# pre-upgrade.sh — run this once before a planned enclave upgrade.
#
# Fetches the running predecessor's TLS fingerprint and writes
# HANDOFF_PEER_TLS_FINGERPRINT into .env.production so the successor
# enclave can pin the predecessor's cert during the B2 handoff.
#
# Usage:
#   scripts/pre-upgrade.sh [ENCLAVE_URL]
#
# ENCLAVE_URL defaults to https://localhost:8080 when not supplied.
# The script must run from the repo root where .env.production lives.
#
# After this script completes, proceed with the upgrade:
#   docker compose -f docker-compose.production.yml up -d --force-recreate enclave
#
# Once the successor is healthy, clear both handoff vars from .env.production:
#   HANDOFF_PEER_URL=
#   HANDOFF_PEER_TLS_FINGERPRINT=
set -euo pipefail

ENCLAVE_URL="${1:-https://localhost:8080}"
ENV_FILE=".env.production"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: $ENV_FILE not found — run from the repo root." >&2
    exit 1
fi

echo "Fetching TLS fingerprint from ${ENCLAVE_URL}/api/v1/tls/fingerprint ..."

# -k because we're fetching the fingerprint OF the self-signed cert —
# chain validation is impossible here by design. The fingerprint itself
# is what we're committing to trust.
RESPONSE=$(curl -fsSk "${ENCLAVE_URL}/api/v1/tls/fingerprint")
FINGERPRINT=$(echo "$RESPONSE" | grep -o '"fingerprint":"[^"]*"' | cut -d'"' -f4)

if [[ -z "$FINGERPRINT" ]]; then
    echo "ERROR: could not parse fingerprint from response:" >&2
    echo "$RESPONSE" >&2
    exit 1
fi

echo "Fingerprint: ${FINGERPRINT}"

# Write or update HANDOFF_PEER_TLS_FINGERPRINT in .env.production.
if grep -q "^HANDOFF_PEER_TLS_FINGERPRINT=" "$ENV_FILE"; then
    sed -i "s|^HANDOFF_PEER_TLS_FINGERPRINT=.*|HANDOFF_PEER_TLS_FINGERPRINT=${FINGERPRINT}|" "$ENV_FILE"
else
    echo "HANDOFF_PEER_TLS_FINGERPRINT=${FINGERPRINT}" >> "$ENV_FILE"
fi

echo "Written HANDOFF_PEER_TLS_FINGERPRINT to ${ENV_FILE}."
echo "Verify HANDOFF_PEER_URL is also set, then run the upgrade."

#!/bin/bash
#############################################################################
# Regenerate the gRPC server certificate with proper SANs.
#
# Why this exists:
#   The original server.crt was issued with CN=enclave and no SAN. Modern
#   TLS clients (Node.js gRPC) reject the connection with
#   ERR_TLS_CERT_ALTNAME_INVALID when the host they dial doesn't appear in
#   the SAN extension. As a workaround the gateway/report-service composes
#   pin "enclave" -> <TEE IP> in /etc/hosts. This script lets us drop that
#   hack by reissuing the cert with the SAN entries that real callers use.
#
# What it does:
#   1. Reads the existing CA (ca.crt + ca.key) — DOES NOT regenerate it.
#      The CA must keep the same identity so existing clients (gateway,
#      report-service) still trust the new server cert without any update
#      on their side.
#   2. Generates a fresh server.key + server.csr.
#   3. Signs the CSR with the CA, embedding the SAN extension.
#   4. Writes the new server.{crt,key} into the certs directory.
#   5. Prints instructions for restarting the enclave and removing the
#      extra_hosts hack from track_record_site/docker-compose.production.yml.
#
# Where to run it:
#   On the TEE host, in the directory that contains ca.crt + ca.key
#   (typically ~/tee-aggregator/certs/). The CA private key never leaves
#   that machine.
#
# Usage:
#   cd ~/tee-aggregator
#   bash scripts/regen-server-cert.sh
#
# Idempotent: re-running just regenerates the server cert. The CA is
# untouched.
#############################################################################

set -euo pipefail

CERTS_DIR="${CERTS_DIR:-./certs}"
CA_CRT="$CERTS_DIR/ca.crt"
CA_KEY="$CERTS_DIR/ca.key"

# Hostnames + IPs that legitimately address the gRPC server. Any caller
# connecting to one of these will pass TLS hostname validation.
SAN_DNS=(
  "enclave"
  "enclave.auditzk.com"
  "localhost"
)
SAN_IP=(
  "127.0.0.1"
  "34.10.197.68"
)

# Validity window. 13 months keeps us in the Let's Encrypt-style cadence
# without risking a sub-1-year accidental expiry.
DAYS=395

if [[ ! -f "$CA_CRT" || ! -f "$CA_KEY" ]]; then
  echo "ERROR: $CA_CRT or $CA_KEY missing." >&2
  echo "Run this script from the directory containing the existing CA." >&2
  exit 1
fi

# Build the SAN string for openssl req -addext.
san_str="subjectAltName="
sep=""
for d in "${SAN_DNS[@]}"; do
  san_str+="${sep}DNS:$d"
  sep=","
done
for i in "${SAN_IP[@]}"; do
  san_str+="${sep}IP:$i"
done

echo "Regenerating server cert in $CERTS_DIR with SANs: $san_str"

# Backup current server cert/key so we can roll back if something breaks.
ts=$(date +%Y%m%d-%H%M%S)
if [[ -f "$CERTS_DIR/server.crt" ]]; then
  cp "$CERTS_DIR/server.crt" "$CERTS_DIR/server.crt.backup-$ts"
fi
if [[ -f "$CERTS_DIR/server.key" ]]; then
  cp "$CERTS_DIR/server.key" "$CERTS_DIR/server.key.backup-$ts"
fi

# 1. New server private key.
openssl genrsa -out "$CERTS_DIR/server.key" 2048

# 2. CSR with CN=enclave (kept stable so gateway-side allowlists still match
#    if anyone added one; the SAN above is what TLS clients actually verify).
openssl req -new \
  -key "$CERTS_DIR/server.key" \
  -out "$CERTS_DIR/server.csr" \
  -subj "/CN=enclave/O=Track Record/C=FR" \
  -addext "$san_str"

# 3. Sign the CSR with the existing CA, copying the SAN through to the cert.
ext_file=$(mktemp)
trap 'rm -f "$ext_file"' EXIT
cat > "$ext_file" <<EOF
$san_str
extendedKeyUsage=serverAuth
keyUsage=digitalSignature,keyEncipherment
EOF

openssl x509 -req \
  -in "$CERTS_DIR/server.csr" \
  -CA "$CA_CRT" \
  -CAkey "$CA_KEY" \
  -CAcreateserial \
  -out "$CERTS_DIR/server.crt" \
  -days "$DAYS" \
  -sha256 \
  -extfile "$ext_file"

# 4. Lock down the private key. The container runs as root in production,
#    so 600 is enough; tighten to 400 if you ever drop privileges.
chmod 600 "$CERTS_DIR/server.key"
chmod 644 "$CERTS_DIR/server.crt"

echo
echo "=== New server cert ==="
openssl x509 -in "$CERTS_DIR/server.crt" -noout -subject -issuer -serial -dates
echo
openssl x509 -in "$CERTS_DIR/server.crt" -noout -ext subjectAltName

echo
echo "=== Next steps ==="
echo "1. Restart the enclave so it reloads the new server.crt:"
echo "   docker compose --env-file .env.production -f docker-compose.production.yml up -d --no-build --force-recreate enclave"
echo
echo "2. Verify the gateway connects with hostname enclave.auditzk.com (no /etc/hosts hack needed):"
echo "   docker logs auditzk_gateway --tail 20 | grep -iE 'connected|tls|enclave'"
echo
echo "3. Once confirmed, drop the extra_hosts blocks from"
echo "   track_record_site/docker-compose.production.yml on both gateway-service"
echo "   and report-service, and switch ENCLAVE_HOST back to enclave.auditzk.com."

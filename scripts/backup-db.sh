#!/bin/bash
#############################################################################
# Daily Backup - Enclave PostgreSQL Database
#
# Creates a compressed pg_dump of the enclave DB and rotates old backups.
# Runs via crontab at 01:00 UTC (after the 00:00 UTC daily sync).
#
# Setup:
#   crontab -e
#   0 1 * * * /home/auditzk/trackrecord/zero-knowledge-aggregator-go/scripts/backup-db.sh >> /home/auditzk/trackrecord/backups/backup.log 2>&1
#
# Restore:
#   gunzip -c backup_20260405_010000.sql.gz | docker exec -i auditzk_postgres_enclave psql -U enclave_user -d enclave_db
#############################################################################

set -euo pipefail

# Configuration
BACKUP_DIR="${HOME}/trackrecord/backups"
CONTAINER="auditzk_postgres_enclave"
DB_NAME="enclave_db"
DB_USER="enclave_user"
RETENTION_DAYS=30
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/backup_${TIMESTAMP}.sql.gz"

# Create backup directory
mkdir -p "$BACKUP_DIR"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Starting backup..."

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[ERROR] Container ${CONTAINER} is not running"
  exit 1
fi

# Create compressed backup
docker exec "$CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" \
  --no-owner --no-privileges --clean --if-exists \
  | gzip > "$BACKUP_FILE"

# Verify backup is not empty
BACKUP_SIZE=$(stat -c%s "$BACKUP_FILE" 2>/dev/null || stat -f%z "$BACKUP_FILE" 2>/dev/null)
if [ "$BACKUP_SIZE" -lt 1000 ]; then
  echo "[ERROR] Backup file too small (${BACKUP_SIZE} bytes), something went wrong"
  rm -f "$BACKUP_FILE"
  exit 1
fi

echo "[OK] Backup created: ${BACKUP_FILE} ($(du -h "$BACKUP_FILE" | cut -f1))"

# Count tables and rows for verification
TABLE_COUNT=$(docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -t -c \
  "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';" | tr -d ' ')
SNAPSHOT_COUNT=$(docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -t -c \
  "SELECT count(*) FROM snapshot_data;" 2>/dev/null | tr -d ' ' || echo "?")
USER_COUNT=$(docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -t -c \
  "SELECT count(*) FROM users;" 2>/dev/null | tr -d ' ' || echo "?")

echo "[OK] Tables: ${TABLE_COUNT}, Users: ${USER_COUNT}, Snapshots: ${SNAPSHOT_COUNT}"

# Rotate old backups
DELETED=0
find "$BACKUP_DIR" -name "backup_*.sql.gz" -mtime +"$RETENTION_DAYS" -delete -print | while read f; do
  DELETED=$((DELETED + 1))
done

TOTAL_BACKUPS=$(find "$BACKUP_DIR" -name "backup_*.sql.gz" | wc -l)
TOTAL_SIZE=$(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1)

echo "[OK] Retention: ${RETENTION_DAYS} days, Total backups: ${TOTAL_BACKUPS}, Total size: ${TOTAL_SIZE}"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Backup complete"
echo "---"

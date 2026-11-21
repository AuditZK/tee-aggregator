-- Add credentials_hash column for deduplication (prevents duplicate connections with same credentials).
ALTER TABLE exchange_connections ADD COLUMN IF NOT EXISTS credentials_hash VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_exchange_connections_credentials_hash
    ON exchange_connections(credentials_hash) WHERE credentials_hash IS NOT NULL;

-- Add sync_interval_minutes column for per-connection sync interval.
ALTER TABLE exchange_connections ADD COLUMN IF NOT EXISTS sync_interval_minutes INT DEFAULT 1440;

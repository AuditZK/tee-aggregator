-- Migration 007: Sync rate limit logs
CREATE TABLE IF NOT EXISTS sync_rate_limit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_uid VARCHAR(255) NOT NULL,
    exchange VARCHAR(100) NOT NULL,
    label VARCHAR(255) NOT NULL DEFAULT '',
    last_sync_time TIMESTAMPTZ NOT NULL,
    sync_count INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_rate_limit_unique
    ON sync_rate_limit_logs(user_uid, exchange, label);

-- Migration 005: Sync status tracking
CREATE TABLE IF NOT EXISTS sync_statuses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_uid VARCHAR(255) NOT NULL,
    exchange VARCHAR(100) NOT NULL,
    label VARCHAR(255) NOT NULL DEFAULT '',
    last_sync_time TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    total_trades INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_statuses_unique
    ON sync_statuses(user_uid, exchange, label);

CREATE INDEX IF NOT EXISTS idx_sync_statuses_status
    ON sync_statuses(status);

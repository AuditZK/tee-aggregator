-- Snapshot data table for daily equity snapshots
CREATE TABLE IF NOT EXISTS snapshot_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_uid VARCHAR(255) NOT NULL,
    exchange VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,  -- 00:00 UTC of snapshot day

    -- Core metrics
    total_equity DECIMAL(20, 8) NOT NULL,
    realized_balance DECIMAL(20, 8) NOT NULL,
    unrealized_pnl DECIMAL(20, 8) DEFAULT 0,

    -- Cash flow
    deposits DECIMAL(20, 8) DEFAULT 0,
    withdrawals DECIMAL(20, 8) DEFAULT 0,

    -- Trade aggregates (individual trades never stored)
    total_trades INTEGER DEFAULT 0,
    total_volume DECIMAL(20, 8) DEFAULT 0,
    total_fees DECIMAL(20, 8) DEFAULT 0,

    -- Market breakdown (JSON)
    breakdown_by_market JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(user_uid, exchange, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_data_user_uid ON snapshot_data(user_uid);
CREATE INDEX IF NOT EXISTS idx_snapshot_data_timestamp ON snapshot_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshot_data_user_time ON snapshot_data(user_uid, timestamp);

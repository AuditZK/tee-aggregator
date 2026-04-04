-- Add label support in snapshots for full exchange+label parity.
-- Existing rows are backfilled with empty label.
ALTER TABLE snapshot_data
    ADD COLUMN IF NOT EXISTS label VARCHAR(255) NOT NULL DEFAULT '';

-- Old uniqueness (user_uid, exchange, timestamp) prevents multi-label snapshots.
ALTER TABLE snapshot_data
    DROP CONSTRAINT IF EXISTS snapshot_data_user_uid_exchange_timestamp_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_data_user_exchange_label_time
    ON snapshot_data(user_uid, exchange, label, timestamp);

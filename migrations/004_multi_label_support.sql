-- Migration 004: Multi-label support
-- Allows multiple accounts per exchange per user (distinguished by label)

-- Drop old partial unique index on (user_uid, exchange)
DROP INDEX IF EXISTS idx_exchange_connections_unique_active;

-- Create new partial unique index on (user_uid, exchange, label)
CREATE UNIQUE INDEX IF NOT EXISTS idx_exchange_connections_unique_active_label
    ON exchange_connections(user_uid, exchange, label) WHERE is_active = true;

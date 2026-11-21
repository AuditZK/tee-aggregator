-- Add exclude_from_report flag for connections that should not be included
-- in certified reports / portfolio-level analytics.
ALTER TABLE exchange_connections
    ADD COLUMN IF NOT EXISTS exclude_from_report BOOLEAN DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_exchange_connections_excluded_active
    ON exchange_connections(user_uid, exchange)
    WHERE is_active = true AND exclude_from_report = true;

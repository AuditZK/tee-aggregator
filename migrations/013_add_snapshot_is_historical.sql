-- Distinguish snapshots reconstructed from broker history (e.g. IBKR Flex
-- daily summaries — equity only, no per-trade detail) from "live" snapshots
-- built from the realtime sync window (balance + 24h trades + cashflows).
--
-- Existing rows are all live → DEFAULT FALSE backfills correctly.
ALTER TABLE snapshot_data
    ADD COLUMN IF NOT EXISTS is_historical BOOLEAN NOT NULL DEFAULT FALSE;

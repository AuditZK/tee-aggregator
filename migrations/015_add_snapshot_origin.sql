-- Record whether a snapshot was reconstructed by the out-of-enclave
-- history-rebuilder service (SEC-001). is_historical alone is insufficient: it
-- is also set for IBKR Flex history, which is rebuilt INSIDE the SEV-SNP
-- perimeter and is legitimately verifiable. The signed report is built from
-- GetVerifiableByUserAndDateRange, which excludes from_external_rebuilder rows,
-- so the report signature only covers enclave-produced data.
--
-- Live snapshots and IBKR Flex history keep the DEFAULT FALSE.
ALTER TABLE snapshot_data
    ADD COLUMN IF NOT EXISTS from_external_rebuilder BOOLEAN NOT NULL DEFAULT FALSE;

-- Backfill rows reconstructed before this column existed. The external
-- rebuilder serves only non-IBKR exchanges (IBKR history is rebuilt in-enclave
-- via Flex), so is_historical + non-IBKR identifies pre-migration rebuilder
-- snapshots. This one-off heuristic is sound for the backfill; ongoing writes
-- set the column explicitly.
UPDATE snapshot_data
SET from_external_rebuilder = TRUE
WHERE is_historical = TRUE AND lower(exchange) <> 'ibkr';

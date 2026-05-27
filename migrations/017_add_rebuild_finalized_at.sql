-- Track whether a connection has been rebuild-finalized via the midnight UTC
-- recalibration pass. NULL = still calibrated to the connect-time live equity
-- (imprecise: endEquity captured at connect-T, not at midnight). After the
-- first daily live sync writes a clean midnight snapshot, the scheduler
-- re-runs reconstructHistory passing the just-written snapshot.totalEquity as
-- EndEquityOverride, so the MTM walk's offset calibration aligns with the
-- exact midnight UTC equity. RebuildFinalizedAt is then set to the run time
-- to prevent re-finalization on subsequent nightly ticks.
--
-- Only applies to connections whose history was rebuilt via the external
-- rebuilder service (HL, Lighter, MEXC). IBKR and other in-enclave history
-- providers (Flex daily equity summaries) don't need recalibration — their
-- per-day equities come directly from the broker statement, not an MTM walk.
ALTER TABLE exchange_connections
    ADD COLUMN IF NOT EXISTS rebuild_finalized_at TIMESTAMPTZ;

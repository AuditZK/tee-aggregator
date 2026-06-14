-- SEC-08: record explicit user opt-in (rebuild_history=true on connection
-- create) durably, so the midnight recalibration pass can scope decrypted-
-- credential egress to consenting connections only.
--
-- The connect-time post-create hook already fires only when the caller opts in,
-- but that signal was never persisted: RecalibrateRebuiltHistories re-decrypted
-- and re-POSTed credentials to the external history-rebuilder for EVERY active
-- external-rebuilder connection (Hyperliquid/...), opt-in or not. Gating that
-- pass on `rebuild_requested_at IS NOT NULL` makes consent durable and
-- fail-safe: connections created before this column exists (NULL) are excluded.
ALTER TABLE exchange_connections
    ADD COLUMN IF NOT EXISTS rebuild_requested_at TIMESTAMPTZ;

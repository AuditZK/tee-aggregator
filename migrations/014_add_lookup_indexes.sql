-- Lookup indexes for two query paths that previously fell back to a
-- sequential scan (audit ENG-007):
--   1. sync_rate_limit_logs purge — CleanupOldLogs filters on last_sync_time.
--   2. snapshot_data latest-by-(user,exchange) lookup with no label —
--      GetLatestByUserExchangeLabel's no-label branch. The unique index from
--      migration 011 leads with (user_uid, exchange, label), so a label-less
--      query cannot use it to drive ORDER BY timestamp DESC.
--
-- Foreign keys (user_uid -> users) are deliberately NOT added here: ADD
-- CONSTRAINT ... FOREIGN KEY validates every existing row and would fail on a
-- database that already holds orphan rows. That belongs in its own migration,
-- after an orphan-cleanup step.

CREATE INDEX IF NOT EXISTS idx_sync_rate_limit_logs_last_sync_time
    ON sync_rate_limit_logs(last_sync_time);

CREATE INDEX IF NOT EXISTS idx_snapshot_data_user_exchange_time
    ON snapshot_data(user_uid, exchange, timestamp);

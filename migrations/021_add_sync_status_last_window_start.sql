-- Where the activity window of the pass stamped last_sync_time began.
--
-- sync_statuses keeps one stamp per connection, and the midnight pass
-- overwrites it. A manual sync rewriting today's row then no longer knows where
-- today's first write started reading, falls back to the previous row's
-- midnight, and re-reads hours that row already booked: a deposit made the
-- morning of a day that was itself rewritten mid-day lands on two rows. The
-- rewrite starts from this column instead, which is where today's first write
-- began; repeated rewrites keep the same start.
--
-- The repository writes it with last_sync_time and keeps it when an attempt
-- keeps the old stamp, so the two always describe the same pass. NULL (every
-- existing row, and the first pass after deploy) falls back to the previous
-- row's midnight.
--
-- The column name is snake_case on both schemas, as for 015. On the production
-- TS/Prisma schema Go migrations are skipped: apply this file by hand. The
-- repository detects the column at first use and works without it.
ALTER TABLE sync_statuses
    ADD COLUMN IF NOT EXISTS last_window_start TIMESTAMPTZ;

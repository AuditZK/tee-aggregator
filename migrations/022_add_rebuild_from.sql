-- The month the user asked a history rebuild to start from, stored on the
-- connection so every later rebuild (midnight recalibration, operator
-- reconstruct) asks the rebuilder for it again.
--
-- It cannot be asked once and forgotten: a rebuild whose answer states no
-- horizon owns the whole rebuilt history and prunes the stored days it does not
-- produce, so a rebuild sent with the default window would delete the older
-- days the first, deeper one wrote.
--
-- NULL (every existing row, and every connection created without a start
-- month) keeps the rebuilder's default window.
--
-- On the production TS/Prisma schema Go migrations are skipped: apply it by
-- hand there as
--   ALTER TABLE exchange_connections ADD COLUMN IF NOT EXISTS "rebuildFrom" DATE;
-- The repository detects the column at first use and works without it.
ALTER TABLE exchange_connections
    ADD COLUMN IF NOT EXISTS rebuild_from DATE;

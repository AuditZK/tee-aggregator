-- Add label support in snapshots for full exchange+label parity.
-- Existing rows are backfilled with empty label.
ALTER TABLE snapshot_data
    ADD COLUMN IF NOT EXISTS label VARCHAR(255) NOT NULL DEFAULT '';

-- Old uniqueness (user_uid, exchange, timestamp) prevents multi-label snapshots.
-- Drop it by column set rather than by the assumed generated name (DATA-02):
-- a DB seeded outside these migrations may have named the constraint
-- differently, where a DROP CONSTRAINT by the guessed name is a silent no-op
-- that leaves the 3-key uniqueness in place and blocks multi-label snapshots.
DO $$
DECLARE
    c text;
BEGIN
    SELECT con.conname INTO c
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    WHERE rel.relname = 'snapshot_data'
      AND con.contype = 'u'
      AND (
        SELECT array_agg(att.attname ORDER BY att.attname)
        FROM unnest(con.conkey) AS k(attnum)
        JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum
      ) = ARRAY['exchange', 'timestamp', 'user_uid']
    LIMIT 1;
    IF c IS NOT NULL THEN
        EXECUTE format('ALTER TABLE snapshot_data DROP CONSTRAINT %I', c);
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_data_user_exchange_label_time
    ON snapshot_data(user_uid, exchange, label, timestamp);

-- Named sets of connections reported together under one account.
--
-- portfolio_members is a journal, not a current list: a member that leaves keeps
-- its row with removed_at set, and coming back opens a new row. What a portfolio
-- has consolidated stays consolidated, and the dates are what a signed report
-- shows of its composition. Only erasing the user removes rows, by the cascade
-- from users, as for the snapshots.
--
-- On the production TS/Prisma schema Go migrations are skipped: apply this file
-- by hand, as was done for 015.
CREATE TABLE IF NOT EXISTS portfolios (
    id          TEXT PRIMARY KEY,
    user_uid    TEXT NOT NULL REFERENCES users (uid) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS portfolios_user_live_name
    ON portfolios (user_uid, lower(name))
    WHERE archived_at IS NULL;

CREATE TABLE IF NOT EXISTS portfolio_members (
    id           BIGSERIAL PRIMARY KEY,
    portfolio_id TEXT NOT NULL REFERENCES portfolios (id) ON DELETE CASCADE,
    exchange     TEXT NOT NULL,
    label        TEXT NOT NULL,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    removed_at   TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS portfolio_members_live
    ON portfolio_members (portfolio_id, lower(exchange), lower(btrim(label)))
    WHERE removed_at IS NULL;

CREATE INDEX IF NOT EXISTS portfolio_members_portfolio
    ON portfolio_members (portfolio_id);

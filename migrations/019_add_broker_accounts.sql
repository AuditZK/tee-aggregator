-- The broker accounts a connection measures, as per-user fingerprints, never
-- the account ids. An IBKR Flex query can be edited to tick other accounts
-- under the same token and query id: pinned, a connection refuses a statement
-- that would splice another account's balance into its stored history, and a
-- second connection cannot measure an account a first one already does.
ALTER TABLE exchange_connections
    ADD COLUMN IF NOT EXISTS broker_accounts TEXT;

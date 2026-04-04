-- Migration 012: Add exchange metadata fields for report exchange_details parity
ALTER TABLE exchange_connections
  ADD COLUMN IF NOT EXISTS kyc_level VARCHAR(50) DEFAULT '';

ALTER TABLE exchange_connections
  ADD COLUMN IF NOT EXISTS is_paper BOOLEAN DEFAULT false;


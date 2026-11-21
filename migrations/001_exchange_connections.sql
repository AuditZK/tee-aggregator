-- Exchange Connections table for storing encrypted API credentials
CREATE TABLE IF NOT EXISTS exchange_connections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_uid VARCHAR(255) NOT NULL,
    exchange VARCHAR(100) NOT NULL,
    label VARCHAR(255),

    -- Encrypted API Key
    encrypted_api_key TEXT NOT NULL,
    api_key_iv TEXT NOT NULL,
    api_key_auth_tag TEXT NOT NULL,

    -- Encrypted API Secret
    encrypted_api_secret TEXT NOT NULL,
    api_secret_iv TEXT NOT NULL,
    api_secret_auth_tag TEXT NOT NULL,

    -- Encrypted Passphrase (optional)
    encrypted_passphrase TEXT,
    passphrase_iv TEXT,
    passphrase_auth_tag TEXT,

    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Partial unique index: one active connection per user+exchange
CREATE UNIQUE INDEX idx_exchange_connections_unique_active
    ON exchange_connections(user_uid, exchange) WHERE is_active = true;

CREATE INDEX idx_exchange_connections_user_uid ON exchange_connections(user_uid);
CREATE INDEX idx_exchange_connections_active ON exchange_connections(user_uid, is_active) WHERE is_active = true;

-- Data Encryption Keys table for hardware-wrapped DEKs
CREATE TABLE IF NOT EXISTS data_encryption_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    encrypted_dek TEXT NOT NULL,
    iv TEXT NOT NULL,
    auth_tag TEXT NOT NULL,
    master_key_id VARCHAR(64) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    rotated_at TIMESTAMPTZ
);

CREATE INDEX idx_dek_active ON data_encryption_keys(is_active) WHERE is_active = true;
CREATE INDEX idx_dek_master_key ON data_encryption_keys(master_key_id);

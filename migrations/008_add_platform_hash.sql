-- Add platform_hash column for zero-knowledge user identification.
-- SHA-256 hash of the platform user ID, enabling reconciliation without exposing the real ID.
ALTER TABLE users ADD COLUMN IF NOT EXISTS platform_hash VARCHAR(64);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_platform_hash
    ON users(platform_hash) WHERE platform_hash IS NOT NULL;

-- Migration 006: Signed reports cache
CREATE TABLE IF NOT EXISTS signed_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id VARCHAR(255) NOT NULL UNIQUE,
    user_uid VARCHAR(255) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    benchmark VARCHAR(100) DEFAULT '',
    report_data JSONB NOT NULL,
    signature TEXT NOT NULL,
    report_hash VARCHAR(64) NOT NULL,
    enclave_version VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signed_reports_user
    ON signed_reports(user_uid);

CREATE UNIQUE INDEX IF NOT EXISTS idx_signed_reports_cache
    ON signed_reports(user_uid, start_date, end_date, benchmark);

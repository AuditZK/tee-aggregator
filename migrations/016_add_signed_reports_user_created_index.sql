-- Index for SignedReportRepo.ListByUser: WHERE user_uid = $1 ORDER BY
-- created_at DESC. The migration-006 index idx_signed_reports_user covers the
-- (user_uid) filter but not the created_at sort, leaving a per-user sort step.
-- This composite covers both; it also supersedes idx_signed_reports_user
-- (left in place — dropping the now-redundant single-column index is a
-- separate decision).
CREATE INDEX IF NOT EXISTS idx_signed_reports_user_created
    ON signed_reports(user_uid, created_at DESC);

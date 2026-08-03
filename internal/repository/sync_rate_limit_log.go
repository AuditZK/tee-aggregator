package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SyncRateLimitLogRepo persists rate-limit events to sync_rate_limit_logs
// (migration 007). The table existed unused since then — the 23h guard was
// presented as the anti-cherry-pick safety but never journaled anything
// (audit 2026-08-01), so throttle behaviour was invisible in production.
type SyncRateLimitLogRepo struct {
	pool *pgxpool.Pool
}

// NewSyncRateLimitLogRepo creates the repository.
func NewSyncRateLimitLogRepo(pool *pgxpool.Pool) *SyncRateLimitLogRepo {
	return &SyncRateLimitLogRepo{pool: pool}
}

// RecordHit upserts one throttle event for a connection: last_sync_time is
// the moment the limit bit (Flex 1018 race loss or stale-statement skip) and
// sync_count accumulates how often it has bitten since the row was created.
func (r *SyncRateLimitLogRepo) RecordHit(ctx context.Context, userUID, exchange, label string, at time.Time) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO sync_rate_limit_logs (user_uid, exchange, label, last_sync_time, sync_count)
		VALUES ($1, $2, $3, $4, 1)
		ON CONFLICT (user_uid, exchange, label)
		DO UPDATE SET
			last_sync_time = EXCLUDED.last_sync_time,
			sync_count = sync_rate_limit_logs.sync_count + 1`,
		userUID, exchange, label, at.UTC())
	return err
}

package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SyncRateLimitLogRepo persists rate-limit events to sync_rate_limit_logs.
// The table existed unused since it was created — the 23h guard was presented
// as the anti-cherry-pick safety but never journaled anything (audit
// 2026-08-01), so throttle behaviour was invisible in production.
//
// Two schemas exist in the wild, mirroring SyncStatusRepo:
//   - TS/Prisma (production): camelCase columns ("userUid", "lastSyncTime",
//     "syncCount"), text id with NO database default (Prisma generates cuid
//     client-side) and NO unique constraint on (userUid, exchange, label) —
//     the table is an append-only event log there, one row per hit.
//   - Go (migration 007): snake_case columns, uuid default, unique index on
//     the triple — upsert with an incrementing counter.
//
// The first deploy assumed the Go schema unconditionally and every prod
// insert failed with 42703 (observed on the 2026-08-04 first run).
type SyncRateLimitLogRepo struct {
	pool           *pgxpool.Pool
	schemaDetected bool
	isTSSchema     bool
}

// NewSyncRateLimitLogRepo creates the repository.
func NewSyncRateLimitLogRepo(pool *pgxpool.Pool) *SyncRateLimitLogRepo {
	return &SyncRateLimitLogRepo{pool: pool}
}

func (r *SyncRateLimitLogRepo) detectSchema(ctx context.Context) {
	if r.schemaDetected {
		return
	}
	var exists bool
	r.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'sync_rate_limit_logs' AND column_name = 'userUid'
		)`).Scan(&exists)
	r.isTSSchema = exists
	r.schemaDetected = true
}

// RecordHit journals one throttle event for a connection (Flex 1018 race loss
// or stale-statement skip) at time `at`.
func (r *SyncRateLimitLogRepo) RecordHit(ctx context.Context, userUID, exchange, label string, at time.Time) error {
	r.detectSchema(ctx)

	if r.isTSSchema {
		// Append-only event log: no unique constraint exists on this schema,
		// so each hit is its own row and syncCount stays 1.
		_, err := r.pool.Exec(ctx, `
			INSERT INTO sync_rate_limit_logs (id, "userUid", exchange, label, "lastSyncTime", "syncCount", "createdAt", "updatedAt")
			VALUES (gen_random_uuid()::text, $1, $2, $3, $4, 1, NOW(), NOW())`,
			userUID, exchange, label, at.UTC())
		return err
	}

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

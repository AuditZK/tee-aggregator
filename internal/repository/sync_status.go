package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SyncStatus represents the sync state for a user/exchange/label combination
type SyncStatus struct {
	ID           string     `json:"id"`
	UserUID      string     `json:"user_uid"`
	Exchange     string     `json:"exchange"`
	Label        string     `json:"label"`
	LastSyncTime *time.Time `json:"last_sync_time,omitempty"`
	// LastWindowStart is where the activity window of the pass stamped
	// LastSyncTime began (migration 021). It travels with LastSyncTime: a
	// write that sets the stamp also sets this, NULL included, so the two
	// always describe the same pass. Nil when unknown or when the column is
	// absent.
	LastWindowStart *time.Time `json:"last_window_start,omitempty"`
	Status          string     `json:"status"` // "pending", "syncing", "completed", "error"
	TotalTrades     int        `json:"total_trades"`
	ErrorMessage    string     `json:"error_message,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// SyncStatusRepo handles sync status persistence.
// Supports both TS (Prisma camelCase) and Go (snake_case) column naming.
type SyncStatusRepo struct {
	pool           *pgxpool.Pool
	schemaMu       sync.Mutex
	schemaDetected bool
	isTSSchema     bool
	// hasWindowStart: migration 021's last_window_start column is present.
	// Production runs the TS schema, where the column is added by hand, so
	// its absence is an expected state, not an error.
	hasWindowStart bool
}

// NewSyncStatusRepo creates a new sync status repository
func NewSyncStatusRepo(pool *pgxpool.Pool) *SyncStatusRepo {
	return &SyncStatusRepo{pool: pool}
}

func (r *SyncStatusRepo) detectSchema(ctx context.Context) {
	r.schemaMu.Lock()
	defer r.schemaMu.Unlock()
	if r.schemaDetected {
		return
	}

	// Check if sync_statuses uses camelCase (TS) or snake_case (Go)
	var exists bool
	if err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'sync_statuses' AND column_name = 'userUid'
		)`).Scan(&exists); err != nil {
		// Retried on the next call rather than latched as the Go schema.
		r.isTSSchema = false
		return
	}

	var hasWindowStart bool
	if err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'sync_statuses' AND column_name = 'last_window_start'
		)`).Scan(&hasWindowStart); err != nil {
		r.isTSSchema = false
		return
	}

	r.isTSSchema = exists
	r.hasWindowStart = hasWindowStart
	r.schemaDetected = true
}

// windowStartFragments returns the last_window_start pieces of an upsert, or
// empty strings when the column is absent. The value follows lastSyncTime: an
// attempt that keeps the old stamp (NULL, a failure) keeps the old window
// start with it, and one that sets the stamp sets the window start too.
func (r *SyncStatusRepo) windowStartFragments(stampCol string, argN int) (col, placeholder, set string) {
	if !r.hasWindowStart {
		return "", "", ""
	}
	return ", last_window_start",
		fmt.Sprintf(", $%d", argN),
		fmt.Sprintf(`,
			last_window_start = CASE WHEN EXCLUDED.%[1]s IS NULL THEN sync_statuses.last_window_start ELSE EXCLUDED.last_window_start END`, stampCol)
}

func (r *SyncStatusRepo) windowStartSelect() string {
	if !r.hasWindowStart {
		return ", NULL::timestamptz"
	}
	return ", last_window_start"
}

// Upsert creates or updates a sync status
func (r *SyncStatusRepo) Upsert(ctx context.Context, s *SyncStatus) error {
	r.detectSchema(ctx)

	if r.isTSSchema {
		return r.upsertTS(ctx, s)
	}

	wsCol, wsPlaceholder, wsSet := r.windowStartFragments("last_sync_time", 8)
	query := fmt.Sprintf(`
		INSERT INTO sync_statuses (user_uid, exchange, label, last_sync_time, status, total_trades, error_message, created_at, updated_at%s)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW()%s)
		ON CONFLICT (user_uid, exchange, label)
		DO UPDATE SET
			-- COALESCE, not EXCLUDED: a caller passing NULL is recording an
			-- attempt that failed, and a failed attempt must not overwrite the
			-- time of the last real sync. Callers that did sync always pass a
			-- value, so this only ever preserves.
			last_sync_time = COALESCE(EXCLUDED.last_sync_time, sync_statuses.last_sync_time),
			status = EXCLUDED.status,
			total_trades = EXCLUDED.total_trades,
			error_message = EXCLUDED.error_message,
			updated_at = NOW()%s
		RETURNING id`, wsCol, wsPlaceholder, wsSet)

	args := []any{s.UserUID, s.Exchange, s.Label, s.LastSyncTime, s.Status, s.TotalTrades, s.ErrorMessage}
	if r.hasWindowStart {
		args = append(args, s.LastWindowStart)
	}
	return r.pool.QueryRow(ctx, query, args...).Scan(&s.ID)
}

func (r *SyncStatusRepo) upsertTS(ctx context.Context, s *SyncStatus) error {
	// TS schema: camelCase columns. Prisma generates cuid() at the application
	// layer so there is no DB-level DEFAULT on id. Generate a UUID here so
	// the INSERT doesn't fail with a NOT NULL violation on first insert.
	now := time.Now().UTC()
	wsCol, wsPlaceholder, wsSet := r.windowStartFragments(`"lastSyncTime"`, 10)
	query := fmt.Sprintf(`
		INSERT INTO sync_statuses (id, "userUid", exchange, label, "lastSyncTime", status, "totalTrades", "errorMessage", "createdAt", "updatedAt"%s)
		VALUES (gen_random_uuid()::text, $1, $2, $3, $4, $5, $6, $7, $8, $9%s)
		ON CONFLICT ("userUid", exchange, label)
		DO UPDATE SET
			-- See the snake_case twin: NULL means "this attempt failed", and a
			-- failed attempt must not overwrite the time of the last real sync.
			"lastSyncTime" = COALESCE(EXCLUDED."lastSyncTime", sync_statuses."lastSyncTime"),
			status = EXCLUDED.status,
			"totalTrades" = EXCLUDED."totalTrades",
			"errorMessage" = EXCLUDED."errorMessage",
			"updatedAt" = EXCLUDED."updatedAt"%s
		RETURNING id`, wsCol, wsPlaceholder, wsSet)

	args := []any{s.UserUID, s.Exchange, s.Label, s.LastSyncTime, s.Status, s.TotalTrades, s.ErrorMessage, now, now}
	if r.hasWindowStart {
		args = append(args, s.LastWindowStart)
	}
	return r.pool.QueryRow(ctx, query, args...).Scan(&s.ID)
}

// GetByUserExchangeLabel retrieves a specific sync status
func (r *SyncStatusRepo) GetByUserExchangeLabel(ctx context.Context, userUID, exchange, label string) (*SyncStatus, error) {
	r.detectSchema(ctx)

	if r.isTSSchema {
		return r.getByKeyTS(ctx, userUID, exchange, label)
	}

	query := `
		SELECT id, user_uid, exchange, label, last_sync_time, status, total_trades, error_message, created_at, updated_at` + r.windowStartSelect() + `
		FROM sync_statuses
		WHERE user_uid = $1 AND exchange = $2 AND label = $3`

	var s SyncStatus
	err := r.pool.QueryRow(ctx, query, userUID, exchange, label).Scan(
		&s.ID, &s.UserUID, &s.Exchange, &s.Label, &s.LastSyncTime,
		&s.Status, &s.TotalTrades, &s.ErrorMessage, &s.CreatedAt, &s.UpdatedAt, &s.LastWindowStart,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (r *SyncStatusRepo) getByKeyTS(ctx context.Context, userUID, exchange, label string) (*SyncStatus, error) {
	query := `
		SELECT id, "userUid", exchange, label, "lastSyncTime", status, "totalTrades", "errorMessage", "createdAt", "updatedAt"` + r.windowStartSelect() + `
		FROM sync_statuses
		WHERE "userUid" = $1 AND exchange = $2 AND label = $3`

	var s SyncStatus
	var errorMsg *string
	err := r.pool.QueryRow(ctx, query, userUID, exchange, label).Scan(
		&s.ID, &s.UserUID, &s.Exchange, &s.Label, &s.LastSyncTime,
		&s.Status, &s.TotalTrades, &errorMsg, &s.CreatedAt, &s.UpdatedAt, &s.LastWindowStart,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if errorMsg != nil {
		s.ErrorMessage = *errorMsg
	}
	return &s, nil
}

// GetByUser retrieves all sync statuses for a user
func (r *SyncStatusRepo) GetByUser(ctx context.Context, userUID string) ([]*SyncStatus, error) {
	r.detectSchema(ctx)

	var query string
	if r.isTSSchema {
		query = `SELECT id, "userUid", exchange, label, "lastSyncTime", status, "totalTrades", "errorMessage", "createdAt", "updatedAt"
			FROM sync_statuses WHERE "userUid" = $1 ORDER BY "updatedAt" DESC`
	} else {
		query = `SELECT id, user_uid, exchange, label, last_sync_time, status, total_trades, error_message, created_at, updated_at
			FROM sync_statuses WHERE user_uid = $1 ORDER BY updated_at DESC`
	}

	rows, err := r.pool.Query(ctx, query, userUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statuses []*SyncStatus
	for rows.Next() {
		var s SyncStatus
		var errorMsg *string
		if err := rows.Scan(
			&s.ID, &s.UserUID, &s.Exchange, &s.Label, &s.LastSyncTime,
			&s.Status, &s.TotalTrades, &errorMsg, &s.CreatedAt, &s.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if errorMsg != nil {
			s.ErrorMessage = *errorMsg
		}
		statuses = append(statuses, &s)
	}
	return statuses, rows.Err()
}

// UpdateStatus updates the status field for a given sync status
func (r *SyncStatusRepo) UpdateStatus(ctx context.Context, userUID, exchange, label, status string, errMsg string) error {
	r.detectSchema(ctx)

	var query string
	if r.isTSSchema {
		query = `UPDATE sync_statuses SET status = $1, "errorMessage" = $2, "updatedAt" = NOW()
			WHERE "userUid" = $3 AND exchange = $4 AND label = $5`
	} else {
		query = `UPDATE sync_statuses SET status = $1, error_message = $2, updated_at = NOW()
			WHERE user_uid = $3 AND exchange = $4 AND label = $5`
	}

	_, err := r.pool.Exec(ctx, query, status, errMsg, userUID, exchange, label)
	return err
}

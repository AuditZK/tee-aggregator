package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// User represents a user in the system
type User struct {
	ID           string    `json:"id"`
	UID          string    `json:"uid"`
	PlatformHash *string   `json:"platform_hash,omitempty"` // SHA-256 of platform user ID
	SyncInterval string    `json:"sync_interval"`           // "hourly" or "daily"
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// UserRepo handles user persistence.
// Supports both TS (Prisma camelCase) and Go (snake_case) column naming.
type UserRepo struct {
	pool *pgxpool.Pool

	schemaMu     sync.Mutex
	schemaLoaded bool
	isTSSchema   bool // true = TS Prisma camelCase columns
}

// NewUserRepo creates a new user repository
func NewUserRepo(pool *pgxpool.Pool) *UserRepo {
	return &UserRepo{pool: pool}
}

// detectSchema checks once whether the DB uses TS Prisma camelCase columns.
// Detection: if "createdAt" column exists in users → TS schema.
func (r *UserRepo) detectSchema(ctx context.Context) bool {
	r.schemaMu.Lock()
	defer r.schemaMu.Unlock()

	if r.schemaLoaded {
		return r.isTSSchema
	}

	const query = `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'users'
			  AND column_name = 'createdAt'
		)`

	var exists bool
	if err := r.pool.QueryRow(ctx, query).Scan(&exists); err != nil {
		r.schemaLoaded = true
		r.isTSSchema = false
		return false
	}

	r.isTSSchema = exists
	r.schemaLoaded = true
	return r.isTSSchema
}

// syncIntervalToMinutes converts a Go sync_interval string to TS syncIntervalMinutes.
func syncIntervalToMinutes(interval string) int {
	switch interval {
	case "daily":
		return 1440
	default: // "hourly"
		return 60
	}
}

// syncIntervalFromMinutes converts a TS syncIntervalMinutes to Go sync_interval string.
func syncIntervalFromMinutes(minutes int) string {
	if minutes >= 1440 {
		return "daily"
	}
	return "hourly"
}

// GetOrCreate gets a user by UID or creates one if not exists
func (r *UserRepo) GetOrCreate(ctx context.Context, uid string) (*User, error) {
	if r.detectSchema(ctx) {
		return r.getOrCreateTS(ctx, uid)
	}

	query := `
		INSERT INTO users (uid, created_at, updated_at)
		VALUES ($1, NOW(), NOW())
		ON CONFLICT (uid) DO UPDATE SET updated_at = NOW()
		RETURNING id, uid, platform_hash, sync_interval, created_at, updated_at`

	var user User
	err := r.pool.QueryRow(ctx, query, uid).Scan(
		&user.ID, &user.UID, &user.PlatformHash, &user.SyncInterval, &user.CreatedAt, &user.UpdatedAt,
	)
	return &user, err
}

func (r *UserRepo) getOrCreateTS(ctx context.Context, uid string) (*User, error) {
	query := `
		INSERT INTO users (uid, "createdAt", "updatedAt")
		VALUES ($1, NOW(), NOW())
		ON CONFLICT (uid) DO UPDATE SET "updatedAt" = NOW()
		RETURNING id, uid, "platformHash", "syncIntervalMinutes", "createdAt", "updatedAt"`

	var user User
	var syncMins int
	err := r.pool.QueryRow(ctx, query, uid).Scan(
		&user.ID, &user.UID, &user.PlatformHash, &syncMins, &user.CreatedAt, &user.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	user.SyncInterval = syncIntervalFromMinutes(syncMins)
	return &user, nil
}

// GetOrCreateWithPlatformHash gets or creates a user and sets platform_hash if not already set
func (r *UserRepo) GetOrCreateWithPlatformHash(ctx context.Context, uid, platformUserID string) (*User, error) {
	hash := HashPlatformID(platformUserID)

	if r.detectSchema(ctx) {
		return r.getOrCreateWithPlatformHashTS(ctx, uid, hash)
	}

	query := `
		INSERT INTO users (uid, platform_hash, created_at, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (uid) DO UPDATE SET
			platform_hash = COALESCE(users.platform_hash, $2),
			updated_at = NOW()
		RETURNING id, uid, platform_hash, sync_interval, created_at, updated_at`

	var user User
	err := r.pool.QueryRow(ctx, query, uid, hash).Scan(
		&user.ID, &user.UID, &user.PlatformHash, &user.SyncInterval, &user.CreatedAt, &user.UpdatedAt,
	)
	return &user, err
}

func (r *UserRepo) getOrCreateWithPlatformHashTS(ctx context.Context, uid, hash string) (*User, error) {
	query := `
		INSERT INTO users (uid, "platformHash", "createdAt", "updatedAt")
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (uid) DO UPDATE SET
			"platformHash" = COALESCE(users."platformHash", $2),
			"updatedAt" = NOW()
		RETURNING id, uid, "platformHash", "syncIntervalMinutes", "createdAt", "updatedAt"`

	var user User
	var syncMins int
	err := r.pool.QueryRow(ctx, query, uid, hash).Scan(
		&user.ID, &user.UID, &user.PlatformHash, &syncMins, &user.CreatedAt, &user.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	user.SyncInterval = syncIntervalFromMinutes(syncMins)
	return &user, nil
}

// GetAllWithConnections returns all users that have at least one active connection
func (r *UserRepo) GetAllWithConnections(ctx context.Context) ([]*User, error) {
	if r.detectSchema(ctx) {
		return r.getAllWithConnectionsTS(ctx)
	}

	query := `
		SELECT DISTINCT u.id, u.uid, u.platform_hash, u.sync_interval, u.created_at, u.updated_at
		FROM users u
		INNER JOIN exchange_connections ec ON ec.user_uid = u.uid
		WHERE ec.is_active = true
		ORDER BY u.created_at`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []*User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.UID, &u.PlatformHash, &u.SyncInterval, &u.CreatedAt, &u.UpdatedAt); err != nil {
			return nil, err
		}
		users = append(users, &u)
	}

	return users, rows.Err()
}

func (r *UserRepo) getAllWithConnectionsTS(ctx context.Context) ([]*User, error) {
	query := `
		SELECT DISTINCT u.id, u.uid, u."platformHash", u."syncIntervalMinutes", u."createdAt", u."updatedAt"
		FROM users u
		INNER JOIN exchange_connections ec ON ec."userUid" = u.uid
		WHERE ec."isActive" = true
		ORDER BY u."createdAt"`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []*User
	for rows.Next() {
		var u User
		var syncMins int
		if err := rows.Scan(&u.ID, &u.UID, &u.PlatformHash, &syncMins, &u.CreatedAt, &u.UpdatedAt); err != nil {
			return nil, err
		}
		u.SyncInterval = syncIntervalFromMinutes(syncMins)
		users = append(users, &u)
	}

	return users, rows.Err()
}

// GetByUID returns a user by their UID
func (r *UserRepo) GetByUID(ctx context.Context, uid string) (*User, error) {
	if r.detectSchema(ctx) {
		return r.getByUIDTS(ctx, uid)
	}

	query := `
		SELECT id, uid, platform_hash, sync_interval, created_at, updated_at
		FROM users
		WHERE uid = $1`

	var user User
	err := r.pool.QueryRow(ctx, query, uid).Scan(
		&user.ID, &user.UID, &user.PlatformHash, &user.SyncInterval, &user.CreatedAt, &user.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

func (r *UserRepo) getByUIDTS(ctx context.Context, uid string) (*User, error) {
	query := `
		SELECT id, uid, "platformHash", "syncIntervalMinutes", "createdAt", "updatedAt"
		FROM users
		WHERE uid = $1`

	var user User
	var syncMins int
	err := r.pool.QueryRow(ctx, query, uid).Scan(
		&user.ID, &user.UID, &user.PlatformHash, &syncMins, &user.CreatedAt, &user.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	user.SyncInterval = syncIntervalFromMinutes(syncMins)
	return &user, nil
}

// UpdateSyncInterval updates a user's sync interval
func (r *UserRepo) UpdateSyncInterval(ctx context.Context, uid string, interval string) error {
	if interval != "hourly" && interval != "daily" {
		interval = "hourly"
	}

	if r.detectSchema(ctx) {
		// TS uses syncIntervalMinutes (INT) instead of sync_interval (VARCHAR)
		query := `UPDATE users SET "syncIntervalMinutes" = $1, "updatedAt" = NOW() WHERE uid = $2`
		_, err := r.pool.Exec(ctx, query, syncIntervalToMinutes(interval), uid)
		return err
	}

	query := `UPDATE users SET sync_interval = $1, updated_at = NOW() WHERE uid = $2`
	_, err := r.pool.Exec(ctx, query, interval, uid)
	return err
}

// HashPlatformID returns the SHA-256 hex hash of a platform user ID
func HashPlatformID(platformUserID string) string {
	h := sha256.Sum256([]byte(platformUserID))
	return hex.EncodeToString(h[:])
}

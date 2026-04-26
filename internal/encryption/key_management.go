package encryption

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

// DEK represents a Data Encryption Key
type DEK struct {
	ID           string    `json:"id"`
	EncryptedDEK string    `json:"encrypted_dek"`
	IV           string    `json:"iv"`
	AuthTag      string    `json:"auth_tag"`
	MasterKeyID  string    `json:"master_key_id"`
	IsActive     bool      `json:"is_active"`
	CreatedAt    time.Time `json:"created_at"`
	RotatedAt    time.Time `json:"rotated_at,omitempty"`
}

// KeyManagementService manages DEK lifecycle.
//
// Lifetime rules:
//   - An instance holds a single unwrapped DEK in memory for the
//     lifetime of the process. DEKs are never written to disk outside
//     the data_encryption_keys table (where they live wrapped).
//   - On startup the service reads the active wrapped DEK from the DB
//     and unwraps it with the master key from KeyDerivationService.
//   - If unwrap fails, the service returns an error so the operator
//     can investigate. It does NOT auto-rotate: auto-rotation would
//     overwrite a DEK that may still be in use by a sibling enclave
//     (e.g. the TS prod enclave sharing the same DB), which would
//     permanently break every credential encrypted with the old DEK.
//
// AllowAutoInit controls whether the service is allowed to create a
// brand-new DEK when no active row exists. This is only safe on a
// fresh database; reuse against a seeded DB must set it to false.
type KeyManagementService struct {
	pool       *pgxpool.Pool
	derivation *KeyDerivationService
	logger     *zap.Logger

	currentDEK []byte
	dekID      string
	mu         sync.RWMutex

	allowAutoInit bool
}

// KeyManagementOptions configures NewKeyManagementService.
type KeyManagementOptions struct {
	// Derivation provides the master key used to unwrap the DEK. If
	// nil, the service builds its own via NewKeyDerivationService.
	Derivation *KeyDerivationService

	// Logger is optional; when non-nil the service records key
	// lifecycle events.
	Logger *zap.Logger

	// AllowAutoInit permits the service to write a fresh DEK to the
	// database when none exists yet. Set false for parallel-test /
	// shared-DB scenarios to avoid silently reseeding a live schema.
	AllowAutoInit bool
}

// NewKeyManagementService creates a new key management service.
func NewKeyManagementService(pool *pgxpool.Pool, opts KeyManagementOptions) (*KeyManagementService, error) {
	derivation := opts.Derivation
	if derivation == nil {
		var err error
		derivation, err = NewKeyDerivationService(opts.Logger)
		if err != nil {
			return nil, fmt.Errorf("init key derivation: %w", err)
		}
	}

	svc := &KeyManagementService{
		pool:          pool,
		derivation:    derivation,
		logger:        opts.Logger,
		allowAutoInit: opts.AllowAutoInit,
	}

	if err := svc.initializeDEK(context.Background()); err != nil {
		return nil, fmt.Errorf("init DEK: %w", err)
	}

	return svc, nil
}

// GetCurrentDEK returns the current DEK for encryption
func (s *KeyManagementService) GetCurrentDEK() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.currentDEK == nil {
		return nil, fmt.Errorf("no active DEK")
	}

	return s.currentDEK, nil
}

// GetEncryptionService returns an encryption service with the current DEK
func (s *KeyManagementService) GetEncryptionService() (*Service, error) {
	dek, err := s.GetCurrentDEK()
	if err != nil {
		return nil, err
	}
	return New(dek)
}

// RotateDEK creates a new DEK and marks the old one as inactive.
// This is an explicit operator-requested action — unlike the broken
// auto-rotate that used to live inside initializeDEK, callers that
// invoke this must be sure no sibling enclave depends on the current
// DEK.
func (s *KeyManagementService) RotateDEK(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rotateDEKLocked(ctx)
}

// initializeDEK loads the active DEK from the database, unwraps it
// with the master key, and caches it in memory.
//
// Supports both Go (snake_case) and TS Prisma (camelCase) column
// names so the service can attach to either schema transparently.
//
// On unwrap failure this function does NOT rotate the DEK. Rotating
// would deactivate the DEK that a sibling TS enclave is still using
// to decrypt credentials, permanently bricking every existing
// exchange_connections row. Instead we return a descriptive error so
// the operator can diagnose the master-key / DEK mismatch.
func (s *KeyManagementService) initializeDEK(ctx context.Context) error {
	dek, schema, err := s.loadActiveDEK(ctx)
	if err != nil {
		return fmt.Errorf("load active dek: %w", err)
	}

	if dek == nil {
		if !s.allowAutoInit {
			return fmt.Errorf("no active DEK in data_encryption_keys and AllowAutoInit=false — refusing to seed a new DEK on a shared database")
		}
		if s.logger != nil {
			s.logger.Warn("no active DEK in database — seeding a fresh one (AllowAutoInit=true)")
		}
		return s.rotateDEKLocked(ctx)
	}

	if s.logger != nil {
		s.logger.Info("loaded active DEK from database",
			zap.String("dek_id", dek.ID),
			zap.String("schema", schema),
			zap.String("stored_master_key_id", dek.MasterKeyID),
			zap.String("current_master_key_id", s.derivation.GetMasterKeyID()),
		)
	}

	// Sanity check: warn if the stored master key id does not match
	// the one we just derived. This usually means the attestation
	// measurement drifted (kernel / container upgrade) and the TS
	// enclave has not yet run its migration script.
	if dek.MasterKeyID != "" && dek.MasterKeyID != s.derivation.GetMasterKeyID() && s.logger != nil {
		s.logger.Warn("master key id mismatch between derivation and stored DEK — unwrap will likely fail",
			zap.String("stored", dek.MasterKeyID),
			zap.String("derived", s.derivation.GetMasterKeyID()),
		)
	}

	wrapped := &EncryptedData{
		Ciphertext: dek.EncryptedDEK,
		IV:         dek.IV,
		AuthTag:    dek.AuthTag,
	}

	// MIGRATION-DEK: try both the hardware-derived and env-derived master
	// keys. This lets a freshly-built binary (different SEV-SNP measurement,
	// so different masterHW) still boot against a DEK that has been
	// re-wrapped via cmd/migrate-dek-wrap with the env master key — which
	// is reproducible across binary upgrades because it derives only from
	// ENCRYPTION_KEY. Hardware is tried first to preserve the existing
	// behaviour for un-migrated DEKs.
	unwrapped, source, err := s.derivation.UnwrapKeyTryAll(wrapped)
	if err != nil {
		return fmt.Errorf(
			"unwrap active DEK (id=%s, stored_master_key_id=%s, derived_master_key_id=%s): %w — "+
				"refusing to rotate the DEK automatically; investigate the master-key mismatch manually "+
				"(if you just upgraded the binary, run cmd/migrate-dek-wrap before the new image boots)",
			dek.ID, dek.MasterKeyID, s.derivation.GetMasterKeyID(), err,
		)
	}

	if s.logger != nil {
		s.logger.Info("DEK unwrapped",
			zap.String("dek_id", dek.ID),
			zap.String("source", string(source)),
		)
	}

	s.currentDEK = unwrapped
	s.dekID = dek.ID
	return nil
}

// loadActiveDEK queries data_encryption_keys for the currently-active
// row, trying the Go snake_case schema first then the TS Prisma
// camelCase schema. Returns (nil, "", nil) when neither schema has an
// active row — this is the signal to seed a new DEK if AllowAutoInit
// is true.
func (s *KeyManagementService) loadActiveDEK(ctx context.Context) (*DEK, string, error) {
	var dek DEK
	err := s.pool.QueryRow(ctx, `
		SELECT id, encrypted_dek, iv, auth_tag, master_key_id
		FROM data_encryption_keys
		WHERE is_active = true
		ORDER BY created_at DESC
		LIMIT 1`).Scan(
		&dek.ID, &dek.EncryptedDEK, &dek.IV, &dek.AuthTag, &dek.MasterKeyID,
	)
	if err == nil {
		return &dek, "go-snake", nil
	}

	// Try TS Prisma camelCase schema. We can't easily distinguish
	// "table has no row" from "column does not exist" at this layer,
	// so we try the second query unconditionally and only report the
	// second error.
	err = s.pool.QueryRow(ctx, `
		SELECT id, "encryptedDEK", iv, "authTag", "masterKeyId"
		FROM data_encryption_keys
		WHERE "isActive" = true
		ORDER BY "createdAt" DESC
		LIMIT 1`).Scan(
		&dek.ID, &dek.EncryptedDEK, &dek.IV, &dek.AuthTag, &dek.MasterKeyID,
	)
	if err == nil {
		return &dek, "ts-camel", nil
	}

	// pgx reports "no rows" via pgx.ErrNoRows; every other error is
	// real (missing table, bad schema, etc.) and should surface.
	// SEC-010: use errors.Is instead of brittle string matching.
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", nil
	}
	// AUDIT-DEK-INIT: when the table exists with snake_case schema only
	// (Go-managed DB), the camelCase fallback throws "undefined_column"
	// (PG SQLSTATE 42703) or "undefined_table" (42P01). Treat these as
	// "this schema variant is absent" and signal nil-DEK so AllowAutoInit
	// can seed a fresh one instead of fataling.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "42703" || pgErr.Code == "42P01") {
		return nil, "", nil
	}
	return nil, "", err
}

// rotateDEKLocked is the internal helper used when we have decided
// it is safe to write a fresh DEK (either after an explicit Rotate()
// call or during initial seeding with AllowAutoInit=true). The caller
// must hold s.mu or be inside the constructor.
//
// SEC-010: the two SQL operations (deactivate old, insert new) now run inside
// a single transaction. A crash or error between them used to leave either
// zero active DEKs or both rows active; now the rollback is atomic.
func (s *KeyManagementService) rotateDEKLocked(ctx context.Context) error {
	newDEK := make([]byte, 32)
	if _, err := rand.Read(newDEK); err != nil {
		return fmt.Errorf("generate DEK: %w", err)
	}

	wrapped, err := s.derivation.WrapKey(newDEK)
	if err != nil {
		return fmt.Errorf("wrap DEK: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin DEK rotation tx: %w", err)
	}
	// Ensures we roll back on any unexpected return path. No-op after Commit.
	defer func() { _ = tx.Rollback(ctx) }()

	if s.dekID != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE data_encryption_keys
			SET is_active = false, rotated_at = NOW()
			WHERE id = $1`, s.dekID); err != nil {
			return fmt.Errorf("deactivate old DEK: %w", err)
		}
	}

	var newID string
	err = tx.QueryRow(ctx, `
		INSERT INTO data_encryption_keys
		(encrypted_dek, iv, auth_tag, master_key_id, is_active, created_at)
		VALUES ($1, $2, $3, $4, true, NOW())
		RETURNING id`,
		wrapped.Ciphertext, wrapped.IV, wrapped.AuthTag,
		s.derivation.GetMasterKeyID(),
	).Scan(&newID)
	if err != nil {
		return fmt.Errorf("store new DEK: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit DEK rotation: %w", err)
	}

	s.currentDEK = newDEK
	s.dekID = newID
	if s.logger != nil {
		s.logger.Info("wrote fresh DEK",
			zap.String("dek_id", newID),
			zap.String("master_key_id", s.derivation.GetMasterKeyID()),
		)
	}
	return nil
}

// IsHardwareKeyAvailable returns true if using hardware-derived master key
func (s *KeyManagementService) IsHardwareKeyAvailable() bool {
	return s.derivation.IsHardwareKey()
}

// GetMasterKeyID returns the current master key identifier
func (s *KeyManagementService) GetMasterKeyID() string {
	return s.derivation.GetMasterKeyID()
}

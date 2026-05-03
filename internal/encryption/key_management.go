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

	// legacyMasterKeyApplied is true when the boot code injected
	// ExternalMasterKey via KeyManagementOptions — i.e. the v0→v1
	// migration path. Used by initializeDEK to detect the case where
	// it should re-wrap the DEK with the measurement-derived master
	// key after a successful unwrap with the legacy one.
	legacyMasterKeyApplied bool

	// measurementMasterKeyID captures what the SEV-SNP measurement
	// would naturally derive, computed before any ExternalMasterKey
	// override. Used as the "destination" master_key_id when the
	// migration re-wrap kicks in.
	measurementMasterKeyID string
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

	// ExternalMasterKey, when non-nil, overrides the master key the
	// service would otherwise derive from the SEV-SNP measurement (or
	// the env-var fallback). Used by the B2 handoff client: the new
	// enclave fetches the master key from its predecessor over an
	// attested ECIES channel and injects it here so DEK unwrap matches
	// what the predecessor wrote.
	//
	// MUST be exactly 32 bytes (AES-256). The slice is copied into the
	// service so callers can safely zeroise the original after the call.
	ExternalMasterKey []byte
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

	// Capture the measurement-derived master_key_id BEFORE any
	// ExternalMasterKey override, so the auto-rewrap path knows what
	// the "destination" identity is.
	measurementMasterKeyID := derivation.GetMasterKeyID()

	// B2 handoff: when ExternalMasterKey is provided, override the
	// derivation's primary master key. The legacy hardware-derived key
	// is still useful for *signing* future wraps but unwrap will use
	// the externally-supplied key (matches the previous binary's wrap).
	legacyApplied := false
	if len(opts.ExternalMasterKey) > 0 {
		if len(opts.ExternalMasterKey) != 32 {
			return nil, fmt.Errorf("ExternalMasterKey must be 32 bytes, got %d", len(opts.ExternalMasterKey))
		}
		copied := make([]byte, 32)
		copy(copied, opts.ExternalMasterKey)
		derivation.masterKey = copied
		derivation.isHardware = false
		legacyApplied = true
		if opts.Logger != nil {
			opts.Logger.Info("master key supplied externally (handoff path)",
				zap.String("master_key_id", derivation.GetMasterKeyID()),
				zap.String("measurement_master_key_id", measurementMasterKeyID),
			)
		}
	}

	svc := &KeyManagementService{
		pool:                   pool,
		derivation:             derivation,
		logger:                 opts.Logger,
		allowAutoInit:          opts.AllowAutoInit,
		legacyMasterKeyApplied: legacyApplied,
		measurementMasterKeyID: measurementMasterKeyID,
	}

	if err := svc.initializeDEK(context.Background()); err != nil {
		return nil, fmt.Errorf("init DEK: %w", err)
	}

	return svc, nil
}

// ExportMasterKey returns a copy of the current master key. Used by the
// B2 handoff server: the predecessor enclave hands its master key to the
// successor over an attested ECIES channel.
//
// CALLER OBLIGATION: zeroise the returned slice as soon as it has been
// encrypted to the successor's pubkey. Holding it any longer extends the
// window where a memory dump could expose it.
//
// This method is intentionally exposed only on the in-process
// KeyManagementService. The handoff server gates calls behind attestation
// verification — see internal/bootstrap.HandoffServer.
func (s *KeyManagementService) ExportMasterKey() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.derivation == nil || s.derivation.masterKey == nil {
		return nil, fmt.Errorf("master key not initialised")
	}
	out := make([]byte, len(s.derivation.masterKey))
	copy(out, s.derivation.masterKey)
	return out, nil
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

	unwrapped, err := s.derivation.UnwrapKey(wrapped)
	if err != nil {
		return fmt.Errorf(
			"unwrap active DEK (id=%s, stored_master_key_id=%s, derived_master_key_id=%s): %w — "+
				"refusing to rotate the DEK automatically; investigate the master-key mismatch manually",
			dek.ID, dek.MasterKeyID, s.derivation.GetMasterKeyID(), err,
		)
	}

	s.currentDEK = unwrapped
	s.dekID = dek.ID

	// Legacy-migration auto-rewrap: when the operator booted this
	// enclave with an LegacyMasterKeyHex (B2 v0→v1 migration), the
	// `derivation` we hold has been overridden to the legacy master
	// key so the existing DEK could be unwrapped. But that key won't
	// match what THIS enclave's measurement derives at the next boot.
	//
	// To make the next boot self-sufficient (no env var dependency),
	// re-wrap the DEK now with the *measurement-derived* master key
	// of this enclave and persist it. After this completes, removing
	// LEGACY_MASTER_KEY_HEX from GCP metadata is safe — the next
	// reboot will derive the same measurement-master, find the
	// re-wrapped DEK, and proceed.
	//
	// We detect the migration scenario by comparing the stored
	// master_key_id against the freshly-measurement-derived one. If
	// they differ AND we just successfully unwrapped, that means the
	// derivation we used for unwrap is NOT the measurement-derived
	// one — i.e. the legacy override.
	if s.legacyMasterKeyApplied && dek.MasterKeyID != s.measurementMasterKeyID {
		if s.logger != nil {
			s.logger.Info("legacy migration: re-wrapping DEK with measurement-derived master key",
				zap.String("dek_id", dek.ID),
				zap.String("old_master_key_id", dek.MasterKeyID),
				zap.String("new_master_key_id", s.measurementMasterKeyID),
			)
		}
		if err := s.rewrapActiveDEKWithMeasurementKey(ctx); err != nil {
			// Don't fail the boot — the enclave is functional with
			// the legacy override. The operator just won't be able
			// to remove the env var until they re-run migration.
			if s.logger != nil {
				s.logger.Error("legacy migration re-wrap failed (boot continues; legacy env var still required)",
					zap.Error(err))
			}
		}
	}

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

// rewrapActiveDEKWithMeasurementKey re-wraps the in-memory DEK using
// the SEV-SNP measurement-derived master key (NOT the legacy override
// currently held in s.derivation.masterKey) and writes the new wrap
// back to data_encryption_keys. After this completes, the next boot
// can derive the same measurement-master and unwrap the new row,
// dropping the operator's dependency on LEGACY_MASTER_KEY_HEX.
//
// Caller must hold s.mu for write OR be inside initializeDEK before
// the service is published.
func (s *KeyManagementService) rewrapActiveDEKWithMeasurementKey(ctx context.Context) error {
	if s.currentDEK == nil {
		return fmt.Errorf("currentDEK not loaded")
	}
	if s.dekID == "" {
		return fmt.Errorf("dekID not loaded")
	}
	if s.measurementMasterKeyID == "" {
		return fmt.Errorf("measurementMasterKeyID empty — cannot determine target identity")
	}

	// Re-derive the measurement-only master key from snpguest. We
	// build a fresh KeyDerivationService here because s.derivation
	// has been overridden with the legacy key — re-deriving gives us
	// a clean view of "what THIS enclave's measurement produces".
	freshDerivation, err := NewKeyDerivationService(s.logger)
	if err != nil {
		return fmt.Errorf("re-derive measurement master key: %w", err)
	}
	if freshDerivation.GetMasterKeyID() != s.measurementMasterKeyID {
		// Sanity: the same enclave should always derive the same
		// master_key_id. If this ever fires, something is wrong
		// with either snpguest or our caching.
		return fmt.Errorf("freshly-derived master_key_id (%s) != captured (%s)",
			freshDerivation.GetMasterKeyID(), s.measurementMasterKeyID)
	}

	// Wrap the existing DEK with the measurement-derived master.
	wrapped, err := freshDerivation.WrapKey(s.currentDEK)
	if err != nil {
		return fmt.Errorf("wrap DEK with measurement master: %w", err)
	}

	// Update the existing row in place. We deliberately DO NOT rotate
	// (deactivate-then-insert): the DEK plaintext is unchanged, only
	// the wrap is, so any sibling enclave still able to derive the
	// legacy master key would lose access — but that's the point of
	// the migration. The old wrap is replaced atomically.
	if _, err := s.pool.Exec(ctx, `
		UPDATE data_encryption_keys
		SET encrypted_dek = $1, iv = $2, auth_tag = $3, master_key_id = $4
		WHERE id = $5`,
		wrapped.Ciphertext, wrapped.IV, wrapped.AuthTag,
		s.measurementMasterKeyID, s.dekID,
	); err != nil {
		return fmt.Errorf("update wrap in DB: %w", err)
	}

	// Now point our derivation back at the measurement master so
	// subsequent in-process wraps use the new identity (e.g. if the
	// operator triggers RotateDEK later).
	s.derivation = freshDerivation
	s.legacyMasterKeyApplied = false

	if s.logger != nil {
		s.logger.Info("legacy migration complete: DEK re-wrapped with measurement master key",
			zap.String("dek_id", s.dekID),
			zap.String("master_key_id", s.measurementMasterKeyID),
			zap.String("hint", "you can now safely remove LEGACY_MASTER_KEY_HEX from the deploy env"),
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

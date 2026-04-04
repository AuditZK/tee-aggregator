package encryption

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

// KeyManagementService manages DEK lifecycle
type KeyManagementService struct {
	pool       *pgxpool.Pool
	derivation *KeyDerivationService

	currentDEK []byte
	dekID      string
	mu         sync.RWMutex
}

// NewKeyManagementService creates a new key management service
func NewKeyManagementService(pool *pgxpool.Pool) (*KeyManagementService, error) {
	derivation, err := NewKeyDerivationService()
	if err != nil {
		return nil, fmt.Errorf("init key derivation: %w", err)
	}

	svc := &KeyManagementService{
		pool:       pool,
		derivation: derivation,
	}

	// Load or create DEK
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

// RotateDEK creates a new DEK and marks the old one as inactive
func (s *KeyManagementService) RotateDEK(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate new DEK
	newDEK := make([]byte, 32)
	if _, err := rand.Read(newDEK); err != nil {
		return fmt.Errorf("generate DEK: %w", err)
	}

	// Wrap with master key
	wrapped, err := s.derivation.WrapKey(newDEK)
	if err != nil {
		return fmt.Errorf("wrap DEK: %w", err)
	}

	// Deactivate old DEK
	if s.dekID != "" {
		_, err := s.pool.Exec(ctx, `
			UPDATE data_encryption_keys
			SET is_active = false, rotated_at = NOW()
			WHERE id = $1`, s.dekID)
		if err != nil {
			return fmt.Errorf("deactivate old DEK: %w", err)
		}
	}

	// Store new DEK
	var newID string
	err = s.pool.QueryRow(ctx, `
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

	s.currentDEK = newDEK
	s.dekID = newID

	return nil
}

// initializeDEK loads or creates the initial DEK.
// Supports both Go (snake_case) and TS Prisma (camelCase) column names.
func (s *KeyManagementService) initializeDEK(ctx context.Context) error {
	// Try Go schema first, then TS schema
	var dek DEK
	err := s.pool.QueryRow(ctx, `
		SELECT id, encrypted_dek, iv, auth_tag, master_key_id
		FROM data_encryption_keys
		WHERE is_active = true
		ORDER BY created_at DESC
		LIMIT 1`).Scan(
		&dek.ID, &dek.EncryptedDEK, &dek.IV, &dek.AuthTag, &dek.MasterKeyID,
	)

	if err != nil {
		// Try TS Prisma camelCase schema
		err = s.pool.QueryRow(ctx, `
			SELECT id, "encryptedDEK", iv, "authTag", "masterKeyId"
			FROM data_encryption_keys
			WHERE "isActive" = true
			ORDER BY "createdAt" DESC
			LIMIT 1`).Scan(
			&dek.ID, &dek.EncryptedDEK, &dek.IV, &dek.AuthTag, &dek.MasterKeyID,
		)
	}

	if err == nil {
		// Unwrap existing DEK
		wrapped := &EncryptedData{
			Ciphertext: dek.EncryptedDEK,
			IV:         dek.IV,
			AuthTag:    dek.AuthTag,
		}

		unwrapped, err := s.derivation.UnwrapKey(wrapped)
		if err != nil {
			// Master key changed, need to migrate
			return s.RotateDEK(ctx)
		}

		s.currentDEK = unwrapped
		s.dekID = dek.ID
		return nil
	}

	// No existing DEK, create new one
	return s.RotateDEK(ctx)
}

// IsHardwareKeyAvailable returns true if using hardware-derived master key
func (s *KeyManagementService) IsHardwareKeyAvailable() bool {
	return s.derivation.IsHardwareKey()
}

// GetMasterKeyID returns the current master key identifier
func (s *KeyManagementService) GetMasterKeyID() string {
	return s.derivation.GetMasterKeyID()
}

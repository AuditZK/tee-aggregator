package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/repository"
)

var ErrConnectionAlreadyExists = errors.New("connection already exists")

const ExistingConnectionNoopMessage = "User connection already exists (no action taken)"

// Credentials holds decrypted API credentials
type Credentials struct {
	UserUID    string
	Exchange   string
	APIKey     string
	APISecret  string
	Passphrase string
}

// ExchangeMetadata holds exchange-level report metadata.
type ExchangeMetadata struct {
	Exchange string
	KYCLevel string
	IsPaper  bool
}

// ConnectionService handles exchange connection business logic
type ConnectionService struct {
	repo       *repository.ConnectionRepo
	encryption *encryption.Service
	factory    *connector.Factory
}

// NewConnectionService creates a new connection service
func NewConnectionService(repo *repository.ConnectionRepo, enc *encryption.Service) *ConnectionService {
	return &ConnectionService{
		repo:       repo,
		encryption: enc,
		factory:    connector.NewFactory(),
	}
}

// CreateConnectionRequest is the input for creating a connection
type CreateConnectionRequest struct {
	UserUID             string
	Exchange            string
	Label               string
	APIKey              string
	APISecret           string
	Passphrase          string
	SyncIntervalMinutes int
	ExcludeFromReport   bool
}

// Create encrypts and stores a new exchange connection
func (s *ConnectionService) Create(ctx context.Context, req *CreateConnectionRequest) error {
	if s.repo == nil || s.encryption == nil {
		return fmt.Errorf("connection service not configured")
	}

	normalizedExchange := normalizeExchange(req.Exchange)
	normalizedLabel := strings.TrimSpace(req.Label)

	existing, err := s.repo.GetByUserExchangeLabel(ctx, req.UserUID, normalizedExchange, normalizedLabel)
	if err != nil && !errors.Is(err, repository.ErrNotFound) {
		return fmt.Errorf("check existing connection: %w", err)
	}
	if err == nil && existing != nil {
		return fmt.Errorf("%w: %s", ErrConnectionAlreadyExists, ExistingConnectionNoopMessage)
	}

	// Test credentials before saving — fail fast with a clear error
	testConn, err := s.factory.Create(&connector.Credentials{
		Exchange:   normalizedExchange,
		APIKey:     req.APIKey,
		APISecret:  req.APISecret,
		Passphrase: req.Passphrase,
	})
	if err != nil {
		return fmt.Errorf("invalid credentials: unsupported exchange %s", normalizedExchange)
	}
	if err := testConn.TestConnection(ctx); err != nil {
		return fmt.Errorf("invalid credentials: %w", err)
	}

	credentialsHash := hashCredentials(req.APIKey, req.APISecret, req.Passphrase)

	// Encrypt API key
	apiKeyEnc, err := s.encryption.EncryptString(req.APIKey)
	if err != nil {
		return fmt.Errorf("encrypt api key: %w", err)
	}

	// Encrypt API secret
	apiSecretEnc, err := s.encryption.EncryptString(req.APISecret)
	if err != nil {
		return fmt.Errorf("encrypt api secret: %w", err)
	}

	// Encrypt passphrase (if present)
	var passphraseEnc *encryption.EncryptedData
	if req.Passphrase != "" {
		passphraseEnc, err = s.encryption.EncryptString(req.Passphrase)
		if err != nil {
			return fmt.Errorf("encrypt passphrase: %w", err)
		}
	}

	conn := &repository.ExchangeConnection{
		UserUID:             req.UserUID,
		Exchange:            normalizedExchange,
		Label:               normalizedLabel,
		CredentialsHash:     credentialsHash,
		SyncIntervalMinutes: normalizeSyncIntervalMinutes(req.SyncIntervalMinutes),
		ExcludeFromReport:   req.ExcludeFromReport,
		EncryptedAPIKey:     apiKeyEnc.Ciphertext,
		APIKeyIV:            apiKeyEnc.IV,
		APIKeyAuthTag:       apiKeyEnc.AuthTag,
		EncryptedAPISecret:  apiSecretEnc.Ciphertext,
		APISecretIV:         apiSecretEnc.IV,
		APISecretAuthTag:    apiSecretEnc.AuthTag,
	}

	if passphraseEnc != nil {
		conn.EncryptedPassphrase = passphraseEnc.Ciphertext
		conn.PassphraseIV = passphraseEnc.IV
		conn.PassphraseAuthTag = passphraseEnc.AuthTag
	}

	if err := s.repo.Create(ctx, conn); err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) {
			return fmt.Errorf("%w: %s", ErrConnectionAlreadyExists, ExistingConnectionNoopMessage)
		}
		return err
	}

	// TS parity: capture exchange metadata (KYC level + paper/live status)
	// after successful connection creation; failures are non-blocking.
	s.captureExchangeMetadata(ctx, conn.ID, &connector.Credentials{
		Exchange:   normalizedExchange,
		APIKey:     req.APIKey,
		APISecret:  req.APISecret,
		Passphrase: req.Passphrase,
	})

	return nil
}

func (s *ConnectionService) captureExchangeMetadata(ctx context.Context, connectionID string, creds *connector.Credentials) {
	if s.repo == nil || s.factory == nil || strings.TrimSpace(connectionID) == "" || creds == nil {
		return
	}

	exchangeConn, err := s.factory.Create(creds)
	if err != nil {
		return
	}

	if fetcher, ok := exchangeConn.(connector.KYCLevelFetcher); ok {
		kycLevel, err := fetcher.FetchKYCLevel(ctx)
		if err == nil {
			if normalized := normalizeKYCLevel(kycLevel); normalized != "" {
				_ = s.repo.UpdateKYCLevel(ctx, connectionID, normalized)
			}
		}
	}

	if detector, ok := exchangeConn.(connector.PaperAccountDetector); ok {
		isPaper, err := detector.DetectIsPaper(ctx)
		if err == nil {
			_ = s.repo.UpdateIsPaper(ctx, connectionID, isPaper)
		}
	}
}

// GetDecryptedCredentials retrieves and decrypts credentials for a connection
func (s *ConnectionService) GetDecryptedCredentials(ctx context.Context, userUID, exchange string) (*Credentials, error) {
	conn, err := s.repo.GetByUserAndExchange(ctx, userUID, normalizeExchange(exchange))
	if err != nil {
		return nil, err
	}
	return s.decryptConnection(conn)
}

// GetDecryptedCredentialsByLabel retrieves and decrypts credentials for a specific connection label.
func (s *ConnectionService) GetDecryptedCredentialsByLabel(ctx context.Context, userUID, exchange, label string) (*Credentials, error) {
	conn, err := s.repo.GetByUserExchangeLabel(ctx, userUID, normalizeExchange(exchange), strings.TrimSpace(label))
	if err != nil {
		return nil, err
	}
	return s.decryptConnection(conn)
}

func (s *ConnectionService) decryptConnection(conn *repository.ExchangeConnection) (*Credentials, error) {
	// Decrypt API key — try Go format (3 fields base64), fallback to TS format (1 field hex)
	apiKey, err := s.decryptField(conn.EncryptedAPIKey, conn.APIKeyIV, conn.APIKeyAuthTag)
	if err != nil {
		return nil, fmt.Errorf("decrypt api key: %w", err)
	}

	// Decrypt API secret
	apiSecret, err := s.decryptField(conn.EncryptedAPISecret, conn.APISecretIV, conn.APISecretAuthTag)
	if err != nil {
		return nil, fmt.Errorf("decrypt api secret: %w", err)
	}

	// Decrypt passphrase (if present)
	var passphrase string
	if conn.EncryptedPassphrase != "" {
		passphrase, err = s.decryptField(conn.EncryptedPassphrase, conn.PassphraseIV, conn.PassphraseAuthTag)
		if err != nil {
			return nil, fmt.Errorf("decrypt passphrase: %w", err)
		}
	}

	return &Credentials{
		UserUID:    conn.UserUID,
		Exchange:   conn.Exchange,
		APIKey:     apiKey,
		APISecret:  apiSecret,
		Passphrase: passphrase,
	}, nil
}

// GetActiveConnections returns all active connections for a user (encrypted)
func (s *ConnectionService) GetActiveConnections(ctx context.Context, userUID string) ([]*repository.ExchangeConnection, error) {
	return s.repo.GetActiveByUser(ctx, userUID)
}

// GetExcludedExchanges returns exchanges marked as excluded from reports/analytics.
func (s *ConnectionService) GetExcludedExchanges(ctx context.Context, userUID string) (map[string]struct{}, error) {
	return s.repo.GetExcludedExchangesByUser(ctx, userUID)
}

// GetExcludedConnectionKeys returns exclusion keys "exchange" or "exchange/label".
func (s *ConnectionService) GetExcludedConnectionKeys(ctx context.Context, userUID string) (map[string]struct{}, error) {
	return s.repo.GetExcludedConnectionKeysByUser(ctx, userUID)
}

// GetExchangeMetadata returns exchange-level metadata for active connections.
func (s *ConnectionService) GetExchangeMetadata(ctx context.Context, userUID string) ([]*ExchangeMetadata, error) {
	details, err := s.repo.GetExchangeDetailsByUser(ctx, userUID)
	if err != nil {
		return nil, err
	}

	out := make([]*ExchangeMetadata, 0, len(details))
	for _, d := range details {
		out = append(out, &ExchangeMetadata{
			Exchange: d.Exchange,
			KYCLevel: d.KYCLevel,
			IsPaper:  d.IsPaper,
		})
	}
	return out, nil
}

// decryptField decrypts a credential field.
// If iv and authTag are present → Go format (3 fields, base64).
// If iv and authTag are empty → TS format (single hex string: iv+tag+ciphertext).
// This allows seamless reading of credentials from both TS and Go enclaves.
func (s *ConnectionService) decryptField(ciphertext, iv, authTag string) (string, error) {
	if iv != "" && authTag != "" {
		// Go format: 3 separate base64 fields
		return s.encryption.DecryptString(&encryption.EncryptedData{
			Ciphertext: ciphertext,
			IV:         iv,
			AuthTag:    authTag,
		})
	}

	// TS format: single hex string (iv_16bytes + tag_16bytes + ciphertext)
	return s.encryption.DecryptTSString(ciphertext)
}

func hashCredentials(apiKey, apiSecret, passphrase string) string {
	input := fmt.Sprintf("%s:%s:%s", apiKey, apiSecret, passphrase)
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func normalizeSyncIntervalMinutes(value int) int {
	if value <= 0 {
		return 1440
	}
	return value
}

func normalizeExchange(exchange string) string {
	return strings.ToLower(strings.TrimSpace(exchange))
}

func normalizeKYCLevel(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "none", "basic", "intermediate", "advanced":
		return strings.ToLower(strings.TrimSpace(level))
	default:
		return ""
	}
}

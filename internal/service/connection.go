package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
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
	// postCreateHook fires after a successful Create. Used to trigger one-shot
	// background work tied to the new connection (e.g. historical snapshot
	// backfill for connectors that support it). Nil = no-op.
	postCreateHook func(ctx context.Context, userUID, exchange, label string)
	// logger is optional; when nil, transient-validation warnings are silent.
	logger *zap.Logger
}

// NewConnectionService creates a new connection service
func NewConnectionService(repo *repository.ConnectionRepo, enc *encryption.Service) *ConnectionService {
	return &ConnectionService{
		repo:       repo,
		encryption: enc,
		factory:    connector.NewFactory(),
	}
}

// SetFactory replaces the connector factory. Used to inject a proxy-aware
// factory after construction (e.g. when EXCHANGE_HTTP_PROXY is configured).
func (s *ConnectionService) SetFactory(f *connector.Factory) {
	s.factory = f
}

// SetPostCreateHook registers a callback invoked asynchronously after a
// connection is successfully created. The hook receives a fresh background
// context (the request context is cancelled by the time the goroutine runs).
// Wired in main.go to SyncService.ReconstructHistoryOnConnect.
func (s *ConnectionService) SetPostCreateHook(fn func(ctx context.Context, userUID, exchange, label string)) {
	s.postCreateHook = fn
}

// SetLogger attaches a zap logger for non-fatal diagnostic events (e.g. saving
// a connection despite a transient upstream validation failure).
func (s *ConnectionService) SetLogger(logger *zap.Logger) {
	s.logger = logger
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
	// RebuildHistory gates the post-create historical reconstruction hook.
	// SEC-ZK-001: for non-IBKR exchanges, "rebuild" sends decrypted credentials
	// to an out-of-perimeter service. We require explicit caller opt-in (frontend
	// toggle, or CLI `"rebuild_history": true`) — the zero value (false) means
	// no rebuild, no plaintext exit. IBKR's in-enclave Flex rebuild is also gated
	// by this flag for consistency, even though it stays inside the ZK perimeter.
	RebuildHistory bool
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
		return fmt.Errorf("invalid credentials: %w", err)
	}
	if err := testConn.TestConnection(ctx); err != nil {
		// Transient upstream failures (busy report generator, rate limit, service
		// hiccup) are NOT credential errors. Save the connection so the daily
		// scheduler retries, and surface the deferred validation in logs.
		if errors.Is(err, connector.ErrTransient) {
			if s.logger != nil {
				s.logger.Warn("upstream validation deferred; saving connection",
					zap.String("user_uid", req.UserUID),
					zap.String("exchange", normalizedExchange),
					zap.String("label", normalizedLabel),
					zap.Error(err),
				)
			}
		} else {
			return fmt.Errorf("invalid credentials: %w", err)
		}
	}

	credentialsHash := hashCredentials(req.APIKey, req.APISecret, req.Passphrase)

	conn := &repository.ExchangeConnection{
		UserUID:             req.UserUID,
		Exchange:            normalizedExchange,
		Label:               normalizedLabel,
		CredentialsHash:     credentialsHash,
		SyncIntervalMinutes: normalizeSyncIntervalMinutes(req.SyncIntervalMinutes),
		ExcludeFromReport:   req.ExcludeFromReport,
	}

	// Encryption format must match the schema. The TS/Prisma schema stores
	// each secret in a single column as hex(iv16||tag16||ciphertext) and has
	// no iv/auth_tag columns; the native Go schema stores ciphertext, iv,
	// auth_tag separately (12-byte nonce, all base64). Picking the wrong
	// one produces rows that fail GCM auth-tag verification on read.
	if s.repo.IsTSSchema(ctx) {
		apiKeyTS, err := s.encryption.EncryptTSString(req.APIKey)
		if err != nil {
			return fmt.Errorf("encrypt api key (ts): %w", err)
		}
		apiSecretTS, err := s.encryption.EncryptTSString(req.APISecret)
		if err != nil {
			return fmt.Errorf("encrypt api secret (ts): %w", err)
		}
		conn.EncryptedAPIKey = apiKeyTS
		conn.EncryptedAPISecret = apiSecretTS
		if req.Passphrase != "" {
			passTS, err := s.encryption.EncryptTSString(req.Passphrase)
			if err != nil {
				return fmt.Errorf("encrypt passphrase (ts): %w", err)
			}
			conn.EncryptedPassphrase = passTS
		}
	} else {
		apiKeyEnc, err := s.encryption.EncryptString(req.APIKey)
		if err != nil {
			return fmt.Errorf("encrypt api key: %w", err)
		}
		apiSecretEnc, err := s.encryption.EncryptString(req.APISecret)
		if err != nil {
			return fmt.Errorf("encrypt api secret: %w", err)
		}
		conn.EncryptedAPIKey = apiKeyEnc.Ciphertext
		conn.APIKeyIV = apiKeyEnc.IV
		conn.APIKeyAuthTag = apiKeyEnc.AuthTag
		conn.EncryptedAPISecret = apiSecretEnc.Ciphertext
		conn.APISecretIV = apiSecretEnc.IV
		conn.APISecretAuthTag = apiSecretEnc.AuthTag
		if req.Passphrase != "" {
			passphraseEnc, err := s.encryption.EncryptString(req.Passphrase)
			if err != nil {
				return fmt.Errorf("encrypt passphrase: %w", err)
			}
			conn.EncryptedPassphrase = passphraseEnc.Ciphertext
			conn.PassphraseIV = passphraseEnc.IV
			conn.PassphraseAuthTag = passphraseEnc.AuthTag
		}
	}

	if err := s.repo.Create(ctx, conn); err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) {
			return fmt.Errorf("%w: %s", ErrConnectionAlreadyExists, ExistingConnectionNoopMessage)
		}
		return err
	}

	// TS parity: capture exchange metadata (KYC level + paper/live status)
	// after successful connection creation; failures are non-blocking.
	// Reuse testConn — it already has cached state from TestConnection (e.g. IBKR paper detection).
	s.captureExchangeMetadata(ctx, conn.ID, testConn)

	// Fire-and-forget post-create hook (historical snapshot backfill).
	// Detached context — the request context dies when the HTTP response is
	// sent and historical reconstruction can run for tens of seconds.
	// SEC-ZK-001: only fire when the caller explicitly opts in. For non-IBKR
	// the hook ships plaintext credentials to an external service; the
	// default-false stance ensures terminals/CLIs that don't pass the field
	// don't trigger that side effect silently.
	if s.postCreateHook != nil && req.RebuildHistory {
		go s.postCreateHook(context.Background(), conn.UserUID, conn.Exchange, conn.Label)
	}

	return nil
}

func (s *ConnectionService) captureExchangeMetadata(ctx context.Context, connectionID string, exchangeConn connector.Connector) {
	if s.repo == nil || strings.TrimSpace(connectionID) == "" || exchangeConn == nil {
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

// GetActiveConnectionByLabel returns a single active connection (encrypted)
// matched by (user, exchange, label) using a TRIM-tolerant exact lookup.
// Useful for callers that already know the exact connection — avoids the
// fetch-all-then-filter dance of GetActiveConnections + iterate.
func (s *ConnectionService) GetActiveConnectionByLabel(ctx context.Context, userUID, exchange, label string) (*repository.ExchangeConnection, error) {
	return s.repo.GetByUserExchangeLabel(ctx, userUID, normalizeExchange(exchange), strings.TrimSpace(label))
}

// GetExcludedExchanges returns exchanges marked as excluded from reports/analytics.
func (s *ConnectionService) GetExcludedExchanges(ctx context.Context, userUID string) (map[string]struct{}, error) {
	return s.repo.GetExcludedExchangesByUser(ctx, userUID)
}

// GetExcludedConnectionKeys returns exclusion keys "exchange" or "exchange/label".
func (s *ConnectionService) GetExcludedConnectionKeys(ctx context.Context, userUID string) (map[string]struct{}, error) {
	return s.repo.GetExcludedConnectionKeysByUser(ctx, userUID)
}

// ListUnfinalizedExternalRebuilds passes through to ConnectionRepo. Used by
// the daily SyncScheduler's midnight recalibration pass to find connections
// whose external-rebuilder history hasn't been re-anchored on a midnight
// snapshot equity yet. See the repo method docstring for filtering rules.
func (s *ConnectionService) ListUnfinalizedExternalRebuilds(ctx context.Context, beforeCutoff time.Time, exchanges []string) ([]*repository.ExchangeConnection, error) {
	return s.repo.ListUnfinalizedExternalRebuilds(ctx, beforeCutoff, exchanges)
}

// MarkRebuildFinalized passes through to ConnectionRepo. Stamps the connection
// once its rebuilt history has been recalibrated against the midnight snapshot,
// so subsequent nightly ticks skip it.
func (s *ConnectionService) MarkRebuildFinalized(ctx context.Context, connID string, at time.Time) error {
	return s.repo.MarkRebuildFinalized(ctx, connID, at)
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

// PersistOAuthTokens re-encrypts refreshed access/refresh tokens and writes
// them back to the connection row for (userUID, exchange, label). Called by
// the cTrader connector's token persister after a successful OAuth refresh so
// the next boot does not start with an already-expired access_token.
func (s *ConnectionService) PersistOAuthTokens(ctx context.Context, userUID, exchange, label, accessToken, refreshToken string) error {
	encAccess, err := s.encryption.EncryptTSString(accessToken)
	if err != nil {
		return fmt.Errorf("encrypt access token: %w", err)
	}
	encRefresh, err := s.encryption.EncryptTSString(refreshToken)
	if err != nil {
		return fmt.Errorf("encrypt refresh token: %w", err)
	}
	return s.repo.UpdateOAuthTokens(ctx, userUID, exchange, label, encAccess, encRefresh)
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
	e := strings.ToLower(strings.TrimSpace(exchange))
	// Normalize broker aliases to their underlying platform name
	switch e {
	case "exness":
		return "mt5"
	case "binanceusdm":
		return "binance_futures"
	}
	return e
}

func normalizeKYCLevel(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "none", "basic", "intermediate", "advanced":
		return strings.ToLower(strings.TrimSpace(level))
	default:
		return ""
	}
}

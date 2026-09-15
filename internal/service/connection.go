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
	"go.uber.org/zap/zapcore"
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
	// postCreateSyncHook fires after EVERY successful Create. It takes the
	// connection's first live snapshot and writes its sync_statuses row.
	// Nil = no-op.
	postCreateSyncHook func(ctx context.Context, userUID, exchange, label string)
	// postCreateRebuildHook fires only when the caller opted into a history
	// rebuild (SEC-ZK-001 / SEC-08). Nil = no-op.
	postCreateRebuildHook func(ctx context.Context, userUID, exchange, label string)
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

// SetPostCreateSyncHook registers the callback that takes a new connection's
// FIRST live snapshot. It runs after every successful Create, whatever the
// caller asked about history.
//
// G-H7 / C7: this used to sit behind req.RebuildHistory together with the
// history rebuild, on the assumption — written in main.go — that "frontend
// only sends false for mt5". The cTrader OAuth callback sends nothing at all,
// so it defaulted to false and the whole hook was skipped: a new cTrader
// connection had no snapshot, no sync_statuses row and no status anywhere in
// the admin until the 00:00 UTC pass. That is exactly the cold-start incident
// of 2026-08-04 this hook was added to fix, re-entered through a different
// door. Taking a snapshot is not credential egress, so nothing gates it.
//
// The hook receives a fresh background context (the request context is
// cancelled by the time the goroutine runs).
func (s *ConnectionService) SetPostCreateSyncHook(fn func(ctx context.Context, userUID, exchange, label string)) {
	s.postCreateSyncHook = fn
}

// SetPostCreateRebuildHook registers the historical-reconstruction callback.
// It runs ONLY when the caller set RebuildHistory: for non-IBKR exchanges the
// rebuild ships decrypted credentials to a service outside the enclave
// perimeter (SEC-ZK-001), and the opt-in is persisted before it fires so the
// nightly recalibration can scope its egress the same way (SEC-08).
func (s *ConnectionService) SetPostCreateRebuildHook(fn func(ctx context.Context, userUID, exchange, label string)) {
	s.postCreateRebuildHook = fn
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
		// C3: for an OAuth broker, "connect again" is how a user recovers a
		// dead authorization — the label is fixed by the callback, so the
		// second attempt always lands on the existing row. Answering
		// already-exists made Reconnect a no-op the whole stack then reported
		// as a success: the gateway 200'd, the frontend said "connected",
		// the Neon mirror was reset to pending/no-error, and the new tokens
		// were dropped on the floor. The only cure was delete-then-reconnect,
		// and nothing in the UI said so.
		if isOAuthExchange(normalizedExchange) {
			return s.reauthorizeOAuthConnection(ctx, existing, req, normalizedExchange)
		}
		return fmt.Errorf("%w: %s", ErrConnectionAlreadyExists, ExistingConnectionNoopMessage)
	}

	// The label dedup above cannot catch the same account arriving twice under
	// different labels — same credentials, cosmetically different names. Both
	// rows would sync the same exchange account and the report aggregation
	// would sum its equity twice. Identical credentials are the reliable proxy
	// for "same account" (IG multi-account logins differ in the pinned account
	// id, which rides in the passphrase and therefore in the hash).
	credentialsHash := hashCredentials(req.APIKey, req.APISecret, req.Passphrase)
	if dup, derr := s.repo.ExistsActiveByCredentialsHash(ctx, req.UserUID, credentialsHash); derr != nil {
		return fmt.Errorf("check duplicate credentials: %w", derr)
	} else if dup {
		return fmt.Errorf("%w: these exact credentials are already connected under another label", ErrConnectionAlreadyExists)
	}

	// Test credentials before saving — fail fast with a clear error
	testConn, err := s.factory.Create(&connector.Credentials{
		Exchange:   normalizedExchange,
		APIKey:     req.APIKey,
		APISecret:  req.APISecret,
		Passphrase: req.Passphrase,
	})
	if err != nil {
		// G-H4: a factory refusal is an unsupported/unconfigured exchange —
		// nothing the caller can fix by re-entering a secret.
		return fmt.Errorf("create connector: %w", err)
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
		} else if connector.IsIPRestriction(err) {
			// Not a credential failure: the venue accepted the key and refused
			// the source address. Saying "invalid credentials" here sends the
			// holder off to regenerate a key that was never the problem, and
			// the replacement fails identically.
			return fmt.Errorf("%w: %w", connector.ErrIPRestricted, err)
		} else if kind := connector.ClassifyConnectFailure(err); kind != connector.ConnectFailureCredentials {
			// G-H4: same reasoning, generalised. "invalid credentials: " is
			// the FIRST entry of the errsanitize table, so prefixing it onto
			// a Spotware outage, a missing CTRADER_CLIENT_ID, a login with no
			// trading account or a 429 turned all four into a 400 telling the
			// user to check credentials they cannot correct — for cTrader they
			// have just completed a successful OAuth login and hold no secret
			// at all. Return the cause unprefixed and let errsanitize pick the
			// category it now has for each of them.
			if kind == connector.ConnectFailureUpstream && isOAuthExchange(normalizedExchange) {
				// The broker never gave a verdict, and for an OAuth
				// connection the user cannot simply retry: re-running the
				// flow means going back through the broker's consent screen.
				// The tokens they just obtained are valid, so keep the
				// connection and let the daily scheduler validate it — the
				// same trade already made for ErrTransient above.
				if s.logger != nil {
					s.logger.Warn("broker unreachable at connect; saving the OAuth connection for the daily retry",
						zap.String("user_uid", req.UserUID),
						zap.String("exchange", normalizedExchange),
						zap.String("label", normalizedLabel),
						zap.Error(err),
					)
				}
			} else {
				return err
			}
		} else {
			return fmt.Errorf("invalid credentials: %w", err)
		}
	}

	// E-H4: store what the connector is HOLDING, not what the request carried.
	// TestConnection can itself have refreshed, and cTrader rotates the
	// refresh token every time it does — storing the request's pair wrote a
	// refresh token the broker had already invalidated, so the connection was
	// born dead and failed its first nightly sync with ACCESS_DENIED.
	storedAPIKey, storedAPISecret := effectiveOAuthCredentials(testConn, req.APIKey, req.APISecret)
	if storedAPIKey != req.APIKey || storedAPISecret != req.APISecret {
		credentialsHash = hashCredentials(storedAPIKey, storedAPISecret, req.Passphrase)
	}

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
		apiKeyTS, err := s.encryption.EncryptTSString(storedAPIKey)
		if err != nil {
			return fmt.Errorf("encrypt api key (ts): %w", err)
		}
		apiSecretTS, err := s.encryption.EncryptTSString(storedAPISecret)
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
		apiKeyEnc, err := s.encryption.EncryptString(storedAPIKey)
		if err != nil {
			return fmt.Errorf("encrypt api key: %w", err)
		}
		apiSecretEnc, err := s.encryption.EncryptString(storedAPISecret)
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
	s.CaptureExchangeMetadata(ctx, conn.ID, testConn)

	// DetectIsPaper runs inside the call above and can refresh too, AFTER the
	// row was written. Catch that last rotation as well (E-H4).
	if finalKey, finalSecret := effectiveOAuthCredentials(testConn, storedAPIKey, storedAPISecret); finalKey != storedAPIKey || finalSecret != storedAPISecret {
		if err := s.PersistOAuthTokens(ctx, conn.UserUID, conn.Exchange, conn.Label, finalKey, finalSecret); err != nil && s.logger != nil {
			s.logger.Error("tokens rotated during connection setup could not be stored; the connection will need re-authorization",
				zap.String("user_uid", conn.UserUID),
				zap.String("exchange", conn.Exchange),
				zap.Error(err),
			)
		} else if err == nil {
			_ = s.repo.UpdateCredentialsHash(ctx, conn.ID, hashCredentials(finalKey, finalSecret, req.Passphrase))
		}
	}

	// The history rebuild stays behind the explicit opt-in.
	// SEC-ZK-001: for non-IBKR the hook ships plaintext credentials to an
	// external service; the default-false stance ensures terminals/CLIs that
	// don't pass the field don't trigger that side effect silently.
	rebuild := s.postCreateRebuildHook != nil && req.RebuildHistory
	if rebuild {
		// SEC-08: persist the opt-in BEFORE firing the hook so the nightly
		// recalibration pass can scope decrypted-credential egress to
		// connections that explicitly consented. Without this durable record
		// the pass would re-egress credentials for every active external-
		// rebuilder connection regardless of consent. Non-fatal: a missing
		// record only means recalibration skips this connection (the safe
		// direction).
		if err := s.repo.MarkRebuildRequested(ctx, conn.ID, time.Now().UTC()); err != nil && s.logger != nil {
			s.logger.Warn("failed to persist rebuild opt-in; nightly recalibration will skip this connection",
				zap.String("user_uid", conn.UserUID),
				zap.String("exchange", conn.Exchange),
				zap.Error(err),
			)
		}
	}

	s.dispatchPostCreateHooks(conn.UserUID, conn.Exchange, conn.Label, rebuild)

	return nil
}

// dispatchPostCreateHooks runs the post-create work on a detached context —
// the request context dies when the response is sent, and both hooks can run
// for tens of seconds.
//
// The first live snapshot runs for EVERY new connection (G-H7): it is a read
// of the account we were just given credentials for, it crosses no perimeter,
// and without it the connection has no today-row and no sync_statuses entry
// until the 00:00 pass. Only the history rebuild is gated on `rebuild`.
//
// Both hooks share ONE goroutine so their order is preserved: the snapshot the
// sync writes is the equity anchor the rebuild dispatch reads
// (EndEquityOverride, 2026-08-04).
func (s *ConnectionService) dispatchPostCreateHooks(userUID, exchange, label string, rebuild bool) {
	runRebuild := rebuild && s.postCreateRebuildHook != nil
	if s.postCreateSyncHook == nil && !runRebuild {
		return
	}
	go func() {
		hookCtx := context.Background()
		if s.postCreateSyncHook != nil {
			s.postCreateSyncHook(hookCtx, userUID, exchange, label)
		}
		if runRebuild {
			s.postCreateRebuildHook(hookCtx, userUID, exchange, label)
		}
	}()
}

// reauthorizeOAuthConnection replaces the stored tokens of an ACTIVE OAuth
// connection with the ones the user just obtained, instead of refusing the
// call as a duplicate (C3). The new tokens are validated first: an
// authorization that does not work must not overwrite one that might.
//
// It deliberately does NOT go through the duplicate-credentials guard: OAuth
// re-issues different tokens for the same account every time, so that hash
// says nothing about identity here. The stored hash is refreshed so it keeps
// describing what the row actually holds.
func (s *ConnectionService) reauthorizeOAuthConnection(
	ctx context.Context,
	existing *repository.ExchangeConnection,
	req *CreateConnectionRequest,
	normalizedExchange string,
) error {
	testConn, err := s.factory.Create(&connector.Credentials{
		Exchange:   normalizedExchange,
		APIKey:     req.APIKey,
		APISecret:  req.APISecret,
		Passphrase: req.Passphrase,
	})
	if err != nil {
		return fmt.Errorf("create connector: %w", err)
	}

	if err := testConn.TestConnection(ctx); err != nil {
		if fatal := reauthValidationOutcome(err); fatal != nil {
			return fatal
		}
		if s.logger != nil {
			s.logger.Warn("broker unreachable while re-authorizing; storing the new tokens anyway",
				zap.String("user_uid", existing.UserUID),
				zap.String("exchange", normalizedExchange),
				zap.String("label", existing.Label),
				zap.Error(err),
			)
		}
	}

	// E-H4: store what the connector is HOLDING. Validation can itself have
	// refreshed, and cTrader rotates the refresh token every time it does.
	accessToken, refreshToken := effectiveOAuthCredentials(testConn, req.APIKey, req.APISecret)

	if err := s.PersistOAuthTokens(ctx, existing.UserUID, normalizedExchange, existing.Label, accessToken, refreshToken); err != nil {
		return fmt.Errorf("persist re-authorized tokens: %w", err)
	}
	if err := s.repo.UpdateCredentialsHash(ctx, existing.ID, hashCredentials(accessToken, refreshToken, req.Passphrase)); err != nil && s.logger != nil {
		s.logger.Warn("re-authorized connection: credentials hash not updated",
			zap.String("user_uid", existing.UserUID),
			zap.String("exchange", normalizedExchange),
			zap.Error(err),
		)
	}

	// The account behind the authorization can have changed (live vs demo);
	// re-read the metadata rather than keeping the old row's.
	s.CaptureExchangeMetadata(ctx, existing.ID, testConn)

	if s.logger != nil {
		s.logger.Info("OAuth connection re-authorized",
			zap.String("user_uid", existing.UserUID),
			zap.String("exchange", normalizedExchange),
			zap.String("label", existing.Label),
		)
	}

	// Take a fresh snapshot straight away: re-authorizing exists to end an
	// outage, and the sync_statuses row it writes is what clears the failure
	// the user came here to fix. No rebuild — nothing was newly connected.
	s.dispatchPostCreateHooks(existing.UserUID, existing.Exchange, existing.Label, false)
	return nil
}

// reauthValidationOutcome decides what a failed validation means during a
// re-authorization: nil to store the new tokens anyway, or the error to
// answer with.
//
// The asymmetry with a first connect is deliberate. Here we already hold a
// pair we have reason to believe is dead — that is why the user is back — and
// the pair they just obtained came out of a successful consent screen. When
// the broker gives no verdict (outage, timeout, throttle) the new pair is the
// better bet, so it is written and the daily pass finds out. Only an actual
// refusal by the broker, or a failure nobody has characterised, keeps the old
// tokens in place: overwriting a working authorization with a broken one
// would be the worse mistake.
func reauthValidationOutcome(err error) error {
	if err == nil {
		return nil
	}
	if connector.IsIPRestriction(err) {
		return fmt.Errorf("%w: %w", connector.ErrIPRestricted, err)
	}
	if errors.Is(err, connector.ErrTransient) {
		return nil
	}
	switch connector.ClassifyConnectFailure(err) {
	case connector.ConnectFailureUpstream:
		return nil
	case connector.ConnectFailureCredentials:
		return fmt.Errorf("invalid credentials: %w", err)
	default:
		return err
	}
}

// effectiveOAuthCredentials returns the tokens the connector currently holds,
// falling back to the ones supplied when it cannot say (E-H4).
func effectiveOAuthCredentials(conn connector.Connector, fallbackAccess, fallbackRefresh string) (string, string) {
	source, ok := conn.(connector.OAuthCredentialSource)
	if !ok {
		return fallbackAccess, fallbackRefresh
	}
	access, refresh := source.CurrentCredentials()
	if strings.TrimSpace(access) == "" {
		access = fallbackAccess
	}
	if strings.TrimSpace(refresh) == "" {
		refresh = fallbackRefresh
	}
	return access, refresh
}

// MetadataCapture is the outcome of one metadata probe. PaperProbed
// distinguishes a stored "live" from a connector that has no paper detector at
// all — both leave IsPaper false, and reading the second as the first is what
// lets a demo account pass for a funded one.
type MetadataCapture struct {
	KYCLevel    string
	IsPaper     bool
	PaperProbed bool
	Err         error
}

// CaptureExchangeMetadata reads paper/live and KYC status from the broker and
// stores it against the connection. The two probes are independent, so a
// non-nil Err can accompany a value stored by the other one. A failed store is
// logged at Error rather than dropped: the report's paper badge reads this
// column, so losing the write silently presents a demo account as a funded one.
func (s *ConnectionService) CaptureExchangeMetadata(ctx context.Context, connectionID string, exchangeConn connector.Connector) MetadataCapture {
	var out MetadataCapture
	if s.repo == nil || strings.TrimSpace(connectionID) == "" || exchangeConn == nil {
		return out
	}

	var failures []error

	if fetcher, ok := exchangeConn.(connector.KYCLevelFetcher); ok {
		kycLevel, err := fetcher.FetchKYCLevel(ctx)
		if err != nil {
			s.logMetadataIssue(zapcore.WarnLevel, "kyc level not read from broker", connectionID, exchangeConn, err)
			failures = append(failures, fmt.Errorf("read kyc level: %w", err))
		} else if normalized := normalizeKYCLevel(kycLevel); normalized != "" {
			if err := s.repo.UpdateKYCLevel(ctx, connectionID, normalized); err != nil {
				s.logMetadataIssue(zapcore.ErrorLevel, "kyc level detected but not stored", connectionID, exchangeConn, err)
				failures = append(failures, fmt.Errorf("store kyc level: %w", err))
			} else {
				out.KYCLevel = normalized
			}
		}
	}

	if detector, ok := exchangeConn.(connector.PaperAccountDetector); ok {
		isPaper, err := detector.DetectIsPaper(ctx)
		if err != nil {
			s.logMetadataIssue(zapcore.WarnLevel, "paper/live status not read from broker", connectionID, exchangeConn, err)
			failures = append(failures, fmt.Errorf("detect paper account: %w", err))
		} else if err := s.repo.UpdateIsPaper(ctx, connectionID, isPaper); err != nil {
			s.logMetadataIssue(zapcore.ErrorLevel, "paper/live status detected but not stored", connectionID, exchangeConn, err)
			failures = append(failures, fmt.Errorf("store paper account: %w", err))
		} else {
			out.IsPaper = isPaper
			out.PaperProbed = true
		}
	}

	out.Err = errors.Join(failures...)
	return out
}

func (s *ConnectionService) logMetadataIssue(level zapcore.Level, msg, connectionID string, exchangeConn connector.Connector, err error) {
	if s.logger == nil {
		return
	}
	s.logger.Log(level, msg,
		zap.String("connection_id", connectionID),
		zap.String("exchange", exchangeConn.Exchange()),
		zap.Error(err),
	)
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
func (s *ConnectionService) ListUnfinalizedExternalRebuilds(ctx context.Context, beforeCutoff, createdAfter time.Time, exchanges []string) ([]*repository.ExchangeConnection, error) {
	return s.repo.ListUnfinalizedExternalRebuilds(ctx, beforeCutoff, createdAfter, exchanges)
}

// MarkRebuildFinalized passes through to ConnectionRepo. Stamps the connection
// once its rebuilt history has been recalibrated against the midnight snapshot,
// so subsequent nightly ticks skip it.
func (s *ConnectionService) MarkRebuildFinalized(ctx context.Context, connID string, at time.Time) error {
	return s.repo.MarkRebuildFinalized(ctx, connID, at)
}

// ClearRebuildConsent passes through to ConnectionRepo.
func (s *ConnectionService) ClearRebuildConsent(ctx context.Context, connID string) error {
	return s.repo.ClearRebuildConsent(ctx, connID)
}

// GetHistoryTarget passes through to ConnectionRepo.
func (s *ConnectionService) GetHistoryTarget(ctx context.Context, userUID, exchange, label string) (*repository.RebuiltHistoryTarget, error) {
	return s.repo.GetHistoryTarget(ctx, userUID, normalizeExchange(exchange), strings.TrimSpace(label))
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

// isOAuthExchange reports whether a connection's credentials are OAuth tokens
// the user obtained through a broker consent screen rather than a key they
// typed. Re-running that flow is expensive enough that an upstream outage at
// connect should not throw the tokens away.
func isOAuthExchange(exchange string) bool {
	return exchange == "ctrader"
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

package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/attestation"
	"github.com/trackrecord/enclave/internal/auth"
	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	tlspkg "github.com/trackrecord/enclave/internal/tls"
	"github.com/trackrecord/enclave/internal/validation"
	"go.uber.org/zap"
)

// resolveUserUID returns the authenticated caller UID (AUTH-001). When
// jwtRequired has verified a bearer token it injects claims.Sub into ctx via
// auth.WithUserUID; we prefer that value over whatever the client wrote in
// the request body. The body-supplied value is used only in dev mode (when
// ENCLAVE_JWT_SECRET is unset and jwtRequired skips auth entirely).
func resolveUserUID(ctx context.Context, bodyUID string) string {
	if uid, ok := auth.UserUIDFromContext(ctx); ok {
		return uid
	}
	return bodyUID
}

// genericInternalError is returned to clients in production paths where
// the underlying error message could leak SQL, file paths, or stack
// fragments (SRV-001). Mirrors the gRPC sanitizer behaviour.
const genericInternalError = "internal server error"

// sanitizeErr returns a client-safe rendering of err. In production
// (ENV=production) every error collapses to genericInternalError so
// pgx/SQL/file-path details never leak; in development the original
// message survives so operators can debug locally. The full error is
// always logged at the call site via h.logger.Error before this is
// invoked, so diagnostics are not lost.
func (h *Handler) sanitizeErr(err error) string {
	if err == nil {
		return ""
	}
	if isProduction() {
		return genericInternalError
	}
	return err.Error()
}

// isProduction mirrors the gRPC server check so the REST sanitizer uses
// the same toggle.
func isProduction() bool {
	env := strings.ToLower(os.Getenv("ENV"))
	nodeEnv := strings.ToLower(os.Getenv("NODE_ENV"))
	return env == "production" || nodeEnv == "production"
}

// QUAL-001: error / status messages reused across REST handlers. Extracted
// as constants so SonarQube's go:S1192 stops complaining and so any wording
// drift between handlers becomes a structural impossibility.
const (
	msgMethodNotAllowed     = "method not allowed"
	msgInvalidRequestBody   = "invalid request body"
	msgUserUIDRequired      = "user_uid is required"
	msgFailedLoadExclusions = "failed to load exclusion rules"
)

type Handler struct {
	logger       *zap.Logger
	connSvc      connectionService
	syncSvc      *service.SyncService
	metricsSvc   *service.MetricsService
	reportSvc    *service.ReportService
	snapshotRepo *repository.SnapshotRepo
	userRepo     *repository.UserRepo

	// New services for attestation, TLS, and E2E encryption
	tlsKeygen *tlspkg.KeyGenerator
	attestSvc *attestation.Service
	eciesSvc  *encryption.ECIESService
}

type HandlerOptions struct {
	Logger       *zap.Logger
	ConnSvc      connectionService
	SyncSvc      *service.SyncService
	MetricsSvc   *service.MetricsService
	ReportSvc    *service.ReportService
	SnapshotRepo *repository.SnapshotRepo
	UserRepo     *repository.UserRepo
	TLSKeygen    *tlspkg.KeyGenerator
	AttestSvc    *attestation.Service
	ECIESSvc     *encryption.ECIESService
}

type connectionService interface {
	Create(ctx context.Context, req *service.CreateConnectionRequest) error
	GetExcludedConnectionKeys(ctx context.Context, userUID string) (map[string]struct{}, error)
}

func NewHandler(
	logger *zap.Logger,
	connSvc connectionService,
	syncSvc *service.SyncService,
	metricsSvc *service.MetricsService,
	reportSvc *service.ReportService,
	snapshotRepo *repository.SnapshotRepo,
	userRepo *repository.UserRepo,
) *Handler {
	return &Handler{
		logger:       logger,
		connSvc:      connSvc,
		syncSvc:      syncSvc,
		metricsSvc:   metricsSvc,
		reportSvc:    reportSvc,
		snapshotRepo: snapshotRepo,
		userRepo:     userRepo,
	}
}

// NewHandlerWithOptions creates a handler with all optional services.
func NewHandlerWithOptions(opts HandlerOptions) *Handler {
	return &Handler{
		logger:       opts.Logger,
		connSvc:      opts.ConnSvc,
		syncSvc:      opts.SyncSvc,
		metricsSvc:   opts.MetricsSvc,
		reportSvc:    opts.ReportSvc,
		snapshotRepo: opts.SnapshotRepo,
		userRepo:     opts.UserRepo,
		tlsKeygen:    opts.TLSKeygen,
		attestSvc:    opts.AttestSvc,
		eciesSvc:     opts.ECIESSvc,
	}
}

// GetTLSFingerprint - GET /api/v1/tls/fingerprint
func (h *Handler) GetTLSFingerprint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	if h.tlsKeygen == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "TLS not initialized",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"fingerprint": h.tlsKeygen.Fingerprint(),
		"algorithm":   "SHA-256",
		"usage":       "Compare with attestation report to verify TLS cert authenticity",
	})
}

// GetAttestation serves /api/v1/attestation.
//
//   - GET returns the cached (5s) attestation quote without freshness guarantees;
//     safe only for same-TLS-session use.
//   - POST with body {"nonce":"<hex>"} (1..64 bytes) returns a fresh quote whose
//     report_data is SHA-256(tls_fp || e2e_pk || signing_pk || nonce). The nonce
//     proves freshness against replay (SEC-101). Never cached.
func (h *Handler) GetAttestation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	if h.attestSvc == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "attestation service not configured",
		})
		return
	}

	var (
		report *attestation.AttestationReport
		err    error
	)
	if r.Method == http.MethodPost {
		var body struct {
			Nonce string `json:"nonce"`
		}
		if err := readJSON(w, r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": msgInvalidRequestBody})
			return
		}
		nonceHex := strings.TrimSpace(body.Nonce)
		if nonceHex == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "nonce is required (hex string)"})
			return
		}
		nonce, decErr := hex.DecodeString(nonceHex)
		if decErr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "nonce must be a hex string"})
			return
		}
		if len(nonce) == 0 || len(nonce) > 64 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "nonce must be 1..64 bytes after hex decode"})
			return
		}
		report, err = h.attestSvc.GetAttestationWithNonce(r.Context(), nonce)
	} else {
		report, err = h.attestSvc.GetAttestation(r.Context())
	}
	if err != nil {
		h.logger.Error("attestation failed", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error": "attestation failed",
		})
		return
	}

	var (
		verified                 bool
		sevSnpEnabled            bool
		vcekVerified             bool
		reportDataBoundToRequest bool
		measurement              string
		reportData               string
		platformVersion          string
	)
	if report.Attestation != nil {
		verified = report.Attestation.Verified
		sevSnpEnabled = report.Attestation.SevSnpEnabled
		vcekVerified = report.Attestation.VcekVerified
		reportDataBoundToRequest = report.Attestation.ReportDataBoundToRequest
		measurement = report.Attestation.Measurement
		reportData = report.Attestation.ReportData
		platformVersion = report.Attestation.PlatformVersion
	}

	e2ePublicKey := ""
	if report.E2EEncryption != nil {
		e2ePublicKey = report.E2EEncryption.PublicKey
	}
	e2ePublicKeyFingerprint := ""
	if e2ePublicKey != "" {
		sum := sha256.Sum256([]byte(e2ePublicKey))
		e2ePublicKeyFingerprint = hex.EncodeToString(sum[:])
	}

	reportSigningPublicKey := ""
	reportSigningAlgorithm := ""
	if report.ReportSigning != nil {
		reportSigningPublicKey = report.ReportSigning.PublicKey
		reportSigningAlgorithm = report.ReportSigning.Algorithm
	}
	reportSigningFingerprint := ""
	if reportSigningPublicKey != "" {
		sum := sha256.Sum256([]byte(reportSigningPublicKey))
		full := hex.EncodeToString(sum[:])
		if len(full) > 16 {
			reportSigningFingerprint = full[:16]
		} else {
			reportSigningFingerprint = full
		}
	}

	tlsFingerprint := ""
	if report.TLSBinding != nil {
		tlsFingerprint = report.TLSBinding.Fingerprint
	}
	if tlsFingerprint == "" && h.tlsKeygen != nil {
		tlsFingerprint = h.tlsKeygen.Fingerprint()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"attestation": map[string]any{
			"verified":                 verified,
			"sevSnpEnabled":            sevSnpEnabled,
			"vcekVerified":             vcekVerified,
			"reportDataBoundToRequest": reportDataBoundToRequest,
			"measurement":              measurement,
			"reportData":               reportData,
			"platformVersion":          platformVersion,
		},
		"tlsBinding": map[string]any{
			"fingerprint":  tlsFingerprint,
			"algorithm":    "SHA-256",
			"bound":        reportData != "",
			"verification": "reportData should equal SHA-256(TLS certificate)",
		},
		"e2eEncryption": map[string]any{
			"publicKey":            e2ePublicKey,
			"publicKeyFingerprint": e2ePublicKeyFingerprint,
			"algorithm":            "ECIES (ECDH P-256 + AES-256-GCM)",
			"usage":                "Encrypt credentials with this key before sending for maximum security",
		},
		"reportSigning": map[string]any{
			"publicKey":            reportSigningPublicKey,
			"publicKeyFingerprint": reportSigningFingerprint,
			"algorithm":            reportSigningAlgorithm,
			"usage":                "Verify track record reports are signed by this enclave",
		},
		"security": map[string]any{
			"tlsMitmProtection": reportData != "",
			"e2eMitmProtection": e2ePublicKey != "",
			"message": func() string {
				if reportData != "" {
					return "Double encryption: TLS for transport + E2E for application layer"
				}
				return "WARNING: TLS fingerprint not bound - MITM possible"
			}(),
		},
	})
}

// ConnectCredentials - POST /api/v1/credentials/connect
// Accepts E2E encrypted credentials and creates a connection.
func (h *Handler) ConnectCredentials(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	if h.eciesSvc == nil || h.connSvc == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false,
			"error":   "E2E encryption or connection service not configured",
		})
		return
	}

	var req struct {
		UserUID   string `json:"user_uid"`
		Exchange  string `json:"exchange"`
		Label     string `json:"label"`
		Encrypted *struct {
			EphemeralPublicKey string `json:"ephemeralPublicKey"`
			IV                 string `json:"iv"`
			Ciphertext         string `json:"ciphertext"`
			Tag                string `json:"tag"`
		} `json:"encrypted"`
		ExcludeFromReport bool `json:"exclude_from_report"`
	}

	if err := readJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   msgInvalidRequestBody,
		})
		return
	}

	if req.Encrypted == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "E2E encryption required. Plaintext credentials are not accepted.",
			"hint":    "Fetch /api/v1/attestation to get the E2E public key, then encrypt credentials client-side before submission.",
		})
		return
	}

	// AUTH-001: prefer the JWT-verified uid over the body-supplied one.
	userUID := resolveUserUID(r.Context(), req.UserUID)

	if userUID == "" || req.Exchange == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "user_uid and exchange are required",
		})
		return
	}

	if req.Encrypted.EphemeralPublicKey == "" || req.Encrypted.IV == "" || req.Encrypted.Ciphertext == "" || req.Encrypted.Tag == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "encrypted payload must include ephemeralPublicKey, iv, ciphertext, and tag",
		})
		return
	}

	// TS sends ephemeralPublicKey as PEM; ECIES parser handles PEM and raw bytes.
	ephPubKeyBytes := []byte(req.Encrypted.EphemeralPublicKey)

	ivBytes, err := hexDecode(req.Encrypted.IV)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "invalid iv"})
		return
	}
	ciphertextBytes, err := hexDecode(req.Encrypted.Ciphertext)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "invalid ciphertext"})
		return
	}
	tagBytes, err := hexDecode(req.Encrypted.Tag)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "invalid tag"})
		return
	}
	ciphertextWithTag := make([]byte, 0, len(ciphertextBytes)+len(tagBytes))
	ciphertextWithTag = append(ciphertextWithTag, ciphertextBytes...)
	ciphertextWithTag = append(ciphertextWithTag, tagBytes...)

	// Decrypt inside enclave
	plaintext, err := h.eciesSvc.Decrypt(ephPubKeyBytes, ivBytes, ciphertextWithTag)
	if err != nil {
		h.logger.Error("E2E decryption failed", zap.Error(err))
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "Failed to decrypt credentials. Ensure you are using the correct E2E public key from /api/v1/attestation.",
		})
		return
	}

	// Parse decrypted credentials
	var creds struct {
		APIKey     string  `json:"api_key"`
		APISecret  *string `json:"api_secret"`
		Passphrase *string `json:"passphrase"`
	}
	if err := json.Unmarshal(plaintext, &creds); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "Decrypted payload is not valid JSON. Expected: { api_key, api_secret, passphrase? }",
		})
		return
	}
	if creds.APIKey == "" || creds.APISecret == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "Decrypted payload missing required fields: api_key and api_secret",
		})
		return
	}

	apiSecret := *creds.APISecret
	passphrase := ""
	if creds.Passphrase != nil {
		passphrase = *creds.Passphrase
	}

	label := strings.TrimSpace(req.Label)
	if label == "" {
		label = strings.TrimSpace(req.Exchange) + " account"
	}

	// Upsert user
	if h.userRepo != nil {
		if _, err := h.userRepo.GetOrCreate(r.Context(), userUID); err != nil {
			h.logger.Error("user upsert failed", zap.Error(err))
		}
	}

	// Create connection
	if err := validation.ValidateCreateConnection(&validation.CreateConnectionRequest{
		UserUID:    userUID,
		Exchange:   req.Exchange,
		Label:      label,
		APIKey:     creds.APIKey,
		APISecret:  apiSecret,
		Passphrase: passphrase,
	}); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	err = h.connSvc.Create(r.Context(), &service.CreateConnectionRequest{
		UserUID:           userUID,
		Exchange:          req.Exchange,
		Label:             label,
		APIKey:            creds.APIKey,
		APISecret:         apiSecret,
		Passphrase:        passphrase,
		ExcludeFromReport: req.ExcludeFromReport,
	})
	if err != nil {
		if errors.Is(err, service.ErrConnectionAlreadyExists) {
			writeJSON(w, http.StatusOK, map[string]any{
				"success":  true,
				"user_uid": userUID,
				"exchange": req.Exchange,
				"error":    service.ExistingConnectionNoopMessage,
			})
			return
		}
		h.logger.Error("create connection from E2E failed", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false,
			"error":   "failed to create connection",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":  true,
		"user_uid": userUID,
		"exchange": req.Exchange,
		"message":  "Credentials encrypted and stored in enclave",
	})
}

func hexDecode(s string) ([]byte, error) {
	return hex.DecodeString(s)
}

func connectionKey(exchange, label string) string {
	ex := strings.ToLower(strings.TrimSpace(exchange))
	lb := strings.ToLower(strings.TrimSpace(label))
	if lb == "" {
		return ex
	}
	return ex + "/" + lb
}

func isConnectionExcluded(excluded map[string]struct{}, exchange, label string) bool {
	if len(excluded) == 0 {
		return false
	}
	if _, ok := excluded[connectionKey(exchange, label)]; ok {
		return true
	}
	_, ok := excluded[strings.ToLower(strings.TrimSpace(exchange))]
	return ok
}

// HealthCheck - GET /health
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "enclave-rest",
		"tls":     h.tlsKeygen != nil,
	})
}

// CreateUserConnection - POST /api/v1/connection
func (h *Handler) CreateUserConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UserUID             string `json:"user_uid"`
		Exchange            string `json:"exchange"`
		Label               string `json:"label"`
		APIKey              string `json:"api_key"`
		APISecret           string `json:"api_secret"`
		Passphrase          string `json:"passphrase"`
		SyncIntervalMinutes int    `json:"sync_interval_minutes"`
		ExcludeFromReport   bool   `json:"exclude_from_report"`
	}

	if err := readJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   msgInvalidRequestBody,
		})
		return
	}

	// AUTH-001: prefer the JWT-verified uid over the body-supplied one.
	userUID := resolveUserUID(r.Context(), req.UserUID)

	if userUID == "" || req.Exchange == "" || req.APIKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "user_uid, exchange, and api_key are required",
		})
		return
	}

	if err := validation.ValidateCreateConnection(&validation.CreateConnectionRequest{
		UserUID:             userUID,
		Exchange:            req.Exchange,
		Label:               req.Label,
		APIKey:              req.APIKey,
		APISecret:           req.APISecret,
		Passphrase:          req.Passphrase,
		SyncIntervalMinutes: req.SyncIntervalMinutes,
	}); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	if h.connSvc == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false,
			"error":   "database not configured",
		})
		return
	}

	// Upsert user first
	if h.userRepo != nil {
		if _, err := h.userRepo.GetOrCreate(r.Context(), userUID); err != nil {
			h.logger.Error("user upsert failed", zap.Error(err))
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"success": false,
				"error":   "failed to create user",
			})
			return
		}
	}

	err := h.connSvc.Create(r.Context(), &service.CreateConnectionRequest{
		UserUID:             userUID,
		Exchange:            req.Exchange,
		Label:               req.Label,
		APIKey:              req.APIKey,
		APISecret:           req.APISecret,
		Passphrase:          req.Passphrase,
		SyncIntervalMinutes: req.SyncIntervalMinutes,
		ExcludeFromReport:   req.ExcludeFromReport,
	})

	if err != nil {
		if errors.Is(err, service.ErrConnectionAlreadyExists) {
			writeJSON(w, http.StatusOK, map[string]any{
				"success":  true,
				"user_uid": userUID,
				"error":    service.ExistingConnectionNoopMessage,
			})
			return
		}
		h.logger.Error("create connection failed",
			zap.String("user_uid", userUID),
			zap.String("exchange", req.Exchange),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false,
			"error":   "failed to create connection",
		})
		return
	}

	h.logger.Info("connection created",
		zap.String("user_uid", userUID),
		zap.String("exchange", req.Exchange),
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"success":  true,
		"user_uid": userUID,
	})
}

// ProcessSyncJob - POST /api/v1/sync
func (h *Handler) ProcessSyncJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UserUID  string `json:"user_uid"`
		Exchange string `json:"exchange"`
	}

	if err := readJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   msgInvalidRequestBody,
		})
		return
	}

	// AUTH-001: prefer the JWT-verified uid over the body-supplied one.
	userUID := resolveUserUID(r.Context(), req.UserUID)

	if userUID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   msgUserUIDRequired,
		})
		return
	}

	rawExchange := req.Exchange
	if err := validation.ValidateSyncRequest(&validation.SyncJobRequest{
		UserUID:  userUID,
		Exchange: rawExchange,
	}); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	req.Exchange = strings.ToLower(rawExchange)

	if h.syncSvc == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false,
			"error":   "sync service not available",
		})
		return
	}

	var results []*service.SyncResult
	var err error

	if req.Exchange != "" {
		result := h.syncSvc.SyncExchange(r.Context(), userUID, req.Exchange)
		results = []*service.SyncResult{result}
	} else {
		results, err = h.syncSvc.SyncUser(r.Context(), userUID)
		if err != nil {
			h.logger.Error("sync user failed", zap.String("user_uid", userUID), zap.Error(err))
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"success": false,
				"error":   h.sanitizeErr(err),
			})
			return
		}
	}

	anySuccess := false
	for _, r := range results {
		if r.Success {
			anySuccess = true
			break
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":  anySuccess,
		"user_uid": userUID,
		"results":  results,
	})
}

// GetMetrics - GET /api/v1/metrics?user_uid=xxx&exchange=xxx&start=xxx&end=xxx
func (h *Handler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	// AUTH-001: prefer the JWT-verified uid over the query-supplied one.
	userUID := resolveUserUID(r.Context(), r.URL.Query().Get("user_uid"))
	if userUID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   msgUserUIDRequired,
		})
		return
	}

	if h.metricsSvc == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false,
			"error":   "metrics service not available",
		})
		return
	}

	rawExchange := r.URL.Query().Get("exchange")
	if rawExchange != "" {
		if err := validation.ValidateExchange(rawExchange); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false,
				"error":   err.Error(),
			})
			return
		}
	}
	exchange := strings.ToLower(rawExchange)

	// Parse date range (milliseconds)
	start := time.Now().AddDate(-1, 0, 0) // Default: 1 year ago
	end := time.Now()

	if s := r.URL.Query().Get("start"); s != "" {
		if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
			start = time.UnixMilli(ms)
		}
	}
	if e := r.URL.Query().Get("end"); e != "" {
		if ms, err := strconv.ParseInt(e, 10, 64); err == nil {
			end = time.UnixMilli(ms)
		}
	}

	excludedConnectionKeys := map[string]struct{}{}
	if h.connSvc != nil {
		excluded, err := h.connSvc.GetExcludedConnectionKeys(r.Context(), userUID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"success": false,
				"error":   msgFailedLoadExclusions,
			})
			return
		}
		excludedConnectionKeys = excluded
	}

	metrics, err := h.metricsSvc.CalculateWithFilters(r.Context(), userUID, start, end, exchange, excludedConnectionKeys)
	if err != nil {
		h.logger.Error("metrics computation failed", zap.String("user_uid", userUID), zap.Error(err))
		writeJSON(w, http.StatusOK, map[string]any{
			"success": false,
			"error":   h.sanitizeErr(err),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":               true,
		"sharpe_ratio":          metrics.SharpeRatio,
		"sortino_ratio":         metrics.SortinoRatio,
		"calmar_ratio":          metrics.CalmarRatio,
		"volatility":            metrics.Volatility,
		"downside_deviation":    metrics.DownsideDeviation,
		"max_drawdown":          metrics.MaxDrawdown,
		"max_drawdown_duration": metrics.MaxDrawdownDuration,
		"current_drawdown":      metrics.CurrentDrawdown,
		"win_rate":              metrics.WinRate,
		"profit_factor":         metrics.ProfitFactor,
		"avg_win":               metrics.AvgWin,
		"avg_loss":              metrics.AvgLoss,
		"total_return":          metrics.TotalReturn,
		"annualized_return":     metrics.AnnualizedReturn,
		"period_start":          metrics.PeriodStart.Unix(),
		"period_end":            metrics.PeriodEnd.Unix(),
		"data_points":           metrics.DataPoints,
	})
}

// GetSnapshots - GET /api/v1/snapshots?user_uid=xxx&exchange=xxx&start=xxx&end=xxx
func (h *Handler) GetSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	// AUTH-001: prefer the JWT-verified uid over the query-supplied one.
	userUID := resolveUserUID(r.Context(), r.URL.Query().Get("user_uid"))
	if userUID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   msgUserUIDRequired,
		})
		return
	}

	if h.snapshotRepo == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false,
			"error":   "database not configured",
		})
		return
	}

	// Parse date range (milliseconds)
	start := time.Now().AddDate(-1, 0, 0)
	end := time.Now()

	if s := r.URL.Query().Get("start"); s != "" {
		if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
			start = time.UnixMilli(ms)
		}
	}
	if e := r.URL.Query().Get("end"); e != "" {
		if ms, err := strconv.ParseInt(e, 10, 64); err == nil {
			end = time.UnixMilli(ms)
		}
	}

	snapshots, err := h.snapshotRepo.GetByUserAndDateRange(r.Context(), userUID, start, end)
	if err != nil {
		h.logger.Error("snapshot fetch failed", zap.String("user_uid", userUID), zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false,
			"error":   h.sanitizeErr(err),
		})
		return
	}

	excludedConnectionKeys := map[string]struct{}{}
	if h.connSvc != nil {
		excludedConnectionKeys, err = h.connSvc.GetExcludedConnectionKeys(r.Context(), userUID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"success": false,
				"error":   msgFailedLoadExclusions,
			})
			return
		}
	}

	// Filter by exchange if specified
	rawExchange := r.URL.Query().Get("exchange")
	if rawExchange != "" {
		if err := validation.ValidateExchange(rawExchange); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"success": false,
				"error":   err.Error(),
			})
			return
		}
	}
	exchange := strings.ToLower(rawExchange)
	var result []map[string]any
	for _, snap := range snapshots {
		if isConnectionExcluded(excludedConnectionKeys, snap.Exchange, snap.Label) {
			continue
		}
		if exchange != "" && strings.ToLower(snap.Exchange) != exchange {
			continue
		}
		result = append(result, map[string]any{
			"user_uid":         snap.UserUID,
			"exchange":         snap.Exchange,
			"label":            snap.Label,
			"timestamp":        snap.Timestamp.UnixMilli(),
			"total_equity":     snap.TotalEquity,
			"realized_balance": snap.RealizedBalance,
			"unrealized_pnl":   snap.UnrealizedPnL,
			"deposits":         snap.Deposits,
			"withdrawals":      snap.Withdrawals,
			"breakdown":        snap.Breakdown,
		})
	}

	if result == nil {
		result = []map[string]any{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":   true,
		"user_uid":  userUID,
		"snapshots": result,
	})
}

// GenerateReport - POST /api/v1/report
func (h *Handler) GenerateReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UserUID            string `json:"user_uid"`
		StartDate          string `json:"start_date"`
		EndDate            string `json:"end_date"`
		ReportName         string `json:"report_name"`
		Benchmark          string `json:"benchmark"`
		BaseCurrency       string `json:"base_currency"`
		IncludeRiskMetrics bool   `json:"include_risk_metrics"`
		IncludeDrawdown    bool   `json:"include_drawdown"`
		Manager            string `json:"manager"`
		Firm               string `json:"firm"`
	}

	if err := readJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   msgInvalidRequestBody,
		})
		return
	}

	// AUTH-001: prefer the JWT-verified uid over the body-supplied one.
	userUID := resolveUserUID(r.Context(), req.UserUID)

	if userUID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   msgUserUIDRequired,
		})
		return
	}

	if h.reportSvc == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"success": false,
			"error":   "report service not available",
		})
		return
	}

	startDate, err := time.Parse("2006-01-02", req.StartDate)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "invalid start_date format (use YYYY-MM-DD)",
		})
		return
	}

	endDate, err := time.Parse("2006-01-02", req.EndDate)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "invalid end_date format (use YYYY-MM-DD)",
		})
		return
	}

	excludedConnectionKeys := map[string]struct{}{}
	if h.connSvc != nil {
		excludedConnectionKeys, err = h.connSvc.GetExcludedConnectionKeys(r.Context(), userUID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"success": false,
				"error":   msgFailedLoadExclusions,
			})
			return
		}
	}

	report, err := h.reportSvc.GenerateReport(r.Context(), &service.GenerateReportRequest{
		UserUID:            userUID,
		StartDate:          startDate,
		EndDate:            endDate,
		ReportName:         req.ReportName,
		Benchmark:          req.Benchmark,
		BaseCurrency:       req.BaseCurrency,
		IncludeRiskMetrics: req.IncludeRiskMetrics,
		IncludeDrawdown:    req.IncludeDrawdown,
		ExcludedExchanges:  excludedConnectionKeys,
		Manager:            req.Manager,
		Firm:               req.Firm,
	})

	if err != nil {
		h.logger.Error("generate report failed", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"success": false,
			"error":   h.sanitizeErr(err),
		})
		return
	}

	writeJSON(w, http.StatusOK, report)
}

// VerifySignature - POST /api/v1/verify
func (h *Handler) VerifySignature(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ReportHash string `json:"report_hash"`
		Signature  string `json:"signature"`
		PublicKey  string `json:"public_key"`
	}

	if err := readJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"valid": false,
			"error": msgInvalidRequestBody,
		})
		return
	}

	if req.ReportHash == "" || req.Signature == "" || req.PublicKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"valid": false,
			"error": "report_hash, signature, and public_key are required",
		})
		return
	}

	if h.reportSvc == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"valid": false,
			"error": "report service not available",
		})
		return
	}

	valid, err := h.reportSvc.VerifySignature(req.ReportHash, req.Signature, req.PublicKey)
	if err != nil {
		h.logger.Warn("verify signature failed", zap.Error(err))
		writeJSON(w, http.StatusOK, map[string]any{
			"valid": false,
			"error": h.sanitizeErr(err),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"valid": valid,
	})
}

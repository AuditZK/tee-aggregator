package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/trackrecord/enclave/internal/auth"
	"github.com/trackrecord/enclave/internal/config"
	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	"github.com/trackrecord/enclave/internal/signing"
	"go.uber.org/zap"
)

// SyncSchedulerRunner can trigger a sync immediately.
type SyncSchedulerRunner interface {
	RunNow()
}

type Server struct {
	cfg       *config.Config
	logger    *zap.Logger
	handler   *Handler
	http      *http.Server
	pool      *pgxpool.Pool
	scheduler SyncSchedulerRunner

	// jwtSecret gates the REST surface when non-nil (SEC-002). Same secret as
	// the gRPC interceptor — typically ENCLAVE_JWT_SECRET. When nil the
	// middleware logs a dev-mode warning and passes requests through.
	jwtSecret []byte

	// jwtExpectedIssuer pins the `iss` claim on inbound JWTs when non-empty
	// (AUTH-002 follow-up). Mirrors the gRPC interceptor.
	jwtExpectedIssuer string
}

// SetScheduler attaches the scheduler for admin sync trigger.
func (s *Server) SetScheduler(sched SyncSchedulerRunner) {
	s.scheduler = sched
}

// SetJWTSecret wires the HS256 secret used to verify Authorization: Bearer
// tokens on sensitive REST endpoints. Call before Start().
func (s *Server) SetJWTSecret(secret []byte) {
	s.jwtSecret = secret
}

// SetJWTExpectedIssuer pins the `iss` claim required on inbound JWTs.
// Empty disables the check (legacy behaviour). Call before Start().
func (s *Server) SetJWTExpectedIssuer(iss string) {
	s.jwtExpectedIssuer = iss
}

func New(cfg *config.Config, logger *zap.Logger, pool *pgxpool.Pool, signer *signing.ReportSigner) *Server {
	var connSvc *service.ConnectionService
	var syncSvc *service.SyncService
	var metricsSvc *service.MetricsService
	var reportSvc *service.ReportService
	var snapshotRepo *repository.SnapshotRepo
	var userRepo *repository.UserRepo

	if pool != nil {
		enc, err := encryption.New(cfg.EncryptionKey)
		if err != nil {
			logger.Error("encryption init failed", zap.Error(err))
		} else {
			connRepo := repository.NewConnectionRepo(pool)
			snapshotRepo = repository.NewSnapshotRepo(pool)
			userRepo = repository.NewUserRepo(pool)

			connSvc = service.NewConnectionService(connRepo, enc)
			syncSvc = service.NewSyncService(connSvc, snapshotRepo, nil, logger)
			metricsSvc = service.NewMetricsService(snapshotRepo)

			if signer != nil {
				reportSvc = service.NewReportService(metricsSvc, snapshotRepo, signer)
				reportSvc.SetConnectionService(connSvc)
			}
		}
	}

	return &Server{
		cfg:     cfg,
		logger:  logger,
		handler: NewHandler(logger, connSvc, syncSvc, metricsSvc, reportSvc, snapshotRepo, userRepo),
		pool:    pool,
	}
}

// SetHandler replaces the handler (used when creating with NewHandlerWithOptions).
func (s *Server) SetHandler(h *Handler) {
	s.handler = h
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// Credential rate limiter: 5 requests per 15 minutes per IP (SEC-004).
	// X-Forwarded-For is honoured only for peers in RATE_LIMIT_TRUSTED_PROXIES.
	credRateLimiter := NewIPRateLimiter(5, 15*time.Minute, s.cfg.RateLimitTrustedProxies...)

	// Public routes (no auth required):
	//   /health            — liveness probe
	//   /api/v1/tls/fingerprint — public by design (used by clients before attestation)
	//   /api/v1/attestation     — public by design (attestation quote)
	//   /api/v1/verify          — public by design (stateless signature check)
	mux.HandleFunc("/health", s.handler.HealthCheck)
	mux.HandleFunc("/api/v1/tls/fingerprint", s.handler.GetTLSFingerprint)
	mux.HandleFunc("/api/v1/attestation", s.handler.GetAttestation)

	// Gated routes: carry user data or mutate state. Must go through jwtRequired
	// when ENCLAVE_JWT_SECRET is set (SEC-002). In dev mode, jwtRequired logs
	// a warning and passes the request through.
	mux.HandleFunc("/api/v1/credentials/connect", credRateLimiter.Middleware(s.jwtRequired(s.handler.ConnectCredentials)))

	// Admin endpoints: enforced localhost-only (SEC-001). The `localhostOnly`
	// wrapper inspects r.RemoteAddr (not X-Forwarded-For, which is spoofable
	// and only set by the front proxy anyway). Non-loopback peers get 403.
	mux.HandleFunc("/api/v1/admin/sync-now", s.localhostOnly(s.handleAdminSyncNow))
	mux.HandleFunc("/api/v1/admin/cashflows", s.localhostOnly(s.handleAdminDumpCashflows))

	// Legacy REST routes are disabled by default for strict TS parity.
	if s.cfg.EnableLegacyREST {
		if s.cfg.IsDevelopment() {
			// Plaintext credential endpoint — legacy only; use /api/v1/credentials/connect.
			mux.HandleFunc("/api/v1/connection", s.jwtRequired(s.handler.CreateUserConnection))
		} else {
			mux.HandleFunc("/api/v1/connection", func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusGone, map[string]any{
					"success": false,
					"error":   "plaintext credential submission is disabled in production; use /api/v1/credentials/connect with E2E encryption",
				})
			})
		}
		mux.HandleFunc("/api/v1/sync", s.jwtRequired(s.handler.ProcessSyncJob))
		mux.HandleFunc("/api/v1/metrics", s.jwtRequired(s.handler.GetMetrics))
		mux.HandleFunc("/api/v1/snapshots", s.jwtRequired(s.handler.GetSnapshots))
		mux.HandleFunc("/api/v1/report", s.jwtRequired(s.handler.GenerateReport))
		mux.HandleFunc("/api/v1/verify", s.handler.VerifySignature)
	}

	// Apply CORS then logging middleware
	var handler http.Handler = mux
	if s.cfg.CORSOrigin != "" {
		handler = CORSMiddleware(s.cfg.CORSOrigin, handler)
	}
	handler = s.loggingMiddleware(handler)

	s.http = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.cfg.RESTPort),
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.http.Shutdown(shutdownCtx)
		if s.pool != nil {
			s.pool.Close()
		}
	}()

	if s.handler == nil || s.handler.tlsKeygen == nil {
		return fmt.Errorf("REST TLS credentials are required (tls key generator not configured)")
	}

	cert, err := tls.X509KeyPair(s.handler.tlsKeygen.CertPEM(), s.handler.tlsKeygen.KeyPEM())
	if err != nil {
		return fmt.Errorf("failed to parse REST TLS keypair: %w", err)
	}

	s.http.TLSConfig = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}

	// Silence TLS handshake errors from scanners/bots (logged to debug instead of stderr)
	s.http.ErrorLog = log.New(io.Discard, "", 0)

	s.logger.Info("server starting",
		zap.String("addr", s.http.Addr),
		zap.Bool("https", true),
		zap.Bool("legacy_rest", s.cfg.EnableLegacyREST),
	)

	return s.http.ListenAndServeTLS("", "")
}

// handleAdminSyncNow triggers the daily sync immediately (admin only).
func (s *Server) handleAdminSyncNow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "POST only"})
		return
	}

	if s.scheduler == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "scheduler not configured"})
		return
	}

	s.logger.Info("admin sync-now triggered")
	go s.scheduler.RunNow()
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "message": "sync triggered, check logs"})
}

// handleAdminDumpCashflows dumps all BALANCE deals for a user/exchange/label since a date.
// Usage: GET /api/v1/admin/cashflows?user_uid=X&exchange=mt5&label=Y&from=2026-04-01
//
// Intended for admin backfills when the normal sync window missed cashflows
// (e.g. the Headway side=buy/sell bug). Requires a running enclave with the
// DEK unwrapped so broker credentials can be decrypted.
func (s *Server) handleAdminDumpCashflows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "GET only"})
		return
	}

	q := r.URL.Query()
	userUID := q.Get("user_uid")
	exchange := q.Get("exchange")
	label := q.Get("label")
	fromStr := q.Get("from")

	if userUID == "" || exchange == "" || fromStr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "user_uid, exchange, and from (YYYY-MM-DD) are required",
		})
		return
	}

	since, err := time.Parse("2006-01-02", fromStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid from date, use YYYY-MM-DD"})
		return
	}

	if s.handler == nil || s.handler.syncSvc == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "sync service not available"})
		return
	}

	cashflows, err := s.handler.syncSvc.DumpCashflows(r.Context(), userUID, exchange, label, since)
	if err != nil {
		s.logger.Error("dump cashflows failed", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": s.handler.sanitizeErr(err)})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":   true,
		"count":     len(cashflows),
		"cashflows": cashflows,
	})
}

// jwtRequired verifies an HS256 bearer token on REST handlers (SEC-002).
//
// When s.jwtSecret is nil (dev mode) the middleware logs a single-shot warning
// and passes the request through so local development works without extra
// setup — same policy as the gRPC authInterceptor.
//
// When s.jwtSecret is set, the middleware:
//   - rejects missing / malformed Authorization headers with 401
//   - verifies via auth.VerifyHS256 (checks exp + aud == "go-enclave")
//   - injects the verified `sub` into the request context via auth.WithUserUID
//     so downstream handlers can prefer the JWT-asserted UID over body fields.
func (s *Server) jwtRequired(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(s.jwtSecret) == 0 {
			s.logger.Warn("REST JWT auth skipped (ENCLAVE_JWT_SECRET not set)",
				zap.String("path", r.URL.Path),
			)
			next(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": "missing authorization header",
			})
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		if tokenStr == authHeader {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": "authorization header must use Bearer scheme",
			})
			return
		}

		claims, err := auth.VerifyHS256WithOptions(tokenStr, s.jwtSecret, auth.VerifyOptions{
			ExpectedIssuer: s.jwtExpectedIssuer,
		})
		if err != nil {
			s.logger.Warn("REST JWT verification failed",
				zap.String("path", r.URL.Path),
				zap.Error(err),
			)
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": "invalid or expired token",
			})
			return
		}

		r = r.WithContext(auth.WithUserUID(r.Context(), claims.Sub))
		next(w, r)
	}
}

// localhostOnly rejects non-loopback peers. Deliberately inspects
// r.RemoteAddr (the actual TCP peer) and ignores X-Forwarded-For / X-Real-IP,
// which are trivially spoofable over HTTPS. The admin tools run inside the
// same container (`docker exec`), so loopback is sufficient.
func (s *Server) localhostOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			s.logger.Warn("admin endpoint blocked: non-loopback peer",
				zap.String("path", r.URL.Path),
				zap.String("remote_addr", r.RemoteAddr),
			)
			writeJSON(w, http.StatusForbidden, map[string]any{
				"error": "admin endpoints are restricted to loopback callers",
			})
			return
		}
		next(w, r)
	}
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)

		// Health checks are debug-level to avoid log pollution (every 30s)
		if r.URL.Path == "/health" {
			s.logger.Debug("request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Duration("duration", time.Since(start)),
			)
			return
		}

		s.logger.Info("request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Duration("duration", time.Since(start)),
		)
	})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// defaultMaxRequestBodyBytes caps REST request bodies (SEC-007). 64 KiB is
// far larger than any legitimate credential / sync / report payload and
// small enough to absorb a burst of hostile requests without growing the
// enclave heap. Exceeding this returns 413 via http.MaxBytesReader.
const defaultMaxRequestBodyBytes = int64(64 << 10)

// readJSON decodes the request body into v with a 64 KiB size cap and
// DisallowUnknownFields enabled (SEC-007). Callers that need a different
// cap should use readJSONWithLimit.
func readJSON(w http.ResponseWriter, r *http.Request, v any) error {
	return readJSONWithLimit(w, r, v, defaultMaxRequestBodyBytes)
}

func readJSONWithLimit(w http.ResponseWriter, r *http.Request, v any, maxBytes int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

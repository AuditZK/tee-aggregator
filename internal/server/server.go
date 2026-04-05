package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
}

// SetScheduler attaches the scheduler for admin sync trigger.
func (s *Server) SetScheduler(sched SyncSchedulerRunner) {
	s.scheduler = sched
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

	// Credential rate limiter: 5 requests per 15 minutes per IP
	credRateLimiter := NewIPRateLimiter(5, 15*time.Minute)

	// TS parity routes (always enabled)
	mux.HandleFunc("/health", s.handler.HealthCheck)
	mux.HandleFunc("/api/v1/tls/fingerprint", s.handler.GetTLSFingerprint)
	mux.HandleFunc("/api/v1/attestation", s.handler.GetAttestation)
	mux.HandleFunc("/api/v1/credentials/connect", credRateLimiter.Middleware(s.handler.ConnectCredentials))

	// Legacy REST routes are disabled by default for strict TS parity.
	if s.cfg.EnableLegacyREST {
		if s.cfg.IsDevelopment() {
			// Plaintext credential endpoint — legacy only; use /api/v1/credentials/connect.
			mux.HandleFunc("/api/v1/connection", s.handler.CreateUserConnection)
		} else {
			mux.HandleFunc("/api/v1/connection", func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusGone, map[string]any{
					"success": false,
					"error":   "plaintext credential submission is disabled in production; use /api/v1/credentials/connect with E2E encryption",
				})
			})
		}
		mux.HandleFunc("/api/v1/sync", s.handler.ProcessSyncJob)
		mux.HandleFunc("/api/v1/admin/sync-now", s.handleAdminSyncNow)
		mux.HandleFunc("/api/v1/metrics", s.handler.GetMetrics)
		mux.HandleFunc("/api/v1/snapshots", s.handler.GetSnapshots)
		mux.HandleFunc("/api/v1/report", s.handler.GenerateReport)
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

func readJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

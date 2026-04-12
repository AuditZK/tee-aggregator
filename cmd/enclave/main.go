package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/trackrecord/enclave/internal/attestation"
	"github.com/trackrecord/enclave/internal/config"
	"github.com/trackrecord/enclave/internal/db"
	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/cache"
	enclaveGrpc "github.com/trackrecord/enclave/internal/grpc"
	"github.com/trackrecord/enclave/internal/logredact"
	"github.com/trackrecord/enclave/internal/logstream"
	"github.com/trackrecord/enclave/internal/metrics"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/scheduler"
	"github.com/trackrecord/enclave/internal/security"
	"github.com/trackrecord/enclave/internal/server"
	"github.com/trackrecord/enclave/internal/service"
	"github.com/trackrecord/enclave/internal/signing"
	tlspkg "github.com/trackrecord/enclave/internal/tls"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// 1. Load config
	cfg := config.Load()

	// SECURITY: In production, enforce secrets from GCP metadata (no .env files)
	if cfg.Env == "production" {
		enforceNoEnvFile()
	}

	// 2. Init base logger with configurable log level
	var baseLogger *zap.Logger
	if cfg.Env == "production" {
		prodCfg := zap.NewProductionConfig()
		if level, err := zapcore.ParseLevel(cfg.LogLevel); err == nil {
			prodCfg.Level = zap.NewAtomicLevelAt(level)
		}
		baseLogger, _ = prodCfg.Build()
	} else {
		devCfg := zap.NewDevelopmentConfig()
		if level, err := zapcore.ParseLevel(cfg.LogLevel); err == nil {
			devCfg.Level = zap.NewAtomicLevelAt(level)
		}
		baseLogger, _ = devCfg.Build()
	}

	// 3. Wrap logger with redaction core (ALWAYS active, TS parity)
	// SECURITY: All sensitive fields (credentials, user IDs, balances) are redacted
	// before any log output. Auditors can verify no sensitive data leaks via logs.
	redactedLogger := zap.New(
		logredact.NewRedactCore(baseLogger.Core()),
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	// 4. Start log stream server (SSE) and wrap logger with broadcast core
	var logStreamServer *logstream.Server
	logger := redactedLogger

	if cfg.LogStreamPort > 0 {
		logStreamServer = logstream.NewServer(cfg.LogStreamPort, cfg.LogStreamAPIKey, baseLogger)
		broadcastCore := logstream.NewBroadcastCore(baseLogger.Core(), logStreamServer)
		logger = zap.New(broadcastCore, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	}
	defer logger.Sync()

	logger.Info("starting enclave worker",
		zap.String("version", "1.0.0-go"),
		zap.String("env", cfg.Env),
	)

	// 4. Memory protection (Linux only, no-op on Windows)
	memProtect := security.NewMemoryProtection(logger)
	memProtect.Apply()

	// 5. REST TLS certificate loading (mandatory, TS parity)
	tlsKeygen, err := tlspkg.NewKeyGeneratorFromFiles(cfg.TLSCertPath, cfg.TLSKeyPath)
	if err != nil {
		logger.Fatal("REST TLS certificates not found or invalid (server refuses to start)",
			zap.String("cert_path", cfg.TLSCertPath),
			zap.String("key_path", cfg.TLSKeyPath),
			zap.Error(err),
		)
	}
	logger.Info("REST TLS certificate loaded",
		zap.String("cert_path", cfg.TLSCertPath),
		zap.String("key_path", cfg.TLSKeyPath),
		zap.String("fingerprint", tlsKeygen.Fingerprint()[:16]+"..."),
	)

	// 6. ECIES service (E2E encryption)
	eciesSvc, err := encryption.NewECIES()
	if err != nil {
		logger.Error("ECIES init failed, continuing without E2E", zap.Error(err))
	} else {
		logger.Info("ECIES service initialized")
	}

	// 7. Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 8. Connect database
	pool := connectDatabase(ctx, cfg, logger)

	// 9. Init encryption (AES for credentials at rest).
	//
	// Credentials on disk were encrypted with a DEK that lives
	// wrapped inside data_encryption_keys. To decrypt them we must:
	//   1. Derive the master key (SEV-SNP measurement or env fallback).
	//   2. Read the active wrapped DEK from the DB.
	//   3. Unwrap it with the master key.
	//   4. Use the unwrapped DEK as the AES-256-GCM key.
	//
	// The previous implementation used cfg.EncryptionKey directly as
	// the AES key, bypassing steps 2 and 3. That path silently
	// produced a "decryption failed: authentication error" for every
	// credential on a DB seeded by the TS enclave.
	//
	// We fall back to the raw ENCRYPTION_KEY path only when there is
	// no database pool (dev harness) or when the DB is empty and
	// auto-seeding is explicitly requested — see the pool != nil
	// branch below.
	var enc *encryption.Service
	if pool != nil {
		keyDerivation, derivErr := encryption.NewKeyDerivationService(logger)
		if derivErr != nil {
			logger.Fatal("key derivation init failed", zap.Error(derivErr))
		}
		logger.Info("key derivation initialized",
			zap.Bool("hardware_sev_snp", keyDerivation.IsHardwareKey()),
			zap.String("master_key_id", keyDerivation.GetMasterKeyID()),
		)

		keyMgmt, kmErr := encryption.NewKeyManagementService(pool, encryption.KeyManagementOptions{
			Derivation:    keyDerivation,
			Logger:        logger,
			AllowAutoInit: cfg.IsDevelopment(),
		})
		if kmErr != nil {
			// Hard fail: running against a DB whose DEK we cannot
			// unwrap means every sync will fail. Bail out early with
			// a descriptive error instead of silently pretending
			// ENCRYPTION_KEY works.
			logger.Fatal("key management init failed (cannot unwrap active DEK)",
				zap.Error(kmErr),
				zap.String("hint", "check ENCRYPTION_KEY, SEV-SNP attestation, and data_encryption_keys.master_key_id"),
			)
		}

		enc, err = keyMgmt.GetEncryptionService()
		if err != nil {
			logger.Fatal("build encryption service from DEK failed", zap.Error(err))
		}
		logger.Info("encryption service bound to unwrapped DEK from data_encryption_keys")
	} else {
		// No DB: use the env-var key directly. This path is for the
		// dev harness that runs the enclave without a backing
		// Postgres, so there is no wrapped DEK to load.
		logger.Warn("no database pool — initializing encryption with ENCRYPTION_KEY directly (dev harness only)")
		enc, err = encryption.New(cfg.EncryptionKey)
		if err != nil {
			logger.Fatal("encryption init failed", zap.Error(err))
		}
	}

	// 10. Init repositories
	var connRepo *repository.ConnectionRepo
	var snapshotRepo *repository.SnapshotRepo
	var userRepo *repository.UserRepo
	var signedReportRepo *repository.SignedReportRepo
	var rateLimitRepo *repository.RateLimitRepo
	var syncStatusRepo *repository.SyncStatusRepo

	if pool != nil {
		connRepo = repository.NewConnectionRepo(pool)
		snapshotRepo = repository.NewSnapshotRepo(pool)
		userRepo = repository.NewUserRepo(pool)
		signedReportRepo = repository.NewSignedReportRepo(pool)
		rateLimitRepo = repository.NewRateLimitRepo(pool)
		syncStatusRepo = repository.NewSyncStatusRepo(pool)
	}

	// 11. Init services
	var connSvc *service.ConnectionService
	var syncSvc *service.SyncService
	var metricsSvc *service.MetricsService
	var reportSvc *service.ReportService
	var rateLimiterSvc *service.RateLimiterService
	benchmarkSvc := service.NewBenchmarkService()

	// 11b. Init connector cache (TS parity: UniversalConnectorCache)
	connectorCache := cache.NewConnectorCache()
	defer connectorCache.Stop()

	if pool != nil {
		connSvc = service.NewConnectionService(connRepo, enc)
		syncSvc = service.NewSyncService(connSvc, snapshotRepo, connectorCache, logger)
		if syncStatusRepo != nil {
			syncSvc.SetSyncStatusRepo(syncStatusRepo)
		}
		metricsSvc = service.NewMetricsService(snapshotRepo)

		if rateLimitRepo != nil {
			rateLimiterSvc = service.NewRateLimiterService(rateLimitRepo, logger)
		}
	}

	// 12. Init report signer (ephemeral key per startup)
	signer, err := signing.NewReportSignerGenerate()
	if err != nil {
		logger.Fatal("failed to initialize report signer", zap.Error(err))
	}
	signingPubKey := signer.PublicKey()
	logger.Info("report signer initialized",
		zap.String("algorithm", signing.SignatureAlgorithm),
		zap.String("public_key", signingPubKey[:16]+"..."),
	)

	if metricsSvc != nil && snapshotRepo != nil {
		reportSvc = service.NewReportServiceFull(
			metricsSvc, snapshotRepo, signedReportRepo,
			signer, benchmarkSvc, logger,
		)
		if connSvc != nil {
			reportSvc.SetConnectionService(connSvc)
		}
	}

	// 13. Attestation service
	var attestSvc *attestation.Service
	{
		opts := attestation.Options{
			DevMode: cfg.IsDevelopment(),
			Logger:  logger,
		}
		if tlsKeygen != nil {
			opts.TLSFingerprint = tlsKeygen.Fingerprint()
		}
		if eciesSvc != nil {
			opts.E2EPublicKey = eciesSvc.PublicKeyPEM()
		}
		opts.SigningPubKey = signingPubKey
		attestSvc = attestation.NewService(opts)
		if logStreamServer != nil {
			logStreamServer.SetAttestationService(attestSvc)
		}
	}

	// 14. Start gRPC server
	grpcTLSConfig, err := buildGRPCTLSConfig(cfg)
	if err != nil {
		logger.Fatal("invalid gRPC TLS configuration", zap.Error(err))
	}
	if grpcTLSConfig == nil {
		logger.Warn("gRPC running without TLS (development mode with GRPC_INSECURE=true)")
	}

	grpcServer := enclaveGrpc.NewServer(logger, connSvc, syncSvc, metricsSvc, reportSvc, snapshotRepo, userRepo, attestSvc)
	go func() {
		if err := grpcServer.Start(cfg.GRPCPort, grpcTLSConfig); err != nil {
			logger.Fatal("gRPC server failed", zap.Error(err))
		}
	}()
	logger.Info("gRPC server started", zap.Int("port", cfg.GRPCPort))

	// 15. Start REST server (with TLS, attestation, ECIES, CORS, rate limiting)
	restServer := server.New(cfg, logger, pool, signer)
	restServer.SetHandler(server.NewHandlerWithOptions(server.HandlerOptions{
		Logger:       logger,
		ConnSvc:      connSvc,
		SyncSvc:      syncSvc,
		MetricsSvc:   metricsSvc,
		ReportSvc:    reportSvc,
		SnapshotRepo: snapshotRepo,
		UserRepo:     userRepo,
		TLSKeygen:    tlsKeygen,
		AttestSvc:    attestSvc,
		ECIESSvc:     eciesSvc,
	}))
	go func() {
		if err := restServer.Start(ctx); err != nil {
			logger.Error("REST server stopped", zap.Error(err))
		}
	}()
	logger.Info("REST server started", zap.Int("port", cfg.RESTPort))

	// 16. Start log stream server
	if logStreamServer != nil {
		if err := logStreamServer.Start(ctx); err != nil {
			logger.Error("log stream server failed", zap.Error(err))
		} else {
			logger.Info("log stream server started", zap.Int("port", cfg.LogStreamPort))
		}
	}

	// 17. Start Prometheus metrics server
	var metricsServer *metrics.Metrics
	if cfg.MetricsEnabled && cfg.MetricsPort > 0 {
		metricsServer = metrics.New(logger)
		if err := metricsServer.Start(cfg.MetricsPort); err != nil {
			logger.Error("metrics server failed", zap.Error(err))
		} else {
			logger.Info("metrics server started", zap.Int("port", cfg.MetricsPort))
		}
	}

	// 18. Start sync scheduler (honours ENABLE_DAILY_SYNC)
	var syncScheduler *scheduler.SyncScheduler
	if syncSvc != nil && userRepo != nil {
		syncScheduler = scheduler.NewSyncScheduler(syncSvc, userRepo, logger)
		restServer.SetScheduler(syncScheduler)
		if cfg.EnableDailySync {
			syncScheduler.Start()
		} else {
			logger.Info("daily sync scheduler disabled by ENABLE_DAILY_SYNC=false (manual sync via gRPC still works)")
		}
	}

	logger.Info("enclave worker ready",
		zap.Int("grpc_port", cfg.GRPCPort),
		zap.Int("rest_port", cfg.RESTPort),
		zap.Bool("database", pool != nil),
		zap.Bool("tls", tlsKeygen != nil),
		zap.Bool("e2e", eciesSvc != nil),
		zap.Bool("attestation", attestSvc != nil),
		zap.Bool("log_stream", logStreamServer != nil),
		zap.Bool("metrics", metricsServer != nil),
	)

	// Suppress unused variable warnings for services used only indirectly
	_ = rateLimiterSvc

	// 19. Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	logger.Info("received shutdown signal", zap.String("signal", sig.String()))

	// Graceful shutdown
	cancel()

	if syncScheduler != nil {
		syncScheduler.Stop()
	}
	grpcServer.Stop()
	if logStreamServer != nil {
		logStreamServer.Stop()
	}
	if metricsServer != nil {
		metricsServer.Stop()
	}
	if tlsKeygen != nil {
		tlsKeygen.Cleanup()
	}
	if pool != nil {
		pool.Close()
	}

	logger.Info("graceful shutdown completed")
}

func connectDatabase(ctx context.Context, cfg *config.Config, logger *zap.Logger) *pgxpool.Pool {
	if cfg.DatabaseURL == "" {
		logger.Warn("DATABASE_URL not set, running without database")
		return nil
	}

	pool, err := db.Connect(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("database connection failed, running without database", zap.Error(err))
		return nil
	}

	if cfg.AutoMigrate {
		if err := db.ApplyMigrations(ctx, pool, cfg.MigrationsDir, logger); err != nil {
			logger.Error("auto-migrate failed, running without database",
				zap.String("dir", cfg.MigrationsDir),
				zap.Error(err),
			)
			pool.Close()
			return nil
		}
	}

	return pool
}

func buildGRPCTLSConfig(cfg *config.Config) (*tls.Config, error) {
	if cfg.GRPCInsecure {
		if !cfg.IsDevelopment() {
			return nil, fmt.Errorf("GRPC_INSECURE=true is only allowed in development")
		}
		return nil, nil
	}

	rootCA, err := os.ReadFile(cfg.TLSCACertPath)
	if err != nil {
		return nil, fmt.Errorf("read gRPC CA cert %s: %w", cfg.TLSCACertPath, err)
	}
	certPEM, err := os.ReadFile(cfg.TLSServerCertPath)
	if err != nil {
		return nil, fmt.Errorf("read gRPC server cert %s: %w", cfg.TLSServerCertPath, err)
	}
	keyPEM, err := os.ReadFile(cfg.TLSServerKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read gRPC server key %s: %w", cfg.TLSServerKeyPath, err)
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse gRPC server keypair (cert=%s key=%s): %w", cfg.TLSServerCertPath, cfg.TLSServerKeyPath, err)
	}

	clientCAPool := x509.NewCertPool()
	if ok := clientCAPool.AppendCertsFromPEM(rootCA); !ok {
		return nil, fmt.Errorf("parse gRPC CA cert PEM (%s): no certificates found", cfg.TLSCACertPath)
	}

	requireClientCert := !cfg.IsDevelopment() || cfg.RequireClientCert
	clientAuth := tls.NoClientCert
	if requireClientCert {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	return &tls.Config{
		MinVersion:               tls.VersionTLS12,
		Certificates:             []tls.Certificate{cert},
		ClientCAs:                clientCAPool,
		ClientAuth:               clientAuth,
		PreferServerCipherSuites: true,
	}, nil
}

// enforceNoEnvFile ensures production enclave does NOT use .env files.
// Secrets must come from GCP metadata server — auditable and verifiable.
// This function:
//  1. Refuses to start if a .env file exists in the working directory
//  2. Verifies GCP metadata server is accessible
//  3. Logs the secret source for audit trail
func enforceNoEnvFile() {
	// Check for .env file — must NOT exist in production
	for _, envFile := range []string{".env", ".env.local", ".env.production"} {
		if _, err := os.Stat(envFile); err == nil {
			fmt.Fprintf(os.Stderr, "FATAL: %s file detected in production.\n", envFile)
			fmt.Fprintf(os.Stderr, "Production enclave must load secrets from GCP metadata, not .env files.\n")
			fmt.Fprintf(os.Stderr, "Remove the file and use: ./scripts/start-enclave.sh\n")
			os.Exit(1)
		}
	}

	// Verify GCP metadata server is accessible
	client := &http.Client{Timeout: 3 * time.Second}
	req, _ := http.NewRequest("GET",
		"http://metadata.google.internal/computeMetadata/v1/instance/id", nil)
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "WARNING: GCP metadata server not accessible.\n")
		fmt.Fprintf(os.Stderr, "Ensure secrets are injected via environment variables (not .env files).\n")
		// Don't exit — allow non-GCP production environments (e.g., Docker -e flags from start-enclave.sh)
	} else {
		resp.Body.Close()
	}
}

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/trackrecord/enclave/internal/attestation"
	"github.com/trackrecord/enclave/internal/config"
	"github.com/trackrecord/enclave/internal/db"
	"github.com/trackrecord/enclave/internal/encryption"
	enclaveGrpc "github.com/trackrecord/enclave/internal/grpc"
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

	// 3. Start log stream server (SSE) and wrap logger with broadcast core
	var logStreamServer *logstream.Server
	logger := baseLogger

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

	// 5. TLS key generation (ECDSA P-256 self-signed cert)
	tlsKeygen, err := tlspkg.NewKeyGenerator()
	if err != nil {
		logger.Error("TLS keygen failed, continuing without TLS binding", zap.Error(err))
	} else {
		logger.Info("TLS certificate generated",
			zap.String("fingerprint", tlsKeygen.Fingerprint()[:16]+"..."),
		)
	}

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

	// 9. Init encryption (AES for credentials at rest)
	enc, err := encryption.New(cfg.EncryptionKey)
	if err != nil {
		logger.Fatal("encryption init failed", zap.Error(err))
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

	if pool != nil {
		connSvc = service.NewConnectionService(connRepo, enc)
		syncSvc = service.NewSyncService(connSvc, snapshotRepo, logger)
		metricsSvc = service.NewMetricsService(snapshotRepo)

		if rateLimitRepo != nil {
			rateLimiterSvc = service.NewRateLimiterService(rateLimitRepo, logger)
		}
	}

	// 12. Init report signer (ephemeral key per startup)
	signer := signing.NewReportSignerGenerate()
	logger.Info("report signer initialized",
		zap.String("algorithm", signing.SignatureAlgorithm),
		zap.String("public_key", signer.PublicKeyHex()[:16]+"..."),
	)

	if metricsSvc != nil && snapshotRepo != nil {
		reportSvc = service.NewReportServiceFull(
			metricsSvc, snapshotRepo, signedReportRepo,
			signer, benchmarkSvc, logger,
		)
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
		opts.SigningPubKey = signer.PublicKeyHex()
		attestSvc = attestation.NewService(opts)
	}

	// 14. Start gRPC server
	grpcServer := enclaveGrpc.NewServer(logger, connSvc, syncSvc, metricsSvc, reportSvc, snapshotRepo, userRepo)
	go func() {
		if err := grpcServer.Start(cfg.GRPCPort); err != nil {
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

	// 18. Start sync scheduler
	var syncScheduler *scheduler.SyncScheduler
	if syncSvc != nil && userRepo != nil {
		syncScheduler = scheduler.NewSyncScheduler(syncSvc, userRepo, logger)
		syncScheduler.Start()
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
	_ = syncStatusRepo

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

	return pool
}

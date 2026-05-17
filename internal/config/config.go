package config

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCPort      int
	GRPCInsecure  bool // Allow insecure gRPC (no TLS) in dev mode
	RESTPort      int
	DatabaseURL   string
	EncryptionKey []byte // 32 bytes for AES-256
	Env           string
	LogLevel      string // "debug", "info", "warn", "error"

	// Log streaming & metrics
	LogStreamPort   int
	LogStreamAPIKey string
	MetricsPort     int
	MetricsEnabled  bool

	// HTTP proxy for geo-restricted exchanges
	ExchangeHTTPProxy string
	ProxyExchanges    string // Comma-separated list, default: "binance"

	// CORS
	CORSOrigin string // Comma-separated allowed origins

	// Benchmark service
	BenchmarkServiceURL string

	// Data retention
	DataRetentionDays int

	// Feature toggles
	EnableDailySync  bool
	EnableLegacyREST bool

	// Migrations
	AutoMigrate   bool
	MigrationsDir string

	// TLS
	TLSCertPath       string // REST TLS cert path (TS: TLS_CERT_PATH)
	TLSKeyPath        string // REST TLS key path (TS: TLS_KEY_PATH)
	TLSCACertPath     string // gRPC TLS CA cert path (TS: TLS_CA_CERT)
	TLSServerCertPath string // gRPC TLS server cert path (TS: TLS_SERVER_CERT)
	TLSServerKeyPath  string // gRPC TLS server key path (TS: TLS_SERVER_KEY)
	RequireClientCert bool   // gRPC mTLS toggle (TS: REQUIRE_CLIENT_CERT)

	// ClientCertCNAllowlist pins the Subject.CommonName values accepted on
	// incoming mTLS handshakes (AUTH-001). Empty = accept any cert chained
	// to TLSCACertPath (legacy behaviour). Parsed from
	// GRPC_CLIENT_CERT_CN_ALLOWLIST (comma-separated). A non-empty list is
	// enforced via tls.Config.VerifyPeerCertificate in
	// cmd/enclave/main.go:buildGRPCTLSConfig.
	ClientCertCNAllowlist []string

	// MeasurementAllowlist is the set of SEV-SNP launch measurements the
	// enclave will accept at startup. Parsed from ENCLAVE_MEASUREMENT_ALLOWLIST
	// (comma-separated hex strings, case-insensitive). When non-empty, a
	// measurement that is not in the list causes startup to abort in
	// production and to log a warning in development (SEC-106).
	MeasurementAllowlist []string

	// RateLimitTrustedProxies lists the CIDR blocks (or bare IPs) whose
	// X-Forwarded-For / X-Real-IP headers the rate limiter trusts as the real
	// client IP (SEC-004). Parsed from RATE_LIMIT_TRUSTED_PROXIES (comma-sep).
	// Empty = ignore the headers entirely, use TCP RemoteAddr.
	RateLimitTrustedProxies []string

	// ReattestInterval is how often the enclave re-fetches its SEV-SNP
	// attestation and refreshes the signer's binding (SEC-112). Zero disables
	// re-attestation (startup-only). Parsed from ENCLAVE_REATTEST_INTERVAL
	// (Go duration string, e.g. "10m", "5m", "1h"). Default 10m.
	ReattestInterval time.Duration

	// JWTExpectedIssuer pins the `iss` claim required on inbound JWTs
	// (AUTH-002 follow-up). Empty disables the check, which is the legacy
	// behaviour; production deployments should set this to the gateway /
	// report-service identifier so a leaked token from a different issuer
	// bound to the same secret is still rejected. Parsed from
	// ENCLAVE_JWT_EXPECTED_ISSUER.
	JWTExpectedIssuer string

	// RebuilderServiceURL points at the history-rebuilder-service (lives in
	// track_record_site/history-rebuilder-service). The enclave POSTs a job
	// here on connection creation for non-IBKR exchanges (Hyperliquid &
	// future crypto), passing the decrypted credentials to a NON-ZK service.
	// Empty = no-op (dev environments without the rebuilder, IBKR-only
	// deployments). Parsed from REBUILDER_SERVICE_URL.
	RebuilderServiceURL string

	// RebuilderInternalToken is the shared-secret sent as X-Internal-Token
	// to authenticate the enclave→rebuilder call. Must match the rebuilder's
	// REBUILDER_INTERNAL_TOKEN env. Required when RebuilderServiceURL is
	// set; otherwise the client refuses to start.
	RebuilderInternalToken string

	// HistorySyncNotifyURL, when set, is the base URL the enclave POSTs a
	// best-effort "history rebuilt" ping to after a connection's historical
	// backfill completes. The enclave appends the userUID to the path
	// (<url>/<userUID>). It carries NO credentials and triggers nothing
	// inside the enclave — it only lets a downstream service (analytics)
	// run a per-user sync without waiting for its daily cron. Empty = no-op.
	// Parsed from HISTORY_SYNC_NOTIFY_URL.
	HistorySyncNotifyURL string

	// HandoffPeerURL, when non-empty, points at the URL of the previous
	// running enclave's handoff endpoint (B2). Set ONLY during the
	// upgrade window when v_N+1 is meant to fetch the master key from
	// v_N. Once handoff is complete, leave empty so v_N+1 doesn't keep
	// pinging the (now-shut-down) predecessor at every restart.
	// Parsed from HANDOFF_PEER_URL.
	HandoffPeerURL string

	// HandoffPeerTLSFingerprint is the SHA-256 fingerprint of the predecessor
	// enclave's leaf TLS certificate (hex, with or without colons). Required
	// alongside HANDOFF_PEER_URL to prevent MITM on the master-key transfer.
	// Fetch from <predecessor>/api/v1/tls/fingerprint before the upgrade window
	// and populate via scripts/pre-upgrade.sh. Failure mode when absent with
	// HANDOFF_PEER_URL set: boot fails with ErrMissingPeerTLSFingerprint.
	// Parsed from HANDOFF_PEER_TLS_FINGERPRINT.
	HandoffPeerTLSFingerprint string

	// LegacyMasterKeyHex is the raw 32-byte master key (hex-encoded) that
	// the previous enclave used to wrap the active DEK. Set this when the
	// SEV-SNP measurement changed (host migration, firmware update) and the
	// new enclave cannot unwrap the existing DEK with the measurement-derived
	// key. The enclave will use this key for the initial unwrap, then
	// automatically re-wrap the DEK with the new measurement-derived master
	// key. Remove from env once the enclave boots successfully (the re-wrap
	// persists to DB, so subsequent boots no longer need it).
	// Parsed from LEGACY_MASTER_KEY_HEX.
	LegacyMasterKeyHex string

	// HandoffSignedAllowlist holds the operator-signed JSON document
	// listing approved release measurements, supplied via the GCP
	// `signed-allowlist` metadata key. Parsed from HANDOFF_SIGNED_ALLOWLIST.
	// Required alongside HANDOFF_PEER_URL — when empty, the B2 handoff client
	// fails with errMissingSignedAllowlist (cmd/enclave/handoff_wire.go). There
	// is no allowlist embedded in the binary.
	HandoffSignedAllowlist string

	// MeasurementAutoRecovery enables automatic DEK unwrap recovery when the
	// SEV-SNP measurement changes (firmware update, host migration). The enclave
	// queries signed_reports for historical measurements, derives candidate master
	// keys, and retries the unwrap. On success it immediately re-wraps the DEK
	// under the current measurement-derived key so subsequent boots need no
	// operator intervention. Disable only during audits or manual recovery
	// procedures. Failure mode when false: boot fails if measurement changed;
	// operator must set LEGACY_MASTER_KEY_HEX. Parsed from MEASUREMENT_AUTO_RECOVERY.
	MeasurementAutoRecovery bool

	// MeasurementRecoveryLookbackDays controls how far back the auto-recovery
	// scan searches signed_reports for historical measurements. Firmware installs
	// older than this window require LEGACY_MASTER_KEY_HEX. Parsed from
	// MEASUREMENT_RECOVERY_LOOKBACK_DAYS. Failure mode when absent: defaults to
	// 180 days.
	MeasurementRecoveryLookbackDays int

	// ErrTrack configures the in-process error aggregation layer
	// (internal/errtrack). It captures Error+ log entries, fingerprints
	// them by top-N stack frames, and exposes the resulting groups via
	// the same logstream server (auth + TLS + attestation-bound).
	//
	// Disabled by default to keep the change opt-in. When disabled the
	// /errors/* endpoints respond 503.
	ErrTrack ErrTrackConfig
}

// ErrTrackConfig is the tunable surface of the in-process error
// aggregation store. All values have safe defaults; see DefaultCapacity
// / DefaultNewGroupRate in internal/errtrack/store.go for the rationale.
type ErrTrackConfig struct {
	Enabled      bool
	Capacity     int // max distinct groups in memory
	NewGroupRate int // max NEW groups admitted per second (token bucket)
}

func Load() *Config {
	return &Config{
		GRPCPort:      getEnvInt("GRPC_PORT", 50051),
		GRPCInsecure:  getEnvBool("GRPC_INSECURE", false),
		RESTPort:      getEnvInt("REST_PORT", 8080),
		DatabaseURL:   getEnv("DATABASE_URL", ""),
		EncryptionKey: getEncryptionKey(),
		Env:           getEnv("ENV", "development"),
		LogLevel:      getEnv("LOG_LEVEL", "info"),

		LogStreamPort:   getEnvInt("LOG_STREAM_PORT", 50052),
		LogStreamAPIKey: getEnv("LOG_STREAM_API_KEY", ""),
		MetricsPort:     getEnvInt("METRICS_PORT", 9090),
		MetricsEnabled:  getEnvBool("METRICS_ENABLED", true),

		ExchangeHTTPProxy: getEnv("EXCHANGE_HTTP_PROXY", ""),
		ProxyExchanges:    getEnv("PROXY_EXCHANGES", "binance"),

		CORSOrigin: getEnv("CORS_ORIGIN", ""),

		BenchmarkServiceURL: getEnv("BENCHMARK_SERVICE_URL", ""),

		DataRetentionDays: getEnvInt("DATA_RETENTION_DAYS", 30),

		EnableDailySync:  getEnvBool("ENABLE_DAILY_SYNC", true),
		EnableLegacyREST: getEnvBool("ENABLE_LEGACY_REST", false),

		AutoMigrate:   getEnvBool("AUTO_MIGRATE", false),
		MigrationsDir: getEnv("MIGRATIONS_DIR", "migrations"),

		TLSCertPath:       getEnv("TLS_CERT_PATH", "/app/certs/cert.pem"),
		TLSKeyPath:        getEnv("TLS_KEY_PATH", "/app/certs/key.pem"),
		TLSCACertPath:     getEnv("TLS_CA_CERT", "/etc/enclave/ca.crt"),
		TLSServerCertPath: getEnv("TLS_SERVER_CERT", "/etc/enclave/server.crt"),
		TLSServerKeyPath:  getEnv("TLS_SERVER_KEY", "/etc/enclave/server.key"),
		RequireClientCert: getEnvBool("REQUIRE_CLIENT_CERT", false),

		MeasurementAllowlist:    parseMeasurementAllowlist(getEnv("ENCLAVE_MEASUREMENT_ALLOWLIST", "")),
		RateLimitTrustedProxies: parseCommaList(getEnv("RATE_LIMIT_TRUSTED_PROXIES", "")),
		ReattestInterval:        getEnvDuration("ENCLAVE_REATTEST_INTERVAL", 10*time.Minute),
		ClientCertCNAllowlist:   parseCommaList(getEnv("GRPC_CLIENT_CERT_CN_ALLOWLIST", "")),
		JWTExpectedIssuer:       strings.TrimSpace(getEnv("ENCLAVE_JWT_EXPECTED_ISSUER", "")),

		RebuilderServiceURL:    strings.TrimSpace(getEnv("REBUILDER_SERVICE_URL", "")),
		RebuilderInternalToken: strings.TrimSpace(getEnv("REBUILDER_INTERNAL_TOKEN", "")),
		HistorySyncNotifyURL:   strings.TrimSpace(getEnv("HISTORY_SYNC_NOTIFY_URL", "")),

		HandoffPeerURL:            strings.TrimSpace(getEnv("HANDOFF_PEER_URL", "")),
		HandoffPeerTLSFingerprint: strings.TrimSpace(getEnv("HANDOFF_PEER_TLS_FINGERPRINT", "")),
		HandoffSignedAllowlist: getEnv("HANDOFF_SIGNED_ALLOWLIST", ""),
		LegacyMasterKeyHex:    strings.TrimSpace(getEnv("LEGACY_MASTER_KEY_HEX", "")),

		MeasurementAutoRecovery:         getEnvBool("MEASUREMENT_AUTO_RECOVERY", true),
		MeasurementRecoveryLookbackDays: getEnvInt("MEASUREMENT_RECOVERY_LOOKBACK_DAYS", 180),

		ErrTrack: ErrTrackConfig{
			Enabled:      getEnvBool("ERRTRACK_ENABLED", false),
			Capacity:     getEnvInt("ERRTRACK_CAPACITY", 0),       // 0 → DefaultCapacity
			NewGroupRate: getEnvInt("ERRTRACK_NEW_GROUP_RATE", 0), // 0 → DefaultNewGroupRate
		},
	}
}

// getEnvDuration parses a Go duration env var, falling back to `fallback` on
// missing or malformed input.
func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

// parseCommaList splits a comma-separated env value into a deduped list,
// trimming whitespace. Returns nil when the input is empty.
func parseCommaList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// parseMeasurementAllowlist splits a comma-separated list of hex measurements,
// lowercases and strips whitespace. Empty entries are discarded. An empty
// input returns nil (allowlist disabled).
func parseMeasurementAllowlist(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.ToLower(strings.TrimSpace(p))
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c *Config) IsDevelopment() bool {
	return c.Env != "production"
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return fallback
}

func getEncryptionKey() []byte {
	// DEK_OVERRIDE takes priority — used when migrating from TS enclave
	// (TS derived master key from measurement, unwrapped DEK; we use the DEK directly)
	if dekHex := os.Getenv("DEK_OVERRIDE"); dekHex != "" {
		key, err := hex.DecodeString(dekHex)
		if err == nil && len(key) == 32 {
			return key
		}
	}

	keyHex := os.Getenv("ENCRYPTION_KEY")
	if keyHex != "" {
		key, err := hex.DecodeString(keyHex)
		if err == nil && len(key) == 32 {
			return key
		}
	}

	// Development fallback: generate random key (not for production!)
	key := make([]byte, 32)
	rand.Read(key)
	return key
}

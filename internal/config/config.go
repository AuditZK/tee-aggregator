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

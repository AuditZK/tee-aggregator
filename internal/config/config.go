package config

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
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
	}
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

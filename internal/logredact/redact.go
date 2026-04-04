// Package logredact provides a zap.Core wrapper that redacts sensitive fields
// from log output. ALWAYS active in all environments for deterministic auditing.
//
// Two-tier redaction system (TS parity):
//
//	TIER 1: Credentials & secrets (api_key, password, token, encryption_key, etc.)
//	TIER 2: Business data (user_uid, exchange, equity, balance, trades, etc.)
//
// An auditor can verify that NO sensitive data ever leaves the enclave in logs.
//
// SECURITY: Redaction is ALWAYS active — no config toggle, no environment check.
// This ensures deterministic behavior for security audits.
package logredact

import (
	"strings"

	"go.uber.org/zap/zapcore"
)

const redacted = "[REDACTED]"

// TIER 1: Credentials & secrets — always redacted.
// Uses prefix matching (TS parity: regex with ^ anchor).
var tier1Prefixes = []string{
	"api_key", "api_secret", "apikey", "apisecret",
	"access_key", "secret_key", "access_token", "refresh_token",
	"password", "passwd", "pwd",
	"token", "bearer", "jwt",
	"encryption_key", "private_key", "secret",
	"authorization", "credentials", "passphrase",
	"encrypted", "ciphertext", "auth_tag",
	"hmac", "dek",
}

// TIER 1: Substring patterns (match anywhere in field name).
var tier1Contains = []string{
	"signature",
}

// TIER 2: Business-sensitive data — always redacted.
// Uses prefix matching to avoid false positives (e.g., "hostname" != "name").
var tier2Prefixes = []string{
	// User identification
	"user_uid", "user_id", "account_id", "customer_id", "uid",
	// Exchange identification
	"exchange", "broker",
	// Financial amounts
	"balance", "equity", "amount", "price", "total_equity",
	"total_balance", "total_return", "annualized",
	"pnl", "profit", "loss", "fee", "commission",
	"deposit", "withdrawal", "margin", "collateral",
	"cash", "stock", "commodit", "unrealized", "realized",
	"liquidation", "accrual",
	"sharpe", "sortino", "calmar", "volatility", "drawdown",
	"var_", "nav", "aum",
	// Trading activity
	"trade", "position", "order", "quantity", "synced",
	// Wallet
	"wallet",
}

// TIER 2: Exact match patterns (avoid "hostname" matching "name").
var tier2Exact = []string{
	"name", "email", "phone", "address", "ssn", "tax_id",
	"size", "volume", "count", "label",
}

// Fields that should NEVER be redacted (safe operational fields).
var safeFields = map[string]struct{}{
	"level": {}, "ts": {}, "caller": {}, "msg": {},
	"method": {}, "path": {}, "duration": {},
	"port": {}, "mode": {}, "status": {},
	"version": {}, "env": {}, "policy": {},
	"error": {}, "stacktrace": {},
	"dir": {}, "hint": {}, "table": {}, "column": {},
	"next_sync_in": {}, "next_sync_at": {},
	"grpc_port": {}, "rest_port": {},
	"database": {}, "tls": {}, "e2e": {},
	"attestation": {}, "log_stream": {}, "metrics": {},
	"algorithm": {}, "fingerprint": {},
	"hardware_key": {}, "master_key_id": {},
	"https": {}, "legacy_rest": {}, "addr": {},
	"cert_path": {}, "key_path": {},
	"service": {}, "scope": {}, "vmlock": {},
}

// shouldRedact checks if a field name matches any sensitive pattern.
// Order: safe list (fast exit) → tier 1 → tier 2.
func shouldRedact(field string) bool {
	lower := strings.ToLower(field)

	// Fast path: safe fields are never redacted
	if _, safe := safeFields[lower]; safe {
		return false
	}

	// TIER 1: credentials & secrets
	for _, p := range tier1Prefixes {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	for _, p := range tier1Contains {
		if strings.Contains(lower, p) {
			return true
		}
	}

	// TIER 2: business data
	for _, p := range tier2Prefixes {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	for _, p := range tier2Exact {
		if lower == p {
			return true
		}
	}

	return false
}

// NewRedactCore wraps a zap.Core to redact sensitive fields.
// SECURITY: Redaction is ALWAYS active — no config toggle.
func NewRedactCore(next zapcore.Core) zapcore.Core {
	return &redactCore{Core: next}
}

type redactCore struct {
	zapcore.Core
}

func (c *redactCore) With(fields []zapcore.Field) zapcore.Core {
	return &redactCore{Core: c.Core.With(redactFields(fields))}
}

func (c *redactCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Core.Enabled(entry.Level) {
		return ce.AddCore(entry, c)
	}
	return ce
}

func (c *redactCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	// Scrub the log message itself for any leaked secrets
	entry.Message = scrubMessage(entry.Message)
	return c.Core.Write(entry, redactFields(fields))
}

func redactFields(fields []zapcore.Field) []zapcore.Field {
	out := make([]zapcore.Field, len(fields))
	for i, f := range fields {
		if shouldRedact(f.Key) {
			out[i] = zapcore.Field{
				Key:    f.Key,
				Type:   zapcore.StringType,
				String: redacted,
			}
		} else {
			// Also check if the string VALUE contains sensitive patterns
			if f.Type == zapcore.StringType && containsSensitiveValue(f.String) {
				out[i] = zapcore.Field{
					Key:    f.Key,
					Type:   zapcore.StringType,
					String: redacted,
				}
			} else {
				out[i] = f
			}
		}
	}
	return out
}

// containsSensitiveValue checks if a string value contains patterns like
// "api_key=xxx" or "password:xxx" that could leak secrets in error messages.
func containsSensitiveValue(val string) bool {
	lower := strings.ToLower(val)
	for _, p := range tier1Prefixes {
		if strings.Contains(lower, p+"=") || strings.Contains(lower, p+":") {
			return true
		}
	}
	return false
}

// scrubMessage redacts sensitive patterns from the log message text itself.
// Catches cases like logger.Info("Connecting to postgresql://user:pass@host/db")
func scrubMessage(msg string) string {
	lower := strings.ToLower(msg)

	// Redact connection strings
	if strings.Contains(lower, "postgresql://") || strings.Contains(lower, "postgres://") {
		return redacted
	}

	// Redact anything that looks like a key=value with sensitive key
	for _, p := range tier1Prefixes {
		if strings.Contains(lower, p) {
			return redacted
		}
	}

	return msg
}

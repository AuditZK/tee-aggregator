// Package logredact provides a zap.Core wrapper that redacts sensitive fields
// from log output. ALWAYS active in all environments for deterministic auditing.
//
// Two-tier redaction system (TS parity):
//
//	TIER 1: Credentials & secrets (api_key, password, token, encryption_key, etc.)
//	TIER 2: Business data (user_uid, exchange, equity, balance, trades, etc.)
//
// An auditor can verify that NO sensitive data ever leaves the enclave in logs.
package logredact

import (
	"strings"

	"go.uber.org/zap/zapcore"
)

const redacted = "[REDACTED]"

// TIER 1: Credentials & secrets — always redacted
var tier1Patterns = []string{
	"api_key", "api_secret", "apikey", "apisecret",
	"access_key", "secret_key", "access_token", "refresh_token",
	"password", "passwd", "pwd",
	"token", "bearer", "jwt",
	"encryption_key", "private_key", "secret",
	"authorization", "credentials", "passphrase",
	"encrypted", "ciphertext", "auth_tag",
	"hmac", "signature",
}

// TIER 2: Business-sensitive data — always redacted
var tier2Patterns = []string{
	// User identification
	"user_uid", "user_id", "account_id", "customer_id",
	// Exchange identification
	"exchange", "broker", "platform",
	// Financial amounts
	"balance", "equity", "amount", "price", "total",
	"pnl", "profit", "loss", "fee", "commission",
	"deposit", "withdrawal", "margin", "collateral",
	"cash", "stock", "commodit", "unrealized", "realized",
	"liquidation", "accrual",
	// Trading activity
	"trade", "position", "order", "quantity", "size", "volume",
	"synced", "count",
	// PII
	"name", "email", "phone", "address", "ssn", "tax_id",
	// Wallet
	"wallet", "public_key",
}

// shouldRedact checks if a field name matches any sensitive pattern.
func shouldRedact(field string) bool {
	lower := strings.ToLower(field)
	for _, p := range tier1Patterns {
		if strings.Contains(lower, p) {
			return true
		}
	}
	for _, p := range tier2Patterns {
		if strings.Contains(lower, p) {
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
			out[i] = f
		}
	}
	return out
}

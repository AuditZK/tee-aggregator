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
	"encoding/json"
	"regexp"
	"strings"

	"go.uber.org/zap/zapcore"
)

const redacted = "[REDACTED]"

// valueScrubRegexes catches vendor-specific credential shapes that don't
// match the tier-1 field-name prefixes. Each pattern is replaced with
// `[REDACTED]` (preserving the surrounding diagnostic text). LOG-001.
var valueScrubRegexes = []*regexp.Regexp{
	// Signed-URL HMAC (e.g. ?timestamp=1&signature=<hex>). The value runs
	// until the next URL delimiter or quote.
	regexp.MustCompile(`(?i)signature=[^&\s"'\\]+`),
	// AWS-style key id — camelCase, so it does not match the underscored
	// `access_key` prefix in tier1Prefixes.
	regexp.MustCompile(`(?i)accesskeyid=[^&\s"'\\]+`),
	// OAuth client_secret form param — `secret_key` prefix in
	// tier1Prefixes would match `secret_key=` but not `client_secret=`.
	regexp.MustCompile(`(?i)client_secret=[^&\s"'\\]+`),
	// HTTP Basic authorization header — the base64 payload holds
	// key:secret and must be dropped whole.
	regexp.MustCompile(`(?i)Authorization:\s*Basic\s+[A-Za-z0-9+/=._-]+`),
}

// scrubSensitiveSubstrings applies every valueScrubRegex to s, replacing
// each match with `[REDACTED]`. It is safe to call on arbitrary text;
// when no pattern matches, the input is returned unchanged.
func scrubSensitiveSubstrings(s string) string {
	for _, re := range valueScrubRegexes {
		s = re.ReplaceAllString(s, redacted)
	}
	return s
}

// maskValue returns a redacted version of val. It first applies the
// surgical regex scrubs (so diagnostic context survives); if the result
// still contains a tier-1 key=value substring the whole thing is replaced
// with `[REDACTED]` — when in doubt, drop the value rather than leak a
// pattern the scrubber missed.
func maskValue(val string) string {
	scrubbed := scrubSensitiveSubstrings(val)
	if containsSensitiveValue(scrubbed) {
		return redacted
	}
	return scrubbed
}

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
			out[i] = zapcore.Field{Key: f.Key, Type: zapcore.StringType, String: redacted}
			continue
		}
		out[i] = maskFieldValue(f)
	}
	return out
}

// RedactFields applies the same field-level redaction as the internal
// redactCore.Write path. Exported so downstream cores (notably the SSE
// BroadcastCore) can scrub the entries they observe BEFORE forwarding
// them — without this, wrapping a redacted core as the broadcast core's
// `inner` only protects the stderr path, not the broadcast path
// (LOG-AUDIT-001).
//
// Returns a new slice; the input is never mutated.
func RedactFields(fields []zapcore.Field) []zapcore.Field {
	return redactFields(fields)
}

// ScrubMessage redacts secret substrings (and full connection strings)
// from a log message text. Exported alongside RedactFields for the same
// reason — see that doc.
func ScrubMessage(msg string) string {
	return scrubMessage(msg)
}

// maskFieldValue scrubs the value of a single field. LOG-001 (extended):
//   - StringType / ErrorType: tier-1 prefix + URL-encoded scrub.
//   - ReflectType: marshal the underlying interface{} to JSON, run the same
//     scrub over the JSON text, and emit it as a single string field.
//     This covers `zap.Any`, `zap.Reflect`, `zap.Inline`, and any
//     constructor that ends up routing through Reflect (the broad bucket
//     for "I don't have a typed zap field for this").
//
// ObjectMarshaler / ArrayMarshaler are NOT scrubbed (zap encodes them
// piecewise into the encoder's writer, which we don't control here).
// Callers that need redaction-safe object fields must continue to use
// pre-scrubbed strings.
func maskFieldValue(f zapcore.Field) zapcore.Field {
	switch f.Type {
	case zapcore.StringType:
		if masked := maskValue(f.String); masked != f.String {
			return zapcore.Field{Key: f.Key, Type: zapcore.StringType, String: masked}
		}
	case zapcore.ErrorType:
		err, ok := f.Interface.(error)
		if !ok || err == nil {
			return f
		}
		raw := err.Error()
		if masked := maskValue(raw); masked != raw {
			return zapcore.Field{Key: f.Key, Type: zapcore.StringType, String: masked}
		}
	case zapcore.ReflectType:
		// LOG-001 (extended): zap.Any / zap.Reflect were previously a hole
		// in the redactor — a benign-named field with a credential-bearing
		// struct value would render unscrubbed. Marshalling once to JSON
		// gives us a string we can scrub with the same tier-1 / tier-2
		// rules as everything else.
		raw, err := json.Marshal(f.Interface)
		if err != nil {
			// Marshalling failed — fall back to the safe default: redact
			// the whole field rather than risk leaking via the encoder.
			return zapcore.Field{Key: f.Key, Type: zapcore.StringType, String: redacted}
		}
		text := string(raw)
		if masked := maskValue(text); masked != text {
			return zapcore.Field{Key: f.Key, Type: zapcore.StringType, String: masked}
		}
	}
	return f
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

	// LOG-001: apply surgical scrubs for vendor-specific patterns before
	// the tier-1 prefix check, so a message like "refresh failed: Basic …"
	// keeps its diagnostic prefix while the secret is dropped.
	scrubbed := scrubSensitiveSubstrings(msg)
	scrubbedLower := strings.ToLower(scrubbed)

	// Redact anything that looks like a key=value with sensitive key
	for _, p := range tier1Prefixes {
		if strings.Contains(scrubbedLower, p) {
			return redacted
		}
	}

	return scrubbed
}

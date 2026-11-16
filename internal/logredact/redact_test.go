package logredact

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestShouldRedact_Tier1(t *testing.T) {
	tier1Fields := []string{
		"api_key", "api_secret", "password", "token",
		"encryption_key", "private_key", "passphrase",
		"encrypted_api_key", "auth_tag", "dek",
	}
	for _, f := range tier1Fields {
		if !shouldRedact(f) {
			t.Errorf("expected %q to be redacted (tier 1)", f)
		}
	}
}

func TestShouldRedact_Tier2(t *testing.T) {
	tier2Fields := []string{
		"user_uid", "exchange", "equity", "balance",
		"total_equity", "pnl", "deposit", "withdrawal",
		"trade", "position", "sharpe", "volatility",
	}
	for _, f := range tier2Fields {
		if !shouldRedact(f) {
			t.Errorf("expected %q to be redacted (tier 2)", f)
		}
	}
}

func TestShouldRedact_Tier2Exact(t *testing.T) {
	exact := []string{"name", "email", "phone", "size", "volume", "label"}
	for _, f := range exact {
		if !shouldRedact(f) {
			t.Errorf("expected exact match %q to be redacted", f)
		}
	}
}

func TestShouldNotRedact_SafeFields(t *testing.T) {
	safe := []string{
		"level", "ts", "caller", "msg", "method", "path",
		"duration", "port", "mode", "status", "version", "env",
		"error", "dir", "hint", "table", "column",
		"next_sync_in", "grpc_port", "rest_port",
		"database", "tls", "algorithm",
	}
	for _, f := range safe {
		if shouldRedact(f) {
			t.Errorf("expected %q to NOT be redacted (safe field)", f)
		}
	}
}

func TestShouldNotRedact_NoFalsePositives(t *testing.T) {
	// "hostname" should NOT match "name" (prefix matching)
	// "filename" should NOT match "name"
	safe := []string{"hostname", "filename", "pathname", "timezone"}
	for _, f := range safe {
		if shouldRedact(f) {
			t.Errorf("expected %q to NOT be redacted (false positive)", f)
		}
	}
}

func TestScrubMessage_ConnectionString(t *testing.T) {
	msg := "Connecting to postgresql://user:pass@host/db"
	if result := scrubMessage(msg); result != redacted {
		t.Errorf("expected connection string to be scrubbed, got %q", result)
	}
}

func TestScrubMessage_SafeMessage(t *testing.T) {
	msg := "gRPC server starting on port 50051"
	if result := scrubMessage(msg); result != msg {
		t.Errorf("expected safe message to pass through, got %q", result)
	}
}

func TestContainsSensitiveValue(t *testing.T) {
	if !containsSensitiveValue("failed: api_key=abc123") {
		t.Error("expected value with api_key= to be sensitive")
	}
	if containsSensitiveValue("normal error message") {
		t.Error("expected normal message to not be sensitive")
	}
}

func TestMaskValue_KeepsBrokerDetailBehindWrapPhrase(t *testing.T) {
	cases := []string{
		`invalid credentials: ig session HTTP 403: {"errorCode":"error.public-api.failure.encryption.required"}`,
		`invalid credentials: ig session HTTP 401: {"errorCode":"error.security.api-key-invalid"}`,
		"invalid credentials: bitget API error: Apikey does not exist (code 40037)",
		"Invalid Credentials: cTrader error ACCESS_DENIED: synthetic detail",
		"sync: decrypt credentials: cipher: message authentication failed",
		"init DEK: load active dek: no rows in result set",
		"decrypt api secret: decode ciphertext: illegal base64 data at input byte 4",
		"rebind credentials: production schema only",
	}
	for _, v := range cases {
		if got := maskValue(v); got != v {
			t.Errorf("maskValue(%q) = %q, want it unchanged", v, got)
		}
	}
}

func TestMaskValue_SecretsStillRedacted(t *testing.T) {
	const secret = "synthetic-secret-1234"
	cases := []string{
		"password: " + secret,
		"password:" + secret,
		"token=" + secret,
		"credentials=" + secret,
		"credentials:" + secret,
		"api_key: " + secret,
		"Authorization: Bearer " + secret,
		"refresh failed, api secret: " + secret,
		"map[apikey:" + secret + "]",
		`{"password":"` + secret + `"}`,
		`{"api_secret": "` + secret + `"}`,
		"invalid credentials: password=" + secret,
		"invalid credentials: token: " + secret,
		"decrypt credentials: passphrase:" + secret,
		`invalid credentials: mt-bridge status 422: {"input":{"password":"` + secret + `"}}`,
	}
	for _, v := range cases {
		if got := maskValue(v); got != redacted {
			t.Errorf("maskValue(%q) = %q, want %q", v, got, redacted)
		}
	}
}

func TestMaskValue_RegexScrubBehindWrapPhrase(t *testing.T) {
	const sig = "0123456789abcdef"
	v := `invalid credentials: Get "https://api.example.test/v1/account?ts=1&signature=` + sig + `": EOF`
	got := maskValue(v)
	if strings.Contains(got, sig) {
		t.Fatalf("signature survived: %q", got)
	}
	if !strings.HasPrefix(got, "invalid credentials: Get") || !strings.Contains(got, redacted) {
		t.Fatalf("diagnostic text lost: %q", got)
	}
}

func TestRedactCore_ConnectRefusalKeepsBrokerCode(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(NewRedactCore(core))

	cause := errors.New(`ig session HTTP 401: {"errorCode":"error.security.api-key-invalid"}`)
	logger.Warn("create connection from E2E refused",
		zap.String("reason", "invalid credentials"),
		zap.Error(fmt.Errorf("invalid credentials: %w", cause)),
		zap.String("exchange", "ig"),
	)

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if got, _ := fields["error"].(string); !strings.Contains(got, "error.security.api-key-invalid") {
		t.Fatalf("broker error code lost: %q", got)
	}
	if fields["exchange"] != redacted {
		t.Fatalf("exchange must stay masked, got %v", fields["exchange"])
	}
}

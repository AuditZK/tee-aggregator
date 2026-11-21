package logredact

import "testing"

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

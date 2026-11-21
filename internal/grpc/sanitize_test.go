package grpc

import (
	"strings"
	"testing"
)

// SEC-07: in production, a matched error category must surface a fixed,
// infrastructure-free message — never the raw error text, which can carry an
// internal host:port, file path, or stack frame.
func TestSanitizeMessageForClient_Production(t *testing.T) {
	t.Setenv("NODE_ENV", "")
	t.Setenv("ENV", "production")
	s := &Server{}

	// The leak vector: a stdlib network error embedding an internal host:port.
	got := s.sanitizeMessageForClient("dial tcp internal-db:5432: connect: connection refused")
	if got != "could not reach the exchange endpoint" {
		t.Fatalf("network error not categorised: %q", got)
	}
	if strings.Contains(got, "internal-db") || strings.Contains(got, "5432") {
		t.Fatalf("internal host:port leaked: %q", got)
	}

	// Actionable categories keep a recognisable message.
	if got := s.sanitizeMessageForClient("exchange rejected request: invalid credentials"); got != "invalid credentials" {
		t.Fatalf("credentials category: %q", got)
	}
	if got := s.sanitizeMessageForClient("IP not whitelisted for this key"); got != "server IP not whitelisted on the exchange" {
		t.Fatalf("whitelist category: %q", got)
	}

	// Anything unmatched collapses to the generic message.
	if got := s.sanitizeMessageForClient("panic: nil deref at /app/internal/foo.go:42"); got != genericInternalError {
		t.Fatalf("unmatched error not generic: %q", got)
	}
}

// Dev mode is unchanged: the raw error passes through for local debugging.
func TestSanitizeMessageForClient_DevPassesThrough(t *testing.T) {
	t.Setenv("NODE_ENV", "")
	t.Setenv("ENV", "development")
	s := &Server{}
	raw := "dial tcp internal-db:5432: connect: connection refused"
	if got := s.sanitizeMessageForClient(raw); got != raw {
		t.Fatalf("dev mode must pass the raw error through, got %q", got)
	}
}

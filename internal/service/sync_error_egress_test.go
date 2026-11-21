package service

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// SEC-12: SyncResult.Error is persisted in sync_statuses and serialised to
// REST/gRPC without passing through either output sanitizer, so it must carry
// a category and never the raw text — which is where SEC-11's Flex token, pgx
// detail and internal host:port ride out.
func TestEgressSyncErrorDropsRawText(t *testing.T) {
	const token = "SYNTHETIC1234567890TOKEN"
	err := fmt.Errorf(`Get "https://gdcdyn.interactivebrokers.com/x?q=1&t=%s&v=3": dial tcp 10.0.0.5:443: connect: connection refused`, token)

	got := egressSyncError("get balance", err)
	if strings.Contains(got, token) {
		t.Fatalf("flex token reached SyncResult.Error: %s", got)
	}
	if strings.Contains(got, "10.0.0.5") || strings.Contains(got, "gdcdyn") {
		t.Fatalf("infrastructure detail reached SyncResult.Error: %s", got)
	}
	if got != "get balance: could not reach the exchange endpoint" {
		t.Fatalf("category lost: %s", got)
	}
}

func TestEgressSyncErrorUnmatchedIsGeneric(t *testing.T) {
	err := errors.New("panic: nil deref at /app/internal/service/sync.go:699")
	got := egressSyncError("save snapshot", err)
	if got != "save snapshot: "+genericSyncFailure {
		t.Fatalf("unmatched error not collapsed: %s", got)
	}
	if strings.Contains(got, "/app/") {
		t.Fatalf("file path leaked: %s", got)
	}
}

func TestEgressSyncErrorWithoutOperation(t *testing.T) {
	if got := egressSyncError("", errors.New("invalid credentials")); got != "invalid credentials" {
		t.Fatalf("bare category: %s", got)
	}
}

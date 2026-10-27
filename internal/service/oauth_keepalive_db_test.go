package service

import (
	"strings"
	"testing"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

// End to end on the production schema: the keep-alive finds the Saxo row,
// rotates it through the real guard, stores the new pair bound to the row, and
// a refusal lands in sync_statuses under the reauth marker.
func TestDBKeepAliveRotatesTheStoredPairAndMarksARefusal(t *testing.T) {
	h := newDBHarness(t)
	broker := newFakeRotatingBroker()
	first := broker.grant()
	h.seedConnection(t, "saxo_sim", "Saxo Account", first.AccessToken, first.RefreshToken)

	k := NewOAuthKeepAlive(h.connSvc, h.sync, zap.NewNop())
	k.minAge = 0
	k.build = func(*repository.ExchangeConnection) (connector.Connector, error) {
		return &keepAliveFakeConnector{broker: broker}, nil
	}

	if counts := k.runOnce(h.ctx); counts[keepAliveOK] != 1 {
		t.Fatalf("outcomes = %v, want one rotation", counts)
	}
	creds, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "saxo_sim", "Saxo Account")
	if err != nil {
		t.Fatalf("read the row back: %v", err)
	}
	if creds.APISecret == first.RefreshToken || !broker.alive(creds.APISecret) {
		t.Fatal("the row does not hold the rotated, live refresh token")
	}

	broker.mu.Lock()
	delete(broker.valid, creds.APISecret)
	broker.mu.Unlock()
	if counts := k.runOnce(h.ctx); counts[keepAliveRefused] != 1 {
		t.Fatalf("outcomes = %v, want one refusal", counts)
	}
	if st := h.status(t, "saxo_sim", "Saxo Account"); !strings.HasPrefix(st.ErrorMessage, "reauth_required:") {
		t.Fatalf("status = %q, want the reauth marker", st.ErrorMessage)
	}
}

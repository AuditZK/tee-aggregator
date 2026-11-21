package service

import (
	"testing"

	"github.com/trackrecord/enclave/internal/encryption"
)

// bindRowInPlace rewrites a seeded row's ciphertexts in the bound format, the
// way a later release stores them, so this one can be checked to read them.
func (h *dbHarness) bindRowInPlace(t *testing.T, label, apiKey, apiSecret string) {
	t.Helper()
	conn, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "binance", label)
	if err != nil {
		t.Fatal(err)
	}
	key, err := h.enc.EncryptTSBound(apiKey, encryption.ConnectionFieldAAD(conn.UserUID, conn.ID, encryption.FieldAPIKey))
	if err != nil {
		t.Fatal(err)
	}
	secret, err := h.enc.EncryptTSBound(apiSecret, encryption.ConnectionFieldAAD(conn.UserUID, conn.ID, encryption.FieldAPISecret))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(h.ctx, `UPDATE exchange_connections SET "encryptedApiKey" = $1, "encryptedApiSecret" = $2 WHERE id = $3`, key, secret, conn.ID); err != nil {
		t.Fatal(err)
	}
}

func TestDBReadsBoundCredentials(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	h.bindRowInPlace(t, "main", dbKey, dbSecret)

	creds, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "binance", "main")
	if err != nil || creds.APIKey != dbKey || creds.APISecret != dbSecret {
		t.Fatalf("bound row read as %+v, %v", creds, err)
	}
}

func TestDBBoundCredentialsDoNotOpenInAnotherRow(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "a", "key-of-a", "secret-of-a")
	h.seedConnection(t, "binance", "b", "key-of-b", "secret-of-b")
	h.bindRowInPlace(t, "a", "key-of-a", "secret-of-a")
	if _, err := h.pool.Exec(h.ctx, `UPDATE exchange_connections AS b
		SET "encryptedApiKey" = a."encryptedApiKey", "encryptedApiSecret" = a."encryptedApiSecret"
		FROM exchange_connections AS a WHERE a.label = 'a' AND b.label = 'b'`); err != nil {
		t.Fatal(err)
	}

	if creds, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "binance", "b"); err == nil {
		t.Fatalf("row b opened row a's bound credentials as %q", creds.APIKey)
	}
}

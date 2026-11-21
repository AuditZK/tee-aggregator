package service

import (
	"strings"
	"testing"

	"github.com/trackrecord/enclave/internal/encryption"
)

func (h *dbHarness) storedKey(t *testing.T, label string) (key, secret string) {
	t.Helper()
	if err := h.pool.QueryRow(h.ctx, `SELECT "encryptedApiKey", "encryptedApiSecret" FROM exchange_connections WHERE label = $1`, label).Scan(&key, &secret); err != nil {
		t.Fatal(err)
	}
	return key, secret
}

func TestDBCreateStoresBoundCredentials(t *testing.T) {
	t.Setenv("ENV", "")
	t.Setenv("NODE_ENV", "")
	h := newDBHarness(t)
	if _, err := h.users.GetOrCreate(h.ctx, dbUser); err != nil {
		t.Fatal(err)
	}
	if err := h.connSvc.Create(h.ctx, &CreateConnectionRequest{
		UserUID: dbUser, Exchange: "mock", Label: "main", APIKey: dbKey, APISecret: dbSecret,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	key, secret := h.storedKey(t, "main")
	if !encryption.IsBoundTS(key) || !encryption.IsBoundTS(secret) {
		t.Fatalf("new connection stored unbound ciphertexts")
	}
}

func TestDBLegacyRowIsBoundOnFirstRead(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	if key, _ := h.storedKey(t, "main"); encryption.IsBoundTS(key) {
		t.Fatal("seed is expected to be a legacy row")
	}

	for i := 0; i < 2; i++ {
		creds, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "binance", "main")
		if err != nil || creds.APIKey != dbKey || creds.APISecret != dbSecret {
			t.Fatalf("read %d = %+v, %v", i, creds, err)
		}
		key, secret := h.storedKey(t, "main")
		if !encryption.IsBoundTS(key) || !encryption.IsBoundTS(secret) {
			t.Fatalf("read %d left the row unbound", i)
		}
		for _, v := range []string{key, secret} {
			if strings.Contains(v, dbKey) || strings.Contains(v, dbSecret) {
				t.Fatal("stored value contains plaintext")
			}
		}
	}
}

func TestDBBoundCiphertextDoesNotOpenElsewhere(t *testing.T) {
	cases := map[string]string{
		"moved to another row": `UPDATE exchange_connections AS b
			SET "encryptedApiKey" = a."encryptedApiKey", "encryptedApiSecret" = a."encryptedApiSecret"
			FROM exchange_connections AS a WHERE a.label = 'a' AND b.label = 'b'`,
		"swapped between the fields of its row": `UPDATE exchange_connections
			SET "encryptedApiKey" = "encryptedApiSecret", "encryptedApiSecret" = "encryptedApiKey"
			WHERE label = 'b'`,
	}
	for name, tamper := range cases {
		t.Run(name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "binance", "a", "key-of-a", "secret-of-a")
			h.seedConnection(t, "binance", "b", "key-of-b", "secret-of-b")
			for _, label := range []string{"a", "b"} {
				if _, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "binance", label); err != nil {
					t.Fatalf("bind %s: %v", label, err)
				}
			}
			if _, err := h.pool.Exec(h.ctx, tamper); err != nil {
				t.Fatal(err)
			}
			if creds, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "binance", "b"); err == nil {
				t.Fatalf("tampered row b opened as %q", creds.APIKey)
			}
		})
	}
}

func TestDBRebindYieldsToAConcurrentTokenRotation(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ctrader", "main", "old-access", "old-refresh")
	stale, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "ctrader", "main")
	if err != nil {
		t.Fatal(err)
	}

	if err := h.connSvc.PersistOAuthTokens(h.ctx, dbUser, "ctrader", "main", "new-access", "new-refresh"); err != nil {
		t.Fatal(err)
	}
	h.connSvc.rebindLegacyRow(h.ctx, stale, &Credentials{APIKey: "old-access", APISecret: "old-refresh"})

	creds, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "ctrader", "main")
	if err != nil {
		t.Fatal(err)
	}
	if creds.APIKey != "new-access" || creds.APISecret != "new-refresh" {
		t.Fatalf("tokens = %q/%q: a stale rebind overwrote the rotation", creds.APIKey, creds.APISecret)
	}
}

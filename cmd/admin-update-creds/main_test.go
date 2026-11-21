package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	"github.com/trackrecord/enclave/internal/testdb"
)

// The tool and the service must bind the same bytes, or a credential the
// operator repairs becomes one the enclave can no longer open.
func TestUpdateTSWritesWhatTheServiceOpens(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	enc, err := encryption.New(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.NewUserRepo(pool).GetOrCreate(ctx, "test-user-tool"); err != nil {
		t.Fatal(err)
	}
	conns := repository.NewConnectionRepo(pool)
	if !conns.IsTSSchema(ctx) {
		t.Fatal("test database is not on the production schema")
	}
	oldKey, _ := enc.EncryptTSString("old-key")
	oldSecret, _ := enc.EncryptTSString("old-secret")
	if err := conns.Create(ctx, &repository.ExchangeConnection{
		UserUID: "test-user-tool", Exchange: "lighter", Label: "main",
		EncryptedAPIKey: oldKey, EncryptedAPISecret: oldSecret,
	}); err != nil {
		t.Fatal(err)
	}

	if err := updateTS(ctx, pool, enc, updateCredsArgs{
		UserUID: "test-user-tool", Exchange: "lighter", Label: "main",
		APIKey: "new-key", APISecret: "new-secret", Passphrase: "new-pass",
	}); err != nil {
		t.Fatalf("updateTS: %v", err)
	}

	creds, err := service.NewConnectionService(conns, enc).GetDecryptedCredentialsByLabel(ctx, "test-user-tool", "lighter", "main")
	if err != nil {
		t.Fatalf("service cannot open what the tool wrote: %v", err)
	}
	if creds.APIKey != "new-key" || creds.APISecret != "new-secret" || creds.Passphrase != "new-pass" {
		t.Fatalf("service read %q/%q/%q", creds.APIKey, creds.APISecret, creds.Passphrase)
	}
}

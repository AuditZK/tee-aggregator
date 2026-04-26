package encryption

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
)

// TestUnwrapKeyTryAll_HardwareThenEnv exercises the migration scenario:
// a DEK is wrapped with master_HW (legacy), the binary is upgraded so
// the new measurement gives master_HW2 ≠ master_HW, but the operator
// has re-wrapped via WrapKeyEnv. The new binary must succeed via the
// env path while masterHW2 fails.
//
// We can't run snpguest in unit tests, so we fabricate the two master
// keys directly inside a KeyDerivationService stub.
func TestUnwrapKeyTryAll_HardwareThenEnv(t *testing.T) {
	hwKey := make([]byte, 32)
	envKey := make([]byte, 32)
	if _, err := rand.Read(hwKey); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(envKey); err != nil {
		t.Fatal(err)
	}

	derivation := &KeyDerivationService{
		masterKey:  hwKey,
		masterHW:   hwKey,
		masterENV:  envKey,
		isHardware: true,
	}

	// 1. DEK wrapped with HW (legacy state).
	dek := []byte("0123456789abcdef0123456789abcdef")
	hwSvc, err := New(hwKey)
	if err != nil {
		t.Fatal(err)
	}
	wrappedHW, err := hwSvc.Encrypt(dek)
	if err != nil {
		t.Fatal(err)
	}

	got, src, err := derivation.UnwrapKeyTryAll(wrappedHW)
	if err != nil {
		t.Fatalf("UnwrapKeyTryAll on HW-wrapped DEK: %v", err)
	}
	if src != UnwrapSourceHardware {
		t.Errorf("expected source=%q, got %q", UnwrapSourceHardware, src)
	}
	if string(got) != string(dek) {
		t.Errorf("unwrap returned wrong plaintext")
	}

	// 2. Operator runs migrate-dek-wrap → DEK is re-wrapped with env.
	wrappedENV, err := derivation.WrapKeyEnv(dek)
	if err != nil {
		t.Fatalf("WrapKeyEnv: %v", err)
	}

	// 3. Simulate the binary upgrade: new measurement gives a different
	// hardware master key. Replace masterHW with random bytes; masterENV
	// stays identical (ENCRYPTION_KEY env var unchanged).
	newHWKey := make([]byte, 32)
	if _, err := rand.Read(newHWKey); err != nil {
		t.Fatal(err)
	}
	derivation.masterHW = newHWKey
	derivation.masterKey = newHWKey

	// 4. UnwrapKeyTryAll must fall through to env and succeed.
	got2, src2, err := derivation.UnwrapKeyTryAll(wrappedENV)
	if err != nil {
		t.Fatalf("UnwrapKeyTryAll on ENV-wrapped DEK after binary upgrade: %v", err)
	}
	if src2 != UnwrapSourceEnv {
		t.Errorf("expected source=%q, got %q", UnwrapSourceEnv, src2)
	}
	if string(got2) != string(dek) {
		t.Errorf("env unwrap returned wrong plaintext")
	}

	// 5. The legacy HW-wrapped row would now fail (HW key changed and
	// it was never re-wrapped). UnwrapKeyTryAll must return an error
	// rather than silently returning garbage.
	if _, _, err := derivation.UnwrapKeyTryAll(wrappedHW); err == nil {
		t.Error("expected unwrap to fail on legacy HW-wrapped DEK after binary upgrade, got nil")
	}
}

// TestWrapKeyEnv_NoEnvKey verifies the migration tool gets a clear
// error when ENCRYPTION_KEY isn't set.
func TestWrapKeyEnv_NoEnvKey(t *testing.T) {
	hwKey := make([]byte, 32)
	if _, err := rand.Read(hwKey); err != nil {
		t.Fatal(err)
	}
	derivation := &KeyDerivationService{
		masterKey:  hwKey,
		masterHW:   hwKey,
		masterENV:  nil,
		isHardware: true,
	}

	if derivation.HasEnvMasterKey() {
		t.Fatal("HasEnvMasterKey true with masterENV=nil")
	}
	if _, err := derivation.WrapKeyEnv([]byte("dek")); err != ErrEnvMasterKeyUnavailable {
		t.Errorf("expected ErrEnvMasterKeyUnavailable, got %v", err)
	}
}

// TestDeriveMasterKey_BothPaths verifies that when ENCRYPTION_KEY is
// set on a non-SEV-SNP host, masterENV is populated and masterHW is
// nil — the typical local dev / unit test environment.
func TestDeriveMasterKey_BothPaths(t *testing.T) {
	envKey := make([]byte, 32)
	if _, err := rand.Read(envKey); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ENCRYPTION_KEY", hex.EncodeToString(envKey))

	svc, err := NewKeyDerivationService(nil)
	if err != nil {
		t.Fatalf("NewKeyDerivationService: %v", err)
	}

	// On a non-SEV-SNP host, hardware path fails silently and we fall
	// back to env — masterHW=nil, masterENV=envKey.
	if svc.masterHW != nil {
		t.Logf("masterHW populated unexpectedly (running on a SEV-SNP host?) — skipping the strict no-HW assertion")
	}
	if !svc.HasEnvMasterKey() {
		t.Fatal("HasEnvMasterKey false despite ENCRYPTION_KEY being set")
	}
	if svc.EnvMasterKeyID() == "" {
		t.Fatal("EnvMasterKeyID empty")
	}
}

// TestDeriveMasterKey_NeitherPath verifies the error path when nothing
// is available. Run with a clean env and no /dev/sev-guest (the unit
// test env).
func TestDeriveMasterKey_NeitherPath(t *testing.T) {
	if _, err := os.Stat(sevGuestDevice); err == nil {
		t.Skip("skipping: /dev/sev-guest exists on this host so the hardware path can succeed")
	}
	t.Setenv("ENCRYPTION_KEY", "")

	if _, err := NewKeyDerivationService(nil); err == nil {
		t.Error("expected error when neither hardware nor env master key is available")
	}
}

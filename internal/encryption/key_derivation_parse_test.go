package encryption

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// parseMeasurementFromDisplay sits at 0% baseline. The behaviour matters:
// if the parser misses the multi-line hex dump, we silently fall back to
// the env-var master key in production, which would mean every DEK
// produced by the prior measurement-derived master is suddenly unusable.
func TestParseMeasurementFromDisplay_HappyPath(t *testing.T) {
	output := strings.Join([]string{
		"Version: 2",
		"Guest SVN: 7",
		"Measurement:",
		"12 06 83 61 36 9c f9 17 9b b6 ac 08 57 2b 7e 15",
		"ed 0b c8 ab b6 98 cb 04 d4 f5 84 f7 ff 51 2a 4c",
		"20 81 c1 f5 b1 05 35 1d bd 45 c0 35 a7 d6 a3 a5",
		"Report Data:",
		"00 00 00 00",
	}, "\n")

	got, ver := parseMeasurementFromDisplay(output)
	if ver != "" {
		t.Errorf("platformVersion=%q, expected empty", ver)
	}
	wantHex := "12068361369cf9179bb6ac08572b7e15ed0bc8abb698cb04d4f584f7ff512a4c2081c1f5b105351dbd45c035a7d6a3a5"
	if hex.EncodeToString(got) != wantHex {
		t.Fatalf("got %x, want %s", got, wantHex)
	}
	if len(got) != 48 {
		t.Fatalf("len=%d, want 48 bytes (3 lines × 16)", len(got))
	}
}

func TestParseMeasurementFromDisplay_MissingHeader(t *testing.T) {
	got, _ := parseMeasurementFromDisplay("Version: 2\nGuest SVN: 7\n")
	if got != nil {
		t.Fatalf("got %x, want nil (no Measurement: header)", got)
	}
}

func TestParseMeasurementFromDisplay_StopsOnNonHexLine(t *testing.T) {
	output := strings.Join([]string{
		"Measurement:",
		"12 06 83 61",
		"Reset Trailing: marker",
		"99 99 99 99", // must NOT be appended
	}, "\n")
	got, _ := parseMeasurementFromDisplay(output)
	if hex.EncodeToString(got) != "12068361" {
		t.Fatalf("got %x — must not consume bytes after a non-hex header", got)
	}
}

// Both an underscored and capitalized "Measurement:" header must work,
// since snpguest output capitalisation has changed across versions.
func TestParseMeasurementFromDisplay_CaseInsensitive(t *testing.T) {
	output := strings.Join([]string{
		"MEASUREMENT:",
		"de ad be ef",
	}, "\n")
	got, _ := parseMeasurementFromDisplay(output)
	if hex.EncodeToString(got) != "deadbeef" {
		t.Fatalf("got %x", got)
	}
}

func TestParseMeasurementFromDisplay_GarbledHexFails(t *testing.T) {
	output := "Measurement:\nzz zz zz zz\n"
	got, _ := parseMeasurementFromDisplay(output)
	// hex.DecodeString fails → returns nil per the implementation.
	if got != nil {
		t.Fatalf("got %x, want nil (bad hex)", got)
	}
}

// GetMasterKeyID is the comparable identity stored in
// data_encryption_keys.master_key_id. It MUST be deterministic — the TS
// enclave computes the same hash from the same master key.
func TestGetMasterKeyID_Deterministic(t *testing.T) {
	key := bytes.Repeat([]byte{0xAB}, 32)
	s1 := &KeyDerivationService{masterKey: key}
	s2 := &KeyDerivationService{masterKey: key}
	if s1.GetMasterKeyID() != s2.GetMasterKeyID() {
		t.Fatal("same masterKey must yield same id")
	}
}

func TestGetMasterKeyID_FormatMatchesSpec(t *testing.T) {
	key := bytes.Repeat([]byte{0xAB}, 32)
	s := &KeyDerivationService{masterKey: key}
	got := s.GetMasterKeyID()
	expectedFull := sha256.Sum256(key)
	want := hex.EncodeToString(expectedFull[:8])
	if got != want {
		t.Fatalf("got %s, want %s (first 8 bytes of sha256 hex)", got, want)
	}
	if len(got) != 16 {
		t.Fatalf("len(id)=%d, want 16 hex chars", len(got))
	}
}

// IsHardwareKey is one of the booleans passed to log fields and decision
// branches all over the codebase — worth a tiny sanity test.
func TestIsHardwareKey(t *testing.T) {
	yes := &KeyDerivationService{isHardware: true}
	no := &KeyDerivationService{isHardware: false}
	if !yes.IsHardwareKey() {
		t.Error("isHardware=true → IsHardwareKey() must be true")
	}
	if no.IsHardwareKey() {
		t.Error("isHardware=false → IsHardwareKey() must be false")
	}
}

// ExportRawMasterKeyForLegacyMigration returns a copy — verify we cannot
// mutate the underlying master key via the returned slice.
func TestExportRawMasterKeyForLegacyMigration_ReturnsCopy(t *testing.T) {
	master := bytes.Repeat([]byte{0xAB}, 32)
	s := &KeyDerivationService{masterKey: master}

	got, err := ExportRawMasterKeyForLegacyMigration(s)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if !bytes.Equal(got, master) {
		t.Fatal("exported bytes must equal master")
	}

	// Mutate the returned copy — must not affect the service.
	got[0] = 0xFF
	if s.masterKey[0] == 0xFF {
		t.Fatal("returned slice was not a copy")
	}
}

func TestExportRawMasterKeyForLegacyMigration_NilSafe(t *testing.T) {
	if _, err := ExportRawMasterKeyForLegacyMigration(nil); err == nil {
		t.Fatal("nil receiver should return an error")
	}
	if _, err := ExportRawMasterKeyForLegacyMigration(&KeyDerivationService{}); err == nil {
		t.Fatal("empty masterKey should return an error")
	}
}

// WrapKey + UnwrapKey round-trip via the AES-GCM path. Tests that both
// sides agree on the layout (base64 ciphertext / iv / auth tag).
func TestWrapUnwrapKey_RoundTrip(t *testing.T) {
	master := bytes.Repeat([]byte{0xCD}, 32)
	s := &KeyDerivationService{masterKey: master}

	dek := bytes.Repeat([]byte{0xEF}, 32)
	wrapped, err := s.WrapKey(dek)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	if wrapped.Ciphertext == "" || wrapped.IV == "" || wrapped.AuthTag == "" {
		t.Fatal("wrapped data missing one of ciphertext/iv/authTag")
	}

	got, err := s.UnwrapKey(wrapped)
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatalf("got %x, want %x", got, dek)
	}
}

func TestWrapUnwrapKey_TamperedAuthTagFailsClosed(t *testing.T) {
	master := bytes.Repeat([]byte{0xCD}, 32)
	s := &KeyDerivationService{masterKey: master}

	dek := []byte("super-secret-dek-bytes-32bytes!")
	wrapped, _ := s.WrapKey(dek)
	wrapped.AuthTag = "AAAAAAAAAAAAAAAAAAAAAA==" // 16 bytes b64 garbage
	if _, err := s.UnwrapKey(wrapped); err == nil {
		t.Fatal("tampered auth tag must fail unwrap")
	}
}

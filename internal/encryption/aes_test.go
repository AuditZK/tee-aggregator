package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	svc, err := New(key)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	plaintext := "my_api_secret_key_123"

	encrypted, err := svc.EncryptString(plaintext)
	if err != nil {
		t.Fatalf("Encrypt() error = %v", err)
	}

	if encrypted.Ciphertext == plaintext {
		t.Error("ciphertext should not equal plaintext")
	}

	decrypted, err := svc.DecryptString(encrypted)
	if err != nil {
		t.Fatalf("Decrypt() error = %v", err)
	}

	if decrypted != plaintext {
		t.Errorf("Decrypt() = %v, want %v", decrypted, plaintext)
	}
}

func TestDecryptWithWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	svc1, _ := New(key1)
	svc2, _ := New(key2)

	encrypted, _ := svc1.EncryptString("secret")

	_, err := svc2.DecryptString(encrypted)
	if err == nil {
		t.Error("expected decryption to fail with wrong key")
	}
}

func TestInvalidKeyLength(t *testing.T) {
	_, err := New([]byte("short"))
	if err == nil {
		t.Error("expected error for short key")
	}
}

func TestDecryptTSFormat(t *testing.T) {
	// Simulate TS encryption: iv(16 bytes) + tag(16 bytes) + ciphertext, all hex
	// We encrypt with the TS method (GCM with 16-byte IV) then decrypt with DecryptTSFormat
	key := make([]byte, 32)
	rand.Read(key)

	svc, err := New(key)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	plaintext := "my_super_secret_api_key"

	// Manually encrypt in TS format (16-byte IV, GCM)
	tsEncrypted := encryptTSFormat(t, key, plaintext)

	// Decrypt with DecryptTSFormat
	decrypted, err := svc.DecryptTSString(tsEncrypted)
	if err != nil {
		t.Fatalf("DecryptTSString() error = %v", err)
	}

	if decrypted != plaintext {
		t.Errorf("DecryptTSString() = %q, want %q", decrypted, plaintext)
	}
}

func TestDecryptTSFormat_WrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	svc2, _ := New(key2)

	tsEncrypted := encryptTSFormat(t, key1, "secret")
	_, err := svc2.DecryptTSString(tsEncrypted)
	if err == nil {
		t.Error("expected decryption to fail with wrong key")
	}
}

func TestDecryptTSFormat_TooShort(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	svc, _ := New(key)

	_, err := svc.DecryptTSString("abcdef")
	if err == nil {
		t.Error("expected error for data too short")
	}
}

// SEC-009: EncryptWithAAD / DecryptWithAAD round-trip with matching AAD.
func TestAAD_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	svc, _ := New(key)

	plaintext := []byte("my_api_secret_key_123")
	aad := ConnectionFieldAAD("user_abc1234567890", "conn_42", "api_key")

	encrypted, err := svc.EncryptWithAAD(plaintext, aad)
	if err != nil {
		t.Fatalf("EncryptWithAAD: %v", err)
	}

	got, err := svc.DecryptWithAAD(encrypted, aad)
	if err != nil {
		t.Fatalf("DecryptWithAAD: %v", err)
	}
	if string(got) != string(plaintext) {
		t.Fatalf("round-trip mismatch: got=%q want=%q", got, plaintext)
	}
}

// SEC-009: ciphertext encrypted for connA must NOT decrypt under connB's AAD.
// This proves the binding works: a DB-level row swap is refused by GCM.
func TestAAD_MismatchRefused(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	svc, _ := New(key)

	plaintext := []byte("my_api_secret_key_123")
	aadA := ConnectionFieldAAD("user_abc1234567890", "connA", "api_key")
	aadB := ConnectionFieldAAD("user_abc1234567890", "connB", "api_key")

	encrypted, err := svc.EncryptWithAAD(plaintext, aadA)
	if err != nil {
		t.Fatalf("EncryptWithAAD: %v", err)
	}

	if _, err := svc.DecryptWithAAD(encrypted, aadB); err == nil {
		t.Fatal("decrypt should fail when AAD does not match")
	}
	// Field swap within the same connection must also fail.
	aadField := ConnectionFieldAAD("user_abc1234567890", "connA", "api_secret")
	if _, err := svc.DecryptWithAAD(encrypted, aadField); err == nil {
		t.Fatal("decrypt should fail when field AAD does not match")
	}
}

// SEC-009: without AAD, legacy ciphertexts still round-trip — backward compat.
func TestAAD_LegacyNoAAD(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	svc, _ := New(key)

	encrypted, err := svc.Encrypt([]byte("legacy-secret"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	got, err := svc.Decrypt(encrypted)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(got) != "legacy-secret" {
		t.Fatalf("legacy round-trip broken: got %q", got)
	}

	// Mixing legacy-encrypted and AAD-decrypt must fail loudly.
	if _, err := svc.DecryptWithAAD(encrypted, []byte("some-aad")); err == nil {
		t.Fatal("legacy ciphertext must not decrypt under non-empty AAD")
	}
}

// TestEncryptTSString_RoundTrip pins the format that the TS schema reader
// expects: EncryptTSString must produce output that DecryptTSString can read
// back. This is the path the connection service takes when writing into the
// TS/Prisma exchange_connections table.
func TestEncryptTSString_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	svc, err := New(key)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for _, plaintext := range []string{
		"12345678",                  // typical MT5 login length
		"my_super_long_api_key_xyz", // longer payload
		"x",                         // single byte
	} {
		t.Run(fmt.Sprintf("len=%d", len(plaintext)), func(t *testing.T) {
			ts, err := svc.EncryptTSString(plaintext)
			if err != nil {
				t.Fatalf("EncryptTSString: %v", err)
			}
			got, err := svc.DecryptTSString(ts)
			if err != nil {
				t.Fatalf("DecryptTSString: %v", err)
			}
			if got != plaintext {
				t.Fatalf("round-trip mismatch: got=%q want=%q", got, plaintext)
			}
		})
	}
}

// TestGoFormatNotInterchangeableWithTSFormat is a regression test for the
// IV-size bug fixed on this branch: rows created via EncryptString
// (12-byte nonce GCM) and naively concatenated as iv||tag||ciphertext hex
// are NOT readable by DecryptTSString, which reads 16 bytes as IV. The two
// formats are not interchangeable. The connection service must call
// EncryptTSString when targeting the TS schema; calling EncryptString and
// shoving the parts into a single hex column produces unreadable rows
// (auth tag check fails because the IV is misaligned by 4 bytes).
func TestGoFormatNotInterchangeableWithTSFormat(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	svc, err := New(key)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	plaintext := "12345678"

	enc, err := svc.EncryptString(plaintext)
	if err != nil {
		t.Fatalf("EncryptString: %v", err)
	}

	// Naive repack: hex(iv12) + hex(tag16) + hex(ct). This is what the
	// removed repackToTSFormat helper used to produce. DecryptTSString
	// reads the first 32 hex chars as IV, eating 4 bytes of the tag, so
	// the auth-tag check must fail.
	ivBytes, err := base64.StdEncoding.DecodeString(enc.IV)
	if err != nil {
		t.Fatalf("decode iv: %v", err)
	}
	tagBytes, err := base64.StdEncoding.DecodeString(enc.AuthTag)
	if err != nil {
		t.Fatalf("decode tag: %v", err)
	}
	ctBytes, err := base64.StdEncoding.DecodeString(enc.Ciphertext)
	if err != nil {
		t.Fatalf("decode ciphertext: %v", err)
	}
	if len(ivBytes) != 12 {
		t.Fatalf("EncryptString IV must be 12 bytes (stdlib GCM), got %d", len(ivBytes))
	}
	misformatted := fmt.Sprintf("%x%x%x", ivBytes, tagBytes, ctBytes)

	if _, err := svc.DecryptTSString(misformatted); err == nil {
		t.Fatal("DecryptTSString should reject Go-format payload with 12-byte IV — the formats are not interchangeable")
	}
}

// encryptTSFormat encrypts like the TS enclave: AES-256-GCM with 16-byte IV, hex output.
func encryptTSFormat(t *testing.T, key []byte, plaintext string) string {
	t.Helper()

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("create cipher: %v", err)
	}

	gcm, err := cipher.NewGCMWithNonceSize(block, 16)
	if err != nil {
		t.Fatalf("create gcm: %v", err)
	}

	iv := make([]byte, 16)
	rand.Read(iv)

	sealed := gcm.Seal(nil, iv, []byte(plaintext), nil)

	// GCM appends tag at the end
	tagSize := gcm.Overhead()
	ciphertext := sealed[:len(sealed)-tagSize]
	tag := sealed[len(sealed)-tagSize:]

	// TS format: iv_hex + tag_hex + ciphertext_hex
	return fmt.Sprintf("%x%x%x", iv, tag, ciphertext)
}

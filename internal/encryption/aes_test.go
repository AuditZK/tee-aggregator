package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
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

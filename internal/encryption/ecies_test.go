package encryption

import (
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/pem"
	"testing"

	"crypto/aes"
	"crypto/cipher"
	"golang.org/x/crypto/hkdf"
)

// eciesEncrypt is a test helper that encrypts with ECIES for round-trip testing.
func eciesEncrypt(serverPubKey *ecdh.PublicKey, plaintext []byte) (ephPubKeyBytes, iv, ciphertext []byte, err error) {
	// Generate ephemeral key pair
	curve := ecdh.P256()
	ephPriv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	ephPubKeyBytes = ephPriv.PublicKey().Bytes()

	// ECDH
	shared, err := ephPriv.ECDH(serverPubKey)
	if err != nil {
		return nil, nil, nil, err
	}

	// HKDF
	hkdfReader := hkdf.New(sha256.New, shared, nil, []byte(eciesInfoString))
	aesKey := make([]byte, 32)
	hkdfReader.Read(aesKey)

	// AES-GCM encrypt
	block, _ := aes.NewCipher(aesKey)
	gcm, _ := cipher.NewGCM(block)

	iv = make([]byte, gcm.NonceSize())
	rand.Read(iv)

	ciphertext = gcm.Seal(nil, iv, plaintext, nil)
	return ephPubKeyBytes, iv, ciphertext, nil
}

func TestECIESRoundTrip(t *testing.T) {
	svc, err := NewECIES()
	if err != nil {
		t.Fatalf("NewECIES() error = %v", err)
	}

	plaintext := []byte(`{"api_key":"test_key","api_secret":"test_secret"}`)

	ephPub, iv, ct, err := eciesEncrypt(svc.publicKey, plaintext)
	if err != nil {
		t.Fatalf("eciesEncrypt error = %v", err)
	}

	decrypted, err := svc.Decrypt(ephPub, iv, ct)
	if err != nil {
		t.Fatalf("Decrypt() error = %v", err)
	}

	if string(decrypted) != string(plaintext) {
		t.Errorf("Decrypt() = %s, want %s", decrypted, plaintext)
	}
}

func TestECIESWrongKey(t *testing.T) {
	svc1, _ := NewECIES()
	svc2, _ := NewECIES()

	plaintext := []byte("secret data")
	ephPub, iv, ct, _ := eciesEncrypt(svc1.publicKey, plaintext)

	// Decrypting with svc2's key should fail
	_, err := svc2.Decrypt(ephPub, iv, ct)
	if err == nil {
		t.Error("expected decryption to fail with wrong key")
	}
}

func TestECIESPEMEphemeralKey(t *testing.T) {
	svc, err := NewECIES()
	if err != nil {
		t.Fatalf("NewECIES() error = %v", err)
	}

	plaintext := []byte(`{"api_key":"test"}`)

	ephPub, iv, ct, err := eciesEncrypt(svc.publicKey, plaintext)
	if err != nil {
		t.Fatalf("eciesEncrypt error = %v", err)
	}

	// Wrap the ephemeral public key in PEM format (like TS clients do)
	pemKey := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: ephPub,
	})

	// Decrypt with PEM-wrapped key should work
	decrypted, err := svc.Decrypt(pemKey, iv, ct)
	if err != nil {
		t.Fatalf("Decrypt() with PEM key error = %v", err)
	}

	if string(decrypted) != string(plaintext) {
		t.Errorf("Decrypt() = %s, want %s", decrypted, plaintext)
	}
}

func TestPublicKeyFormats(t *testing.T) {
	svc, _ := NewECIES()

	pemStr := svc.PublicKeyPEM()
	if pemStr == "" {
		t.Error("PublicKeyPEM() should not be empty")
	}

	hexStr := svc.PublicKeyHex()
	if hexStr == "" {
		t.Error("PublicKeyHex() should not be empty")
	}

	b64Str := svc.PublicKeyBase64()
	if b64Str == "" {
		t.Error("PublicKeyBase64() should not be empty")
	}
}

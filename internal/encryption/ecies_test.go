package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"testing"

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

	pemStr, err := svc.PublicKeyPEM()
	if err != nil {
		t.Fatalf("PublicKeyPEM() error = %v", err)
	}
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

// The published key MUST be a PEM-wrapped SubjectPublicKeyInfo: a bare EC
// point in a "PUBLIC KEY" block is rejected by every SPKI parser the /setup
// flow relies on (WebCrypto, Python cryptography, .NET) — SEC-ECIES-001.
func TestPublicKeyPEM_IsParseableSPKI(t *testing.T) {
	svc, _ := NewECIES()

	pemStr, err := svc.PublicKeyPEM()
	if err != nil {
		t.Fatalf("PublicKeyPEM() error = %v", err)
	}
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil || block.Type != "PUBLIC KEY" {
		t.Fatalf("PublicKeyPEM() did not yield a PUBLIC KEY block: %+v", block)
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("published key is not valid SPKI (the /setup bug): %v", err)
	}
	ec, ok := pub.(*ecdsa.PublicKey)
	if !ok {
		t.Fatalf("ParsePKIXPublicKey returned %T, want *ecdsa.PublicKey", pub)
	}
	if ec.Curve != elliptic.P256() {
		t.Errorf("published key curve = %v, want P-256", ec.Curve)
	}
}

// End-to-end: a standard SPKI client (WebCrypto / Python) imports the
// published key, encrypts, and the enclave must be able to Decrypt.
func TestECIES_EncryptWithPublishedSPKIKey(t *testing.T) {
	svc, _ := NewECIES()

	pemStr, err := svc.PublicKeyPEM()
	if err != nil {
		t.Fatalf("PublicKeyPEM() error = %v", err)
	}
	block, _ := pem.Decode([]byte(pemStr))
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("SPKI parse (client side): %v", err)
	}
	serverKey, err := pub.(*ecdsa.PublicKey).ECDH()
	if err != nil {
		t.Fatalf("ECDH conversion (client side): %v", err)
	}

	plaintext := []byte(`{"api_key":"k","api_secret":"s"}`)
	ephPub, iv, ct, err := eciesEncrypt(serverKey, plaintext)
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

// ParseEphemeralPublicKey must normalise all three shapes a client can send
// — PEM-wrapped SPKI, raw SPKI DER, raw uncompressed point — to the raw point.
func TestParseEphemeralPublicKey_Formats(t *testing.T) {
	priv, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	rawPoint := priv.PublicKey().Bytes()
	spkiDER, err := x509.MarshalPKIXPublicKey(priv.PublicKey())
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	spkiPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: spkiDER})

	for _, tc := range []struct {
		name string
		in   []byte
	}{
		{"raw uncompressed point", rawPoint},
		{"raw SPKI DER", spkiDER},
		{"PEM-wrapped SPKI", spkiPEM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseEphemeralPublicKey(tc.in)
			if err != nil {
				t.Fatalf("ParseEphemeralPublicKey: %v", err)
			}
			if !bytes.Equal(got, rawPoint) {
				t.Errorf("normalised key = %x, want raw point %x", got, rawPoint)
			}
		})
	}
}

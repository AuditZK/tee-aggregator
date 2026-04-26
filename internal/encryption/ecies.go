package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"

	"golang.org/x/crypto/hkdf"
)

const eciesInfoString = "enclave-e2e-encryption"

// ECIESService handles ECIES (Elliptic Curve Integrated Encryption Scheme)
// for end-to-end credential encryption between client and enclave.
type ECIESService struct {
	privateKey *ecdh.PrivateKey
	publicKey  *ecdh.PublicKey
}

// NewECIES generates a new ECDH P-256 key pair for E2E encryption.
func NewECIES() (*ECIESService, error) {
	curve := ecdh.P256()
	privateKey, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ecdh key: %w", err)
	}

	return &ECIESService{
		privateKey: privateKey,
		publicKey:  privateKey.PublicKey(),
	}, nil
}

// PublicKeyPEM returns the public key in PEM format.
func (e *ECIESService) PublicKeyPEM() string {
	derBytes := e.publicKey.Bytes()
	block := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: derBytes,
	}
	return string(pem.EncodeToMemory(block))
}

// PublicKeyHex returns the public key as a hex string.
func (e *ECIESService) PublicKeyHex() string {
	return hex.EncodeToString(e.publicKey.Bytes())
}

// PublicKeyBase64 returns the public key as a base64 string.
func (e *ECIESService) PublicKeyBase64() string {
	return base64.StdEncoding.EncodeToString(e.publicKey.Bytes())
}

// ParseEphemeralPublicKey parses an ephemeral public key from either raw bytes
// (uncompressed P-256 point, 65 bytes) or PEM-encoded format (as sent by TS clients).
func ParseEphemeralPublicKey(data []byte) ([]byte, error) {
	// Try PEM decode first (TS clients send PEM strings)
	block, _ := pem.Decode(data)
	if block != nil {
		return block.Bytes, nil
	}
	// Already raw bytes
	return data, nil
}

// Decrypt decrypts data encrypted with ECIES (ECDH + HKDF + AES-256-GCM).
// ephemeralPubKeyBytes: the client's ephemeral public key (raw bytes or PEM)
// iv: initialization vector (12 bytes)
// ciphertext: encrypted data (includes GCM auth tag appended)
func (e *ECIESService) Decrypt(ephemeralPubKeyBytes, iv, ciphertext []byte) ([]byte, error) {
	// Parse ephemeral public key (supports both raw and PEM formats)
	rawKey, err := ParseEphemeralPublicKey(ephemeralPubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("parse ephemeral public key format: %w", err)
	}

	curve := ecdh.P256()
	ephemeralPubKey, err := curve.NewPublicKey(rawKey)
	if err != nil {
		return nil, fmt.Errorf("parse ephemeral public key: %w", err)
	}

	// ECDH shared secret
	sharedSecret, err := e.privateKey.ECDH(ephemeralPubKey)
	if err != nil {
		return nil, fmt.Errorf("ecdh: %w", err)
	}

	// Derive AES key using HKDF-SHA256
	hkdfReader := hkdf.New(sha256.New, sharedSecret, nil, []byte(eciesInfoString))
	aesKey := make([]byte, 32) // AES-256
	if _, err := hkdfReader.Read(aesKey); err != nil {
		return nil, fmt.Errorf("hkdf derive: %w", err)
	}

	// AES-256-GCM decrypt
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, fmt.Errorf("aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm: %w", err)
	}

	plaintext, err := gcm.Open(nil, iv, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("gcm decrypt: %w", err)
	}

	return plaintext, nil
}

// Cleanup drops the reference to the ECDH private key so the Go runtime can
// eventually reclaim it. SEC-001: this is NOT a memory wipe — Go's runtime
// owns the underlying scalar and there is no public API on `ecdh.PrivateKey`
// to zero it. The bytes persist until garbage collection runs and reuses
// the page, which is non-deterministic.
//
// The enclave's broader threat model relies on AMD SEV-SNP memory
// encryption for at-rest protection rather than on Go-level wiping; see
// `internal/security/memory_linux.go` for the process-level controls
// (RLIMIT_CORE=0, ptrace_scope check) that actually matter for forensics.
func (e *ECIESService) Cleanup() {
	e.privateKey = nil
}

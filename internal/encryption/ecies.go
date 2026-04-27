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

// PrivateKey returns the underlying ECDH private key. Exposed for the
// B2 handoff client which needs to decrypt the handoff response with
// the same private key whose public part is published in the
// attestation report. Caller MUST keep the returned reference inside
// the TEE process boundary — never log it, never serialise it.
func (e *ECIESService) PrivateKey() *ecdh.PrivateKey {
	return e.privateKey
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

// EncryptToPubkey encrypts plaintext to a recipient's ECDH P-256 public
// key, returning (ephemeral_pubkey_bytes, iv, ciphertext). Mirror of
// Decrypt — same HKDF / AES-GCM construction — used by the B2 handoff
// server to wrap the master key for the successor enclave.
//
// recipientPubKeyBytes accepts either raw uncompressed P-256 (65 bytes)
// or a PEM-encoded SubjectPublicKeyInfo (the same shape the enclave
// publishes in /api/v1/attestation.e2eEncryption.publicKey).
//
// This is a free function (not a method) so it does not require an
// ECIESService instance — the caller doesn't need a long-lived
// recipient-side state, just the recipient's pubkey.
func EncryptToPubkey(recipientPubKeyBytes, plaintext []byte) (ephPub, iv, ciphertext []byte, err error) {
	rawKey, err := ParseEphemeralPublicKey(recipientPubKeyBytes)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse recipient pubkey format: %w", err)
	}

	curve := ecdh.P256()
	recipientPubKey, err := curve.NewPublicKey(rawKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse recipient pubkey: %w", err)
	}

	// Generate the ephemeral keypair on the sender side.
	ephPriv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate ephemeral keypair: %w", err)
	}

	sharedSecret, err := ephPriv.ECDH(recipientPubKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("ecdh: %w", err)
	}

	// Same HKDF parameters as Decrypt — both sides must agree exactly.
	hkdfReader := hkdf.New(sha256.New, sharedSecret, nil, []byte(eciesInfoString))
	aesKey := make([]byte, 32)
	if _, err := hkdfReader.Read(aesKey); err != nil {
		return nil, nil, nil, fmt.Errorf("hkdf derive: %w", err)
	}

	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gcm: %w", err)
	}

	iv = make([]byte, gcm.NonceSize())
	if _, err := rand.Read(iv); err != nil {
		return nil, nil, nil, fmt.Errorf("generate iv: %w", err)
	}
	ct := gcm.Seal(nil, iv, plaintext, nil)

	return ephPriv.PublicKey().Bytes(), iv, ct, nil
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

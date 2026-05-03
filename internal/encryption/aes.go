package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
)

var ErrDecryptionFailed = errors.New("decryption failed: authentication error")

// errCreateCipherFmt is the wrap pattern used when aes.NewCipher fails.
// Extracted to satisfy go:S1192 — duplicated across the four code paths
// that build a cipher (Encrypt/Decrypt × stdlib + TS-format).
const errCreateCipherFmt = "create cipher: %w"

// Service handles AES-256-GCM encryption/decryption.
//
// PERF-003: the cipher.Block and AEAD wrappers are built once in New and
// reused across calls. cipher.AEAD is documented thread-safe for Seal/Open
// (see crypto/cipher docs), so no locking is required.
type Service struct {
	key    []byte      // 32 bytes for AES-256 (kept for legacy callers reading s.key)
	aead   cipher.AEAD // GCM with the standard 12-byte nonce (Encrypt/Decrypt)
	aead16 cipher.AEAD // GCM with 16-byte nonce (TS-format compatibility)
}

// New creates an encryption service with the given key.
func New(key []byte) (*Service, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf(errCreateCipherFmt, err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}
	aead16, err := cipher.NewGCMWithNonceSize(block, tsIVLen)
	if err != nil {
		return nil, fmt.Errorf("create gcm with nonce size %d: %w", tsIVLen, err)
	}
	return &Service{key: key, aead: aead, aead16: aead16}, nil
}

// EncryptedData holds the encrypted payload and metadata
type EncryptedData struct {
	Ciphertext string `json:"ciphertext"` // base64
	IV         string `json:"iv"`         // base64
	AuthTag    string `json:"auth_tag"`   // base64
}

// Encrypt encrypts plaintext using AES-256-GCM (no AAD). For new
// integrations that want ciphertext bound to a specific
// (user, connection, field) tuple, use EncryptWithAAD (SEC-009).
func (s *Service) Encrypt(plaintext []byte) (*EncryptedData, error) {
	return s.EncryptWithAAD(plaintext, nil)
}

// EncryptWithAAD encrypts plaintext with optional Additional Authenticated
// Data (SEC-009). The AAD is bound into the auth tag but not encrypted — the
// same bytes must be supplied to DecryptWithAAD or Open returns
// ErrDecryptionFailed. Typical usage: aad = []byte(userUID + "|" +
// connectionID + "|api_key"), which prevents an attacker with DB-write
// access from swapping ciphertext rows between users/fields.
func (s *Service) EncryptWithAAD(plaintext, aad []byte) (*EncryptedData, error) {
	// PERF-003: s.aead is built once in New(); reuse it.
	gcm := s.aead

	iv := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("generate iv: %w", err)
	}

	sealed := gcm.Seal(nil, iv, plaintext, aad)

	// GCM appends auth tag at the end (16 bytes)
	tagSize := gcm.Overhead()
	ciphertext := sealed[:len(sealed)-tagSize]
	authTag := sealed[len(sealed)-tagSize:]

	return &EncryptedData{
		Ciphertext: base64.StdEncoding.EncodeToString(ciphertext),
		IV:         base64.StdEncoding.EncodeToString(iv),
		AuthTag:    base64.StdEncoding.EncodeToString(authTag),
	}, nil
}

// Decrypt decrypts ciphertext using AES-256-GCM (no AAD).
// For AAD-bound ciphertexts, use DecryptWithAAD (SEC-009).
func (s *Service) Decrypt(data *EncryptedData) ([]byte, error) {
	return s.DecryptWithAAD(data, nil)
}

// DecryptWithAAD decrypts ciphertext produced by EncryptWithAAD. The AAD
// passed here must byte-exactly match the one used at encryption time, or
// the auth-tag check fails and ErrDecryptionFailed is returned (SEC-009).
func (s *Service) DecryptWithAAD(data *EncryptedData, aad []byte) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(data.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}

	iv, err := base64.StdEncoding.DecodeString(data.IV)
	if err != nil {
		return nil, fmt.Errorf("decode iv: %w", err)
	}

	authTag, err := base64.StdEncoding.DecodeString(data.AuthTag)
	if err != nil {
		return nil, fmt.Errorf("decode auth tag: %w", err)
	}

	// PERF-003: s.aead is built once in New(); reuse it.
	gcm := s.aead

	// Reconstruct sealed data (ciphertext + auth tag). Use a fresh slice to
	// avoid mutating the decoded ciphertext.
	sealed := make([]byte, 0, len(ciphertext)+len(authTag))
	sealed = append(sealed, ciphertext...)
	sealed = append(sealed, authTag...)

	plaintext, err := gcm.Open(nil, iv, sealed, aad)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return plaintext, nil
}

// ConnectionFieldAAD builds a canonical AAD for per-field credential
// encryption (SEC-009). Callers should use this helper so the encrypt and
// decrypt sides cannot drift on byte layout. The field argument is a short
// constant ("api_key", "api_secret", "passphrase") that prevents an attacker
// from swapping the three encrypted columns within the same row.
func ConnectionFieldAAD(userUID, connectionID, field string) []byte {
	if userUID == "" || connectionID == "" || field == "" {
		return nil
	}
	return []byte(userUID + "|" + connectionID + "|" + field)
}

// DecryptTSFormat decrypts data stored in the TypeScript enclave format.
// TS format: single hex string = iv(16 bytes hex) + authTag(16 bytes hex) + ciphertext(hex)
// This allows the Go enclave to read credentials encrypted by the TS enclave
// when deployed on the same hardware (same ENCRYPTION_KEY / DEK).
func (s *Service) DecryptTSFormat(hexData string) ([]byte, error) {
	if len(hexData) < (tsIVLen+tsTagLen)*2 {
		return nil, fmt.Errorf("ts encrypted data too short: %d chars", len(hexData))
	}

	// Parse hex-encoded components using stdlib hex.DecodeString
	ivHex := hexData[:tsIVLen*2]                        // first 32 hex chars = 16 bytes IV
	tagHex := hexData[tsIVLen*2 : (tsIVLen+tsTagLen)*2] // next 32 hex chars = 16 bytes tag
	ciphertextHex := hexData[(tsIVLen+tsTagLen)*2:]     // remainder = ciphertext

	iv, err := hex.DecodeString(ivHex)
	if err != nil {
		return nil, fmt.Errorf("decode ts iv: %w", err)
	}

	authTag, err := hex.DecodeString(tagHex)
	if err != nil {
		return nil, fmt.Errorf("decode ts auth tag: %w", err)
	}

	ciphertext, err := hex.DecodeString(ciphertextHex)
	if err != nil {
		return nil, fmt.Errorf("decode ts ciphertext: %w", err)
	}

	// PERF-003: s.aead16 is the cached 16-byte-nonce GCM (TS-format).
	gcm := s.aead16

	// Reconstruct sealed data (ciphertext + auth tag)
	sealed := append(ciphertext, authTag...)

	plaintext, err := gcm.Open(nil, iv, sealed, nil)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return plaintext, nil
}

// DecryptTSString decrypts a TS-format hex string and returns a string.
func (s *Service) DecryptTSString(hexData string) (string, error) {
	plaintext, err := s.DecryptTSFormat(hexData)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// EncryptTSFormat produces the single-column hex string used by the TS enclave
// schema for encrypted credentials: hex(iv_16 || tag_16 || ciphertext).
// The GCM nonce is 16 bytes to match the TS convention (not the stdlib 12).
func (s *Service) EncryptTSFormat(plaintext []byte) (string, error) {
	// PERF-003: s.aead16 is the cached 16-byte-nonce GCM (TS-format).
	gcm := s.aead16

	iv := make([]byte, tsIVLen)
	if _, err := rand.Read(iv); err != nil {
		return "", fmt.Errorf("generate iv: %w", err)
	}

	sealed := gcm.Seal(nil, iv, plaintext, nil)
	tagSize := gcm.Overhead()
	ciphertext := sealed[:len(sealed)-tagSize]
	authTag := sealed[len(sealed)-tagSize:]

	// TS layout: iv(16) + tag(16) + ciphertext, all hex-encoded.
	return hex.EncodeToString(iv) + hex.EncodeToString(authTag) + hex.EncodeToString(ciphertext), nil
}

// EncryptTSString is a string-typed convenience wrapper around EncryptTSFormat.
func (s *Service) EncryptTSString(plaintext string) (string, error) {
	return s.EncryptTSFormat([]byte(plaintext))
}

const (
	tsIVLen  = 16 // TS uses 16-byte IV (non-standard but valid for GCM)
	tsTagLen = 16 // 16-byte auth tag (standard)
)

// EncryptString is a convenience method for encrypting strings
func (s *Service) EncryptString(plaintext string) (*EncryptedData, error) {
	return s.Encrypt([]byte(plaintext))
}

// DecryptString is a convenience method for decrypting to string
func (s *Service) DecryptString(data *EncryptedData) (string, error) {
	plaintext, err := s.Decrypt(data)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

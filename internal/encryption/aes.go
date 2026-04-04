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

// Service handles AES-256-GCM encryption/decryption
type Service struct {
	key []byte // 32 bytes for AES-256
}

// New creates an encryption service with the given key
func New(key []byte) (*Service, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes, got %d", len(key))
	}
	return &Service{key: key}, nil
}

// EncryptedData holds the encrypted payload and metadata
type EncryptedData struct {
	Ciphertext string `json:"ciphertext"` // base64
	IV         string `json:"iv"`         // base64
	AuthTag    string `json:"auth_tag"`   // base64
}

// Encrypt encrypts plaintext using AES-256-GCM
func (s *Service) Encrypt(plaintext []byte) (*EncryptedData, error) {
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}

	iv := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("generate iv: %w", err)
	}

	sealed := gcm.Seal(nil, iv, plaintext, nil)

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

// Decrypt decrypts ciphertext using AES-256-GCM
func (s *Service) Decrypt(data *EncryptedData) ([]byte, error) {
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

	block, err := aes.NewCipher(s.key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}

	// Reconstruct sealed data (ciphertext + auth tag)
	sealed := append(ciphertext, authTag...)

	plaintext, err := gcm.Open(nil, iv, sealed, nil)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return plaintext, nil
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
	ivHex := hexData[:tsIVLen*2]                           // first 32 hex chars = 16 bytes IV
	tagHex := hexData[tsIVLen*2 : (tsIVLen+tsTagLen)*2]    // next 32 hex chars = 16 bytes tag
	ciphertextHex := hexData[(tsIVLen+tsTagLen)*2:]        // remainder = ciphertext

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

	// AES-256-GCM with 16-byte IV (TS uses 16, not standard 12)
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	// NewGCMWithNonceSize allows non-standard IV sizes (16 bytes instead of 12)
	gcm, err := cipher.NewGCMWithNonceSize(block, tsIVLen)
	if err != nil {
		return nil, fmt.Errorf("create gcm with nonce size %d: %w", tsIVLen, err)
	}

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

const (
	tsIVLen  = 16 // TS uses 16-byte IV (non-standard but valid for GCM)
	tsTagLen = 16 // 16-byte auth tag (standard)
)

func hexDecode(s string) ([]byte, error) {
	b := make([]byte, len(s)/2)
	for i := 0; i < len(b); i++ {
		hi := hexVal(s[i*2])
		lo := hexVal(s[i*2+1])
		if hi < 0 || lo < 0 {
			return nil, fmt.Errorf("invalid hex char at position %d", i*2)
		}
		b[i] = byte(hi<<4 | lo)
	}
	return b, nil
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c - 'a' + 10)
	case c >= 'A' && c <= 'F':
		return int(c - 'A' + 10)
	default:
		return -1
	}
}

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

package encryption

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"

	"golang.org/x/crypto/hkdf"
)

const (
	sevGuestDevice = "/dev/sev-guest"
	masterKeySize  = 32
)

// KeyDerivationService derives master keys from hardware measurements
type KeyDerivationService struct {
	masterKey  []byte
	isHardware bool
}

// NewKeyDerivationService creates a new key derivation service.
// Falls back to ENCRYPTION_KEY env var when SEV-SNP hardware is unavailable (TS parity).
func NewKeyDerivationService() (*KeyDerivationService, error) {
	svc := &KeyDerivationService{}
	if err := svc.deriveMasterKey(); err != nil {
		return nil, err
	}
	return svc, nil
}

// deriveMasterKey derives the master key from SEV-SNP or from ENCRYPTION_KEY env var.
func (s *KeyDerivationService) deriveMasterKey() error {
	// Try to read from SEV-SNP hardware
	measurement, err := s.getSEVMeasurement()
	if err != nil {
		// Fallback: derive from ENCRYPTION_KEY env var (TS parity)
		envKey := os.Getenv("ENCRYPTION_KEY")
		if envKey != "" {
			keyBytes, decErr := hexDecode(envKey)
			if decErr == nil && len(keyBytes) == 32 {
				// Derive master key from env key using HKDF (same as TS key-derivation.service.ts)
				hash := sha256.New
				reader := hkdf.New(hash, keyBytes, nil, []byte("enclave-master-key-v1"))
				s.masterKey = make([]byte, masterKeySize)
				if _, err := io.ReadFull(reader, s.masterKey); err != nil {
					return fmt.Errorf("derive master key from env: %w", err)
				}
				s.isHardware = false
				return nil
			}
		}

		// Last resort: random key (development only, cannot read existing DEKs)
		s.masterKey = make([]byte, masterKeySize)
		if _, err := rand.Read(s.masterKey); err != nil {
			return fmt.Errorf("generate dev key: %w", err)
		}
		s.isHardware = false
		return nil
	}

	// Derive master key using HKDF (TS parity: HKDF_INFO = 'track-record-enclave-dek')
	hash := sha256.New
	reader := hkdf.New(hash, measurement, nil, []byte("track-record-enclave-dek"))

	s.masterKey = make([]byte, masterKeySize)
	if _, err := io.ReadFull(reader, s.masterKey); err != nil {
		return fmt.Errorf("derive master key: %w", err)
	}

	s.isHardware = true
	return nil
}

// getSEVMeasurement reads the SEV-SNP measurement using snpguest.
func (s *KeyDerivationService) getSEVMeasurement() ([]byte, error) {
	// Check if running in SEV-SNP environment
	if _, err := os.Stat(sevGuestDevice); os.IsNotExist(err) {
		return nil, fmt.Errorf("SEV-SNP not available")
	}

	// Use snpguest to get the measurement (same as TS attestation flow)
	snpguestPath := "/usr/bin/snpguest"
	if _, err := os.Stat(snpguestPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("snpguest not found")
	}

	// Create temp files for report
	tmpDir, err := os.MkdirTemp("", "snp-key-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	reportPath := tmpDir + "/report.bin"
	requestPath := tmpDir + "/request.bin"

	// Write empty request data
	os.WriteFile(requestPath, make([]byte, 64), 0600)

	// Generate report
	cmd := exec.Command(snpguestPath, "report", reportPath, requestPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("snpguest report: %w: %s", err, string(output))
	}

	// Display and parse measurement
	displayCmd := exec.Command(snpguestPath, "display", "report", reportPath)
	output, err := displayCmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("snpguest display: %w", err)
	}

	// Parse measurement from output
	re := regexp.MustCompile(`(?i)measurement[:\s]+([0-9a-fA-F]+)`)
	matches := re.FindSubmatch(output)
	if len(matches) < 2 {
		return nil, fmt.Errorf("measurement not found in snpguest output")
	}

	measurement, err := hexDecode(string(matches[1]))
	if err != nil {
		return nil, fmt.Errorf("decode measurement: %w", err)
	}

	return measurement, nil
}

// WrapKey encrypts a DEK with the master key
func (s *KeyDerivationService) WrapKey(dek []byte) (*EncryptedData, error) {
	svc, err := New(s.masterKey)
	if err != nil {
		return nil, err
	}
	return svc.Encrypt(dek)
}

// UnwrapKey decrypts a DEK with the master key
func (s *KeyDerivationService) UnwrapKey(wrapped *EncryptedData) ([]byte, error) {
	svc, err := New(s.masterKey)
	if err != nil {
		return nil, err
	}
	return svc.Decrypt(wrapped)
}

// IsHardwareKey returns true if using hardware-derived key
func (s *KeyDerivationService) IsHardwareKey() bool {
	return s.isHardware
}

// GetMasterKeyID returns a hash of the master key for identification
func (s *KeyDerivationService) GetMasterKeyID() string {
	hash := sha256.Sum256(s.masterKey)
	return fmt.Sprintf("%x", hash[:8])
}

package encryption

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/snpguest"
	"go.uber.org/zap"
	"golang.org/x/crypto/hkdf"
)

const (
	sevGuestDevice = "/dev/sev-guest"
	masterKeySize  = 32

	// hkdfInfoMasterKey must match the TS enclave's HKDF info string
	// (src/services/key-derivation.service.ts: HKDF_INFO =
	// 'track-record-enclave-dek'). If this diverges, Go will derive a
	// different master key from the same SEV-SNP measurement and be
	// unable to unwrap DEKs produced by the TS enclave.
	hkdfInfoMasterKey = "track-record-enclave-dek"
)

// KeyDerivationService derives master keys from hardware measurements.
//
// On hosts with /dev/sev-guest + snpguest available it reads the SEV-SNP
// attestation report, extracts the launch measurement, and derives the
// master key via HKDF-SHA256 with the same parameters as the TS enclave.
// Without the hardware path, it falls back to interpreting the
// ENCRYPTION_KEY environment variable as the master key directly — this
// mirrors the TS enclave's dev-mode behaviour and is only safe for test
// environments where both TS and Go point at the same pre-seeded key.
type KeyDerivationService struct {
	masterKey    []byte
	isHardware   bool
	snpguestPath string
	logger       *zap.Logger
}

// NewKeyDerivationService creates a new key derivation service.
func NewKeyDerivationService(logger *zap.Logger) (*KeyDerivationService, error) {
	svc := &KeyDerivationService{
		snpguestPath: snpguest.ResolvePath(logger),
		logger:       logger,
	}
	if err := svc.deriveMasterKey(); err != nil {
		return nil, err
	}
	return svc, nil
}

// deriveMasterKey populates s.masterKey. Preferred path is SEV-SNP
// hardware. Falls back to the ENCRYPTION_KEY environment variable when
// hardware is not available.
func (s *KeyDerivationService) deriveMasterKey() error {
	measurement, platformVersion, err := s.getSEVMeasurement()
	if err == nil && len(measurement) > 0 {
		// TS parity: HKDF-SHA256(measurement, salt=platformVersion-utf8,
		// info="track-record-enclave-dek", length=32). The TS enclave
		// passes Buffer.from(platformVersion, 'utf8') when present and
		// Buffer.alloc(0) otherwise. Go's hkdf.New treats an empty
		// []byte identically to nil per RFC 5869.
		var salt []byte
		if platformVersion != "" {
			salt = []byte(platformVersion)
		}
		reader := hkdf.New(sha256.New, measurement, salt, []byte(hkdfInfoMasterKey))
		s.masterKey = make([]byte, masterKeySize)
		if _, err := io.ReadFull(reader, s.masterKey); err != nil {
			return fmt.Errorf("derive master key from sev-snp measurement: %w", err)
		}
		s.isHardware = true
		if s.logger != nil {
			s.logger.Info("master key derived from SEV-SNP measurement",
				zap.String("measurement_prefix", hex.EncodeToString(measurement[:min(8, len(measurement))])),
				zap.String("master_key_id", s.GetMasterKeyID()),
			)
		}
		return nil
	}

	// Hardware path unavailable or failed. Fall back to the env-var key.
	// This matches the TS enclave's dev-mode behaviour where
	// ENCRYPTION_KEY is interpreted as the 32-byte master key directly
	// (no HKDF, no hardware derivation). Only safe when the TS and Go
	// enclaves agree on the same pre-seeded master key.
	envKey := strings.TrimSpace(os.Getenv("ENCRYPTION_KEY"))
	if envKey == "" {
		if err != nil {
			return fmt.Errorf("cannot derive master key: sev-snp unavailable (%w) and ENCRYPTION_KEY not set", err)
		}
		return fmt.Errorf("cannot derive master key: sev-snp unavailable and ENCRYPTION_KEY not set")
	}

	keyBytes, decErr := hex.DecodeString(envKey)
	if decErr != nil {
		return fmt.Errorf("ENCRYPTION_KEY is not valid hex: %w", decErr)
	}
	if len(keyBytes) != masterKeySize {
		return fmt.Errorf("ENCRYPTION_KEY must decode to %d bytes, got %d", masterKeySize, len(keyBytes))
	}
	s.masterKey = keyBytes
	s.isHardware = false
	if s.logger != nil {
		reason := "sev-snp unavailable"
		if err != nil {
			reason = err.Error()
		}
		s.logger.Warn("falling back to ENCRYPTION_KEY env var as master key (dev/test mode)",
			zap.String("reason", reason),
			zap.String("master_key_id", s.GetMasterKeyID()),
		)
	}
	return nil
}

// getSEVMeasurement extracts the launch measurement from a fresh
// snpguest attestation report. Returns (measurement, platformVersion).
// platformVersion is empty for snpguest-derived reports because the
// standard `snpguest display report` output does not include a
// "Platform Version" field (the TS enclave also observes this).
func (s *KeyDerivationService) getSEVMeasurement() ([]byte, string, error) {
	if _, err := os.Stat(sevGuestDevice); os.IsNotExist(err) {
		return nil, "", fmt.Errorf("sev-snp not available: %s missing", sevGuestDevice)
	}
	if s.snpguestPath == "" {
		return nil, "", fmt.Errorf("snpguest binary not found")
	}

	tmpDir, err := os.MkdirTemp("", "snp-keyderiv-*")
	if err != nil {
		return nil, "", fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	reportPath := filepath.Join(tmpDir, "report.bin")
	requestPath := filepath.Join(tmpDir, "request.bin")
	if err := os.WriteFile(requestPath, make([]byte, 64), 0600); err != nil {
		return nil, "", fmt.Errorf("write request data: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, s.snpguestPath, "report", reportPath, requestPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		// Retry with --random because some kernels reject empty request
		// data; the measurement field is independent of the request
		// data so this does not affect key derivation.
		retry := exec.CommandContext(ctx, s.snpguestPath, "report", reportPath, requestPath, "--random")
		if retryOut, retryErr := retry.CombinedOutput(); retryErr != nil {
			return nil, "", fmt.Errorf("snpguest report: %w: %s / %s", err, string(output), string(retryOut))
		}
	}

	displayCmd := exec.CommandContext(ctx, s.snpguestPath, "display", "report", reportPath)
	output, err := displayCmd.CombinedOutput()
	if err != nil {
		return nil, "", fmt.Errorf("snpguest display: %w: %s", err, string(output))
	}

	measurement, platformVersion := parseMeasurementFromDisplay(string(output))
	if len(measurement) == 0 {
		return nil, "", fmt.Errorf("measurement not found in snpguest output")
	}
	return measurement, platformVersion, nil
}

// measurementHeaderRegex matches the "Measurement:" section header.
// The actual measurement bytes follow on one or more subsequent lines
// as space-separated hex octets — we collect them with a small state
// machine in parseMeasurementFromDisplay to mirror the TS parser.
var measurementHeaderRegex = regexp.MustCompile(`(?i)^measurement:\s*$`)

// hexDumpLineRegex matches a continuation line in a hex dump, e.g.
// "12 06 83 61 36 9c f9 17 9b b6 ac 08 57 2b 7e 15".
var hexDumpLineRegex = regexp.MustCompile(`(?i)^[0-9a-f]{2}(\s+[0-9a-f]{2})+\s*$`)

// parseMeasurementFromDisplay extracts the measurement bytes from
// `snpguest display report` output. Also returns a platformVersion
// string (currently always empty for snpguest-sourced reports; kept as
// a return value so the caller can evolve this cleanly without
// changing the signature).
func parseMeasurementFromDisplay(output string) ([]byte, string) {
	var inMeasurement bool
	var hexBuffer bytes.Buffer

	for _, rawLine := range strings.Split(output, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}

		if measurementHeaderRegex.MatchString(line) {
			inMeasurement = true
			continue
		}

		if inMeasurement {
			if hexDumpLineRegex.MatchString(line) {
				// Strip whitespace and accumulate as raw hex.
				hexBuffer.WriteString(strings.ReplaceAll(line, " ", ""))
				continue
			}
			// First non-hex line after Measurement: ends the section.
			break
		}
	}

	if hexBuffer.Len() == 0 {
		return nil, ""
	}
	bytes, err := hex.DecodeString(hexBuffer.String())
	if err != nil {
		return nil, ""
	}
	return bytes, ""
}

// WrapKey encrypts a DEK with the master key using AES-256-GCM.
// Matches TS key-derivation.service.ts wrapKey() which uses a 12-byte
// IV and base64 encoding for the three fields.
func (s *KeyDerivationService) WrapKey(dek []byte) (*EncryptedData, error) {
	svc, err := New(s.masterKey)
	if err != nil {
		return nil, err
	}
	return svc.Encrypt(dek)
}

// UnwrapKey decrypts a wrapped DEK with the master key. The three
// base64 fields (IV, ciphertext, auth tag) come from the
// data_encryption_keys table and were written by the TS enclave using
// exactly the same layout — see wrapKey() above.
func (s *KeyDerivationService) UnwrapKey(wrapped *EncryptedData) ([]byte, error) {
	svc, err := New(s.masterKey)
	if err != nil {
		return nil, err
	}
	return svc.Decrypt(wrapped)
}

// IsHardwareKey returns true if the master key came from SEV-SNP
// attestation, false if it came from the ENCRYPTION_KEY env var
// fallback.
func (s *KeyDerivationService) IsHardwareKey() bool {
	return s.isHardware
}

// GetMasterKeyID returns the first 8 bytes of SHA-256(masterKey) as hex,
// matching the TS enclave's getMasterKeyId() so that the
// data_encryption_keys.master_key_id column is comparable between Go
// and TS.
func (s *KeyDerivationService) GetMasterKeyID() string {
	hash := sha256.Sum256(s.masterKey)
	return hex.EncodeToString(hash[:8])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

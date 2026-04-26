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
//
// MIGRATION-DEK (zero-downtime binary upgrade): when both paths are
// available the service keeps both master keys in memory. Callers can
// then unwrap a DEK with the hardware-derived key (legacy DEKs wrapped
// by the previous binary) AND re-wrap it with the env-derived key
// (which survives binary upgrades — same ENCRYPTION_KEY across builds).
// See cmd/migrate-dek-wrap for the one-shot rewrap procedure.
type KeyDerivationService struct {
	// masterKey is the *primary* master key used by WrapKey and
	// UnwrapKey. It points to masterHW when hardware is available,
	// otherwise to masterENV. Existing call sites keep their semantics.
	masterKey []byte

	// masterHW is the SEV-SNP measurement-derived master key. nil when
	// /dev/sev-guest is missing or snpguest is unreachable.
	masterHW []byte

	// masterENV is the ENCRYPTION_KEY-derived master key. nil when the
	// env var is not set / not 32 bytes of hex.
	masterENV []byte

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

// deriveMasterKey populates s.masterHW and/or s.masterENV from the
// available sources, then sets s.masterKey to the preferred one
// (hardware if present, else env). At least one of the two paths must
// succeed; otherwise the function returns an error.
//
// Why both? cmd/migrate-dek-wrap needs to unwrap a DEK with masterHW
// (legacy wrap) and re-wrap it with masterENV in the same process so a
// future binary upgrade — which changes the SEV-SNP measurement and
// therefore masterHW — can still unwrap the DEK via masterENV.
func (s *KeyDerivationService) deriveMasterKey() error {
	hwErr := s.deriveHardwareMasterKey()
	envErr := s.deriveEnvMasterKey()

	switch {
	case s.masterHW != nil:
		s.masterKey = s.masterHW
		s.isHardware = true
		if s.logger != nil && s.masterENV != nil {
			s.logger.Info("env-derived master key also available (migration helper)",
				zap.String("env_master_key_id", s.envMasterKeyID()))
		}
	case s.masterENV != nil:
		s.masterKey = s.masterENV
		s.isHardware = false
		if s.logger != nil {
			reason := "sev-snp unavailable"
			if hwErr != nil {
				reason = hwErr.Error()
			}
			s.logger.Warn("falling back to ENCRYPTION_KEY env var as master key (dev/test mode)",
				zap.String("reason", reason),
				zap.String("master_key_id", s.GetMasterKeyID()),
			)
		}
	default:
		// Both paths failed — surface the most informative error.
		if hwErr != nil && envErr != nil {
			return fmt.Errorf("cannot derive master key: sev-snp unavailable (%w) and ENCRYPTION_KEY missing/invalid (%w)", hwErr, envErr)
		}
		if hwErr != nil {
			return fmt.Errorf("cannot derive master key: sev-snp unavailable (%w) and ENCRYPTION_KEY not set", hwErr)
		}
		return fmt.Errorf("cannot derive master key: sev-snp unavailable and ENCRYPTION_KEY missing/invalid: %w", envErr)
	}
	return nil
}

// deriveHardwareMasterKey populates s.masterHW from the SEV-SNP
// measurement when available. Returns the underlying error on failure
// so the caller can include it in the aggregate error if both paths fail.
func (s *KeyDerivationService) deriveHardwareMasterKey() error {
	measurement, platformVersion, err := s.getSEVMeasurement()
	if err != nil {
		return err
	}
	if len(measurement) == 0 {
		return fmt.Errorf("empty SEV-SNP measurement")
	}

	// TS parity: HKDF-SHA256(measurement, salt=platformVersion-utf8,
	// info="track-record-enclave-dek", length=32).
	var salt []byte
	if platformVersion != "" {
		salt = []byte(platformVersion)
	}
	reader := hkdf.New(sha256.New, measurement, salt, []byte(hkdfInfoMasterKey))
	key := make([]byte, masterKeySize)
	if _, err := io.ReadFull(reader, key); err != nil {
		return fmt.Errorf("derive master key from sev-snp measurement: %w", err)
	}
	s.masterHW = key
	if s.logger != nil {
		s.logger.Info("master key derived from SEV-SNP measurement",
			zap.String("measurement_prefix", hex.EncodeToString(measurement[:min(8, len(measurement))])),
			zap.String("master_key_id", s.hwMasterKeyID()),
		)
	}
	return nil
}

// deriveEnvMasterKey populates s.masterENV from the ENCRYPTION_KEY env
// var when set and valid. Returns the underlying error on failure so
// the caller can include it in the aggregate error if both paths fail.
func (s *KeyDerivationService) deriveEnvMasterKey() error {
	envKey := strings.TrimSpace(os.Getenv("ENCRYPTION_KEY"))
	if envKey == "" {
		return fmt.Errorf("ENCRYPTION_KEY not set")
	}
	keyBytes, err := hex.DecodeString(envKey)
	if err != nil {
		return fmt.Errorf("ENCRYPTION_KEY is not valid hex: %w", err)
	}
	if len(keyBytes) != masterKeySize {
		return fmt.Errorf("ENCRYPTION_KEY must decode to %d bytes, got %d", masterKeySize, len(keyBytes))
	}
	s.masterENV = keyBytes
	return nil
}

// hwMasterKeyID returns the master_key_id for the hardware-derived
// master key (or "" if it isn't available).
func (s *KeyDerivationService) hwMasterKeyID() string {
	if s.masterHW == nil {
		return ""
	}
	hash := sha256.Sum256(s.masterHW)
	return hex.EncodeToString(hash[:8])
}

// envMasterKeyID returns the master_key_id for the env-derived master
// key (or "" if it isn't available).
func (s *KeyDerivationService) envMasterKeyID() string {
	if s.masterENV == nil {
		return ""
	}
	hash := sha256.Sum256(s.masterENV)
	return hex.EncodeToString(hash[:8])
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

// UnwrapSource identifies which master key successfully decrypted a
// wrapped DEK. Useful for telling the operator whether the active DEK
// is still bound to the SEV-SNP measurement or has already been
// migrated to env-only wrap.
type UnwrapSource string

const (
	UnwrapSourceHardware UnwrapSource = "hardware"
	UnwrapSourceEnv      UnwrapSource = "env"
)

// UnwrapKeyTryAll attempts to decrypt a wrapped DEK with every master
// key the service has available. It tries hardware first (legacy DEKs
// produced by older binaries on the same enclave will succeed there),
// then falls back to env (DEKs that have been re-wrapped via
// cmd/migrate-dek-wrap, which survives a binary upgrade because the
// env master key is independent of the SEV-SNP measurement).
//
// Returns the unwrapped DEK and the source that produced it. When all
// available paths fail, returns the error from the last attempt so
// callers can surface a precise diagnostic.
func (s *KeyDerivationService) UnwrapKeyTryAll(wrapped *EncryptedData) ([]byte, UnwrapSource, error) {
	var lastErr error

	if s.masterHW != nil {
		svc, err := New(s.masterHW)
		if err == nil {
			if dek, err := svc.Decrypt(wrapped); err == nil {
				return dek, UnwrapSourceHardware, nil
			} else {
				lastErr = err
			}
		} else {
			lastErr = err
		}
	}

	if s.masterENV != nil {
		svc, err := New(s.masterENV)
		if err == nil {
			if dek, err := svc.Decrypt(wrapped); err == nil {
				return dek, UnwrapSourceEnv, nil
			} else {
				lastErr = err
			}
		} else {
			lastErr = err
		}
	}

	if lastErr == nil {
		// Should not happen — either we attempted at least one source
		// (and lastErr is set) or deriveMasterKey would have returned
		// an error before this point.
		lastErr = fmt.Errorf("no master key available")
	}
	return nil, "", lastErr
}

// WrapKeyEnv encrypts a DEK with the env-derived master key
// specifically. Used by cmd/migrate-dek-wrap to re-wrap an existing
// DEK so it survives a binary upgrade.
//
// Returns ErrEnvMasterKeyUnavailable when ENCRYPTION_KEY isn't set —
// migrating to env-wrap requires the env key to be present.
func (s *KeyDerivationService) WrapKeyEnv(dek []byte) (*EncryptedData, error) {
	if s.masterENV == nil {
		return nil, ErrEnvMasterKeyUnavailable
	}
	svc, err := New(s.masterENV)
	if err != nil {
		return nil, err
	}
	return svc.Encrypt(dek)
}

// EnvMasterKeyID returns the master_key_id derived from the env path,
// or "" when ENCRYPTION_KEY is not set / invalid. Exposed for the
// migration tool which must record the new master_key_id alongside
// the re-wrapped DEK.
func (s *KeyDerivationService) EnvMasterKeyID() string {
	return s.envMasterKeyID()
}

// HasEnvMasterKey reports whether the env-derived master key is
// available. Callers that need to migrate DEK wraps must check this
// before attempting WrapKeyEnv.
func (s *KeyDerivationService) HasEnvMasterKey() bool {
	return s.masterENV != nil
}

// ErrEnvMasterKeyUnavailable is returned by WrapKeyEnv when
// ENCRYPTION_KEY is not set — the migration tool surfaces this
// directly to the operator with a remediation hint.
var ErrEnvMasterKeyUnavailable = fmt.Errorf("env master key not available (ENCRYPTION_KEY missing or invalid)")

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

package attestation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/snpguest"
	"go.uber.org/zap"
)

// Platform values embedded in both the REST attestation response and the
// signed report payload. Explicit constants make it a compile error to
// mis-spell "dev" vs "unattested-dev" vs "sev-snp" (SEC-115).
const (
	// PlatformSevSnp means the enclave is running on attested AMD SEV-SNP
	// hardware and the SevSnpReport fields are meaningful.
	PlatformSevSnp = "sev-snp"

	// PlatformUnattestedDev replaces the legacy "dev" value. Renamed so a
	// client that only inspects the top-level Platform field cannot quietly
	// treat unattested output as attested output.
	PlatformUnattestedDev = "unattested-dev"
)

// VCEK certificate filenames. QUAL-001: extracted to remove the 3- and
// 4-way duplications across fetchAndCacheVCEK.
const (
	vcekPEMFile = "vcek.pem"
	askPEMFile  = "ask.pem"
)

// AttestationReport holds the full attestation response.
type AttestationReport struct {
	Attestation   *SevSnpReport `json:"attestation"`
	TLSBinding    *TLSBinding   `json:"tls_binding"`
	E2EEncryption *E2EInfo      `json:"e2e_encryption"`
	ReportSigning *SigningInfo  `json:"report_signing"`
	Security      *SecurityInfo `json:"security"`
	Platform      string        `json:"platform"` // "sev-snp" or "unattested-dev" (SEC-115)
}

// SevSnpReport contains SEV-SNP hardware attestation data.
//
// ReportDataBoundToRequest is true when the REPORT_DATA field of the SEV-SNP
// quote contains the hash we asked snpguest to embed (tls_fp || e2e_pk ||
// signing_pk [|| nonce]). When false we fell back to snpguest --random — the
// quote is still signed by AMD hardware but the 64-byte REPORT_DATA is not a
// cryptographic binding to this enclave's keys. A verifier checking the
// tls/signing-key binding MUST refuse reports with this flag set to false
// (SEC-104).
type SevSnpReport struct {
	Verified                 bool   `json:"verified"`
	SevSnpEnabled            bool   `json:"sev_snp_enabled"`
	Measurement              string `json:"measurement,omitempty"`
	ReportData               string `json:"report_data,omitempty"`
	ReportDataBoundToRequest bool   `json:"report_data_bound_to_request"`
	PlatformVersion          string `json:"platform_version,omitempty"`
	VcekVerified             bool   `json:"vcek_verified"`
}

// TLSBinding holds TLS certificate fingerprint for attestation binding.
type TLSBinding struct {
	Fingerprint string `json:"fingerprint"`
	Bound       bool   `json:"bound"`
	Algorithm   string `json:"algorithm"`
}

// E2EInfo holds E2E encryption public key info.
type E2EInfo struct {
	PublicKey string `json:"public_key"`
	Algorithm string `json:"algorithm"`
}

// SigningInfo holds report signing public key info.
type SigningInfo struct {
	PublicKey string `json:"public_key"`
	Algorithm string `json:"algorithm"`
}

// SecurityInfo summarizes security features.
type SecurityInfo struct {
	TLSMitmProtection bool `json:"tls_mitm_protection"`
	E2EMitmProtection bool `json:"e2e_mitm_protection"`
	HardwareAttested  bool `json:"hardware_attested"`
}

// Service manages SEV-SNP attestation with caching.
type Service struct {
	tlsFingerprint string
	e2ePublicKey   string
	signingPubKey  string
	devMode        bool
	logger         *zap.Logger

	// Resolved snpguest binary path (empty if not found on PATH)
	snpguestPath string

	// VCEK cert caching
	vcekCacheDir string
	vcekTTL      time.Duration

	// Attestation cache
	attestCache    *AttestationReport
	attestCachedAt time.Time
	attestTTL      time.Duration

	mu sync.RWMutex
}

// Options configures the attestation service.
type Options struct {
	TLSFingerprint string
	E2EPublicKey   string
	SigningPubKey  string
	DevMode        bool
	Logger         *zap.Logger
}

// NewService creates a new attestation service.
//
// Resolves the snpguest binary at construction time via the shared
// snpguest.ResolvePath helper, which checks /usr/local/bin, /usr/bin,
// then $PATH. This matters because Dockerfile.production installs
// snpguest to /usr/local/bin while older setups placed it in /usr/bin —
// without resolution the service would silently fall back to dev mode
// on the TEE.
func NewService(opts Options) *Service {
	return &Service{
		tlsFingerprint: opts.TLSFingerprint,
		e2ePublicKey:   opts.E2EPublicKey,
		signingPubKey:  opts.SigningPubKey,
		devMode:        opts.DevMode,
		logger:         opts.Logger,
		snpguestPath:   snpguest.ResolvePath(opts.Logger),
		vcekCacheDir:   "/var/cache/enclave/certs",
		vcekTTL:        7 * 24 * time.Hour,
		attestTTL:      5 * time.Second,
	}
}

// GetAttestation returns the current attestation report (cached for 5s).
// The quote has no client nonce — it is only safe for same-TLS-session
// verification. For freshness guarantees across sessions / callers, use
// GetAttestationWithNonce (SEC-101).
func (s *Service) GetAttestation(ctx context.Context) (*AttestationReport, error) {
	s.mu.RLock()
	if s.attestCache != nil && time.Since(s.attestCachedAt) < s.attestTTL {
		cached := s.attestCache
		s.mu.RUnlock()
		return cached, nil
	}
	s.mu.RUnlock()

	report, err := s.generateAttestation(ctx, nil)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.attestCache = report
	s.attestCachedAt = time.Now()
	s.mu.Unlock()

	return report, nil
}

// GetAttestationWithNonce returns a fresh attestation report whose report_data
// binds the supplied client nonce (SEC-101). Never cached: every call invokes
// snpguest. Callers should pass a 32+ byte random nonce they generated for the
// current verification session.
//
// The resulting report_data = SHA256(tls_fp || e2e_pk || signing_pk || nonce).
// A verifier checks the same hash locally after fetching the quote and the
// TLS / signing / E2E public keys, which proves freshness (nonce) AND the
// keys-to-hardware binding in one step.
func (s *Service) GetAttestationWithNonce(ctx context.Context, nonce []byte) (*AttestationReport, error) {
	if len(nonce) == 0 {
		return nil, fmt.Errorf("nonce is required")
	}
	return s.generateAttestation(ctx, nonce)
}

func (s *Service) generateAttestation(ctx context.Context, nonce []byte) (*AttestationReport, error) {
	report := &AttestationReport{
		TLSBinding: &TLSBinding{
			Fingerprint: s.tlsFingerprint,
			Bound:       s.tlsFingerprint != "",
			Algorithm:   "SHA-256",
		},
		E2EEncryption: &E2EInfo{
			PublicKey: s.e2ePublicKey,
			Algorithm: "ECIES-P256-HKDF-AES256GCM",
		},
		ReportSigning: &SigningInfo{
			PublicKey: s.signingPubKey,
			Algorithm: "ECDSA-P256-SHA256",
		},
		Security: &SecurityInfo{
			TLSMitmProtection: s.tlsFingerprint != "",
			E2EMitmProtection: s.e2ePublicKey != "",
		},
	}

	if s.devMode {
		report.Platform = PlatformUnattestedDev
		report.Attestation = &SevSnpReport{
			Verified:      false,
			SevSnpEnabled: false,
		}
		report.Security.HardwareAttested = false
		return report, nil
	}

	// Try SEV-SNP attestation
	sevReport, err := s.fetchSevSnpAttestation(ctx, nonce)
	if err != nil {
		s.logger.Warn("SEV-SNP attestation failed, falling back to dev mode", zap.Error(err))
		report.Platform = PlatformUnattestedDev
		report.Attestation = &SevSnpReport{
			Verified:      false,
			SevSnpEnabled: false,
		}
		report.Security.HardwareAttested = false
		return report, nil
	}

	report.Platform = PlatformSevSnp
	report.Attestation = sevReport
	report.Security.HardwareAttested = sevReport.Verified
	return report, nil
}

func (s *Service) fetchSevSnpAttestation(ctx context.Context, nonce []byte) (*SevSnpReport, error) {
	// Build report data: SHA-256 of (tls_fp || e2e_pk || signing_pk || nonce).
	// nonce is nil for cached GET /api/v1/attestation and non-nil for the
	// nonce-bound POST flow (SEC-101).
	reportData := s.buildReportData(nonce)

	// Only snpguest is supported. Previous Azure IMDS / GCP metadata fallbacks
	// returned Verified=true after a plain HTTP 200 without verifying the
	// Azure attested-document JWS or the GCP Confidential-VM JWT (SEC-003 /
	// SEC-103). They have been removed pending a proper implementation.
	//
	// On a host without snpguest the caller falls back to dev mode (see
	// generateAttestation) — attested=false is the honest answer.
	return s.fetchWithSnpguest(ctx, reportData)
}

// buildReportData computes the 64-byte REPORT_DATA payload embedded in the
// SEV-SNP attestation quote. SEC-102: each input is length-prefixed with its
// big-endian uint64 byte length so (tls_fp, e2e_pk) pairs whose concatenation
// happens to collide with a different split can never produce identical
// report_data. Mirrored in pkg/reportverify.expectedReportData.
func (s *Service) buildReportData(nonce []byte) string {
	h := sha256.New()
	writeLenPrefixed(h, []byte(s.tlsFingerprint))
	writeLenPrefixed(h, []byte(s.e2ePublicKey))
	writeLenPrefixed(h, []byte(s.signingPubKey))
	writeLenPrefixed(h, nonce) // nil nonce ⇒ writes 8 zero bytes, still domain-separated
	return hex.EncodeToString(h.Sum(nil))
}

func writeLenPrefixed(h hash.Hash, b []byte) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(b)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(b)
}

// hexLineRegex matches a line that is part of a multi-line hex dump from
// `snpguest display report`, e.g. "12 06 83 61 36 9c f9 17 ...".
// snpguest 0.10+ emits one field header per section ("Measurement:"), then
// one or more lines of space-separated hex bytes, so we need a state-machine
// parser (see parseSnpguestReport) rather than single-line regexes.
var hexLineRegex = regexp.MustCompile(`(?i)^[0-9a-f]{2}(\s+[0-9a-f]{2})+\s*$`)

func (s *Service) fetchWithSnpguest(ctx context.Context, reportData string) (*SevSnpReport, error) {
	snpguestPath := s.snpguestPath
	if snpguestPath == "" {
		return nil, fmt.Errorf("snpguest not found on this system")
	}

	// Create temp directory for report files (TS parity)
	tmpDir, err := os.MkdirTemp("", "snp-attestation-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	reportPath := filepath.Join(tmpDir, "report.bin")
	requestPath := filepath.Join(tmpDir, "request.bin")
	certsDir := filepath.Join(tmpDir, "certs")
	os.MkdirAll(certsDir, 0700)

	// Write request data (TLS fingerprint padded to 64 bytes)
	requestBytes := make([]byte, 64)
	if decoded, err := hex.DecodeString(reportData); err == nil {
		copy(requestBytes, decoded)
	}
	if err := os.WriteFile(requestPath, requestBytes, 0600); err != nil {
		return nil, fmt.Errorf("write request data: %w", err)
	}

	// Step 1: Generate attestation report. The report embeds our 64-byte
	// request data (TLS fingerprint SHA-256) in its REPORT_DATA field; this
	// is what lets clients verify that the TLS cert they connect to is
	// bound to the attestation report.
	cmd := exec.CommandContext(ctx, snpguestPath, "report", reportPath, requestPath)
	output, err := cmd.CombinedOutput()
	reportBoundToRequest := true
	if err != nil {
		// Fallback: retry with --random. snpguest will fill REPORT_DATA with
		// random bytes, which means we LOSE the TLS binding for this report.
		// We surface this loudly so operators can diagnose why the primary
		// path failed instead of silently serving an unbound attestation.
		s.logger.Warn("snpguest report with request data failed, falling back to --random (TLS binding will be lost for this report)",
			zap.Error(err),
			zap.String("output", string(output)),
		)
		cmd = exec.CommandContext(ctx, snpguestPath, "report", reportPath, requestPath, "--random")
		output, err = cmd.CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("snpguest report: %w: %s", err, string(output))
		}
		reportBoundToRequest = false
	}

	// Step 2: Fetch and verify VCEK certificates
	vcekVerified := s.fetchAndCacheVCEK(ctx, reportPath, certsDir)

	// Step 3: Verify attestation with VCEK
	if vcekVerified {
		verifyCmd := exec.CommandContext(ctx, snpguestPath, "verify", "attestation", certsDir, reportPath)
		if verifyOut, err := verifyCmd.CombinedOutput(); err != nil {
			s.logger.Warn("snpguest verify failed", zap.String("output", string(verifyOut)))
			vcekVerified = false
		} else {
			s.logger.Info("snpguest VCEK verification successful")
		}
	}

	// Step 4: Display and parse the report
	displayCmd := exec.CommandContext(ctx, snpguestPath, "display", "report", reportPath)
	displayOutput, err := displayCmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("snpguest display: %w: %s", err, string(displayOutput))
	}

	report := &SevSnpReport{
		Verified:                 true,
		SevSnpEnabled:            true,
		VcekVerified:             vcekVerified,
		ReportDataBoundToRequest: reportBoundToRequest,
	}
	parseSnpguestReport(string(displayOutput), report)

	if report.Measurement == "" {
		return nil, fmt.Errorf("failed to parse measurement from snpguest output")
	}

	// Fallback: if display-report didn't emit the Report Data section (older
	// snpguest builds, or unexpected output format), recover it from the
	// request file we wrote ourselves. TS parity: sev-snp-attestation.service.ts
	// calls readRequestDataFile() for exactly the same reason.
	//
	// Only do this when we know the report IS bound to the request (i.e. we
	// didn't fall back to --random). Otherwise the file would lie about the
	// binding.
	if report.ReportData == "" && reportBoundToRequest {
		if rawRequest, err := os.ReadFile(requestPath); err == nil {
			report.ReportData = hex.EncodeToString(rawRequest)
			s.logger.Debug("recovered report_data from request file (display output had no Report Data section)")
		}
	}

	return report, nil
}

// parseSnpguestReport parses the multi-line output of `snpguest display report`.
//
// snpguest 0.10+ emits one section header per field (e.g. "Measurement:") then
// one or more lines of space-separated hex bytes. The previous single-line
// regex implementation only captured the first 2 hex chars. We mirror the TS
// parser (src/services/sev-snp-attestation.service.ts) by running a small
// state machine that accumulates hex bytes until a new field header is seen.
//
// Example input:
//
//	Version: 2
//	Guest SVN: 7
//	Measurement:
//	12 06 83 61 36 9c f9 17 9b b6 ac 08 57 2b 7e 15
//	ed 0b c8 ab b6 98 cb 04 d4 f5 84 f7 ff 51 2a 4c
//	20 81 c1 f5 b1 05 35 1d bd 45 c0 35 a7 d6 a3 a5
//	Report Data:
//	...
func parseSnpguestReport(output string, report *SevSnpReport) {
	var currentField string
	var hexBuffer []string

	flush := func() {
		if currentField == "" || len(hexBuffer) == 0 {
			return
		}
		// Join and strip all whitespace
		combined := strings.Join(hexBuffer, "")
		combined = strings.ReplaceAll(combined, " ", "")
		combined = strings.ReplaceAll(combined, "\t", "")
		switch currentField {
		case "measurement":
			report.Measurement = combined
		case "report_data":
			report.ReportData = combined
		case "platform_version":
			report.PlatformVersion = combined
		}
		hexBuffer = hexBuffer[:0]
	}

	for _, rawLine := range strings.Split(output, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}

		// If we're inside a known field and this line is a hex dump row,
		// accumulate it.
		if currentField != "" && hexLineRegex.MatchString(line) {
			hexBuffer = append(hexBuffer, line)
			continue
		}

		// Otherwise, a new header starts — flush the previous field and
		// detect the new one.
		flush()

		switch {
		case strings.HasPrefix(line, "Measurement:"):
			currentField = "measurement"
		case strings.HasPrefix(line, "Report Data:"):
			currentField = "report_data"
		case strings.HasPrefix(line, "Platform Version:"), strings.HasPrefix(line, "Platform_version:"):
			currentField = "platform_version"
		default:
			// Any other "Foo: value" line ends the current field without
			// starting a new tracked one.
			if strings.Contains(line, ":") {
				currentField = ""
			}
		}
	}

	// Final flush for the last field in the file.
	flush()
}

func (s *Service) fetchAndCacheVCEK(ctx context.Context, reportPath, certsDir string) bool {
	// Check cache first
	vcekPath := filepath.Join(s.vcekCacheDir, vcekPEMFile)
	if info, err := os.Stat(vcekPath); err == nil {
		if time.Since(info.ModTime()) < s.vcekTTL {
			// Copy cached certs to temp certsDir for verification
			if err := copyFile(vcekPath, filepath.Join(certsDir, vcekPEMFile)); err != nil {
				s.logger.Warn("failed to copy cached VCEK cert to temp dir",
					zap.String("src", vcekPath),
					zap.Error(err),
				)
				return false
			}
			caPath := filepath.Join(s.vcekCacheDir, askPEMFile)
			if _, err := os.Stat(caPath); err == nil {
				if err := copyFile(caPath, filepath.Join(certsDir, askPEMFile)); err != nil {
					s.logger.Warn("failed to copy cached CA cert to temp dir",
						zap.String("src", caPath),
						zap.Error(err),
					)
					return false
				}
			}
			return true
		}
	}

	// Ensure cache dir exists before writing (silent-create failure was masking errors)
	if err := os.MkdirAll(s.vcekCacheDir, 0700); err != nil {
		s.logger.Warn("failed to create VCEK cache dir (continuing without cache)",
			zap.String("dir", s.vcekCacheDir),
			zap.Error(err),
		)
	}

	if s.snpguestPath == "" {
		return false
	}

	// Fetch from AMD KDS via snpguest (TS parity: fetch vcek pem + ca pem)
	cmd := exec.CommandContext(ctx, s.snpguestPath, "fetch", "vcek", "pem", certsDir, reportPath)
	if err := cmd.Run(); err != nil {
		s.logger.Warn("failed to fetch VCEK certificate", zap.Error(err))
		return false
	}

	caCmd := exec.CommandContext(ctx, s.snpguestPath, "fetch", "ca", "pem", certsDir,
		"--endorser", "vcek", "-r", reportPath)
	if err := caCmd.Run(); err != nil {
		s.logger.Warn("failed to fetch CA certificate", zap.Error(err))
	}

	// Cache for next time (non-fatal: failure just means next boot refetches)
	if err := copyFile(filepath.Join(certsDir, vcekPEMFile), vcekPath); err != nil {
		s.logger.Warn("failed to cache VCEK certificate (next boot will refetch)",
			zap.String("dst", vcekPath),
			zap.Error(err),
		)
	}
	askSrc := filepath.Join(certsDir, askPEMFile)
	if _, err := os.Stat(askSrc); err == nil {
		if err := copyFile(askSrc, filepath.Join(s.vcekCacheDir, askPEMFile)); err != nil {
			s.logger.Warn("failed to cache ASK certificate (next boot will refetch)",
				zap.Error(err),
			)
		}
	}

	return true
}

// copyFile copies src to dst with 0600 perms. Returns an error on any failure
// instead of silently discarding it — callers must log/handle the error so
// VCEK cache issues are observable.
func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	if err := os.WriteFile(dst, data, 0600); err != nil {
		return fmt.Errorf("write %s: %w", dst, err)
	}
	return nil
}

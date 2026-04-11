package attestation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"go.uber.org/zap"
)

// AttestationReport holds the full attestation response.
type AttestationReport struct {
	Attestation   *SevSnpReport `json:"attestation"`
	TLSBinding    *TLSBinding   `json:"tls_binding"`
	E2EEncryption *E2EInfo      `json:"e2e_encryption"`
	ReportSigning *SigningInfo  `json:"report_signing"`
	Security      *SecurityInfo `json:"security"`
	Platform      string        `json:"platform"` // "sev-snp" or "dev"
}

// SevSnpReport contains SEV-SNP hardware attestation data.
type SevSnpReport struct {
	Verified        bool   `json:"verified"`
	SevSnpEnabled   bool   `json:"sev_snp_enabled"`
	Measurement     string `json:"measurement,omitempty"`
	ReportData      string `json:"report_data,omitempty"`
	PlatformVersion string `json:"platform_version,omitempty"`
	VcekVerified    bool   `json:"vcek_verified"`
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
// Resolves the snpguest binary at construction time by checking a list of
// well-known paths and falling back to $PATH lookup. This matters because
// Dockerfile.production installs snpguest to /usr/local/bin while older
// setups placed it in /usr/bin — without resolution the service would
// silently fall back to dev mode on the TEE.
func NewService(opts Options) *Service {
	return &Service{
		tlsFingerprint: opts.TLSFingerprint,
		e2ePublicKey:   opts.E2EPublicKey,
		signingPubKey:  opts.SigningPubKey,
		devMode:        opts.DevMode,
		logger:         opts.Logger,
		snpguestPath:   resolveSnpguestPath(opts.Logger),
		vcekCacheDir:   "/var/cache/enclave/certs",
		vcekTTL:        7 * 24 * time.Hour,
		attestTTL:      5 * time.Second,
	}
}

// resolveSnpguestPath returns the first snpguest binary found on disk.
// Returns "" if none is found — callers must treat that as "SEV-SNP unavailable".
func resolveSnpguestPath(logger *zap.Logger) string {
	candidates := []string{
		"/usr/local/bin/snpguest",
		"/usr/bin/snpguest",
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if p, err := exec.LookPath("snpguest"); err == nil {
		return p
	}
	if logger != nil {
		logger.Warn("snpguest binary not found — SEV-SNP attestation will fall back to dev mode",
			zap.Strings("searched", candidates),
		)
	}
	return ""
}

// GetAttestation returns the current attestation report (cached for 5s).
func (s *Service) GetAttestation(ctx context.Context) (*AttestationReport, error) {
	s.mu.RLock()
	if s.attestCache != nil && time.Since(s.attestCachedAt) < s.attestTTL {
		cached := s.attestCache
		s.mu.RUnlock()
		return cached, nil
	}
	s.mu.RUnlock()

	report, err := s.generateAttestation(ctx)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.attestCache = report
	s.attestCachedAt = time.Now()
	s.mu.Unlock()

	return report, nil
}

func (s *Service) generateAttestation(ctx context.Context) (*AttestationReport, error) {
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
		report.Platform = "dev"
		report.Attestation = &SevSnpReport{
			Verified:      false,
			SevSnpEnabled: false,
		}
		report.Security.HardwareAttested = false
		return report, nil
	}

	// Try SEV-SNP attestation
	sevReport, err := s.fetchSevSnpAttestation(ctx)
	if err != nil {
		s.logger.Warn("SEV-SNP attestation failed, falling back to dev mode", zap.Error(err))
		report.Platform = "dev"
		report.Attestation = &SevSnpReport{
			Verified:      false,
			SevSnpEnabled: false,
		}
		report.Security.HardwareAttested = false
		return report, nil
	}

	report.Platform = "sev-snp"
	report.Attestation = sevReport
	report.Security.HardwareAttested = sevReport.Verified
	return report, nil
}

func (s *Service) fetchSevSnpAttestation(ctx context.Context) (*SevSnpReport, error) {
	// Build report data: SHA-256 of TLS fingerprint for binding
	reportData := s.buildReportData()

	// Try snpguest first
	if snpReport, err := s.fetchWithSnpguest(ctx, reportData); err == nil {
		return snpReport, nil
	}

	// Try Azure IMDS
	if snpReport, err := s.fetchFromAzure(ctx); err == nil {
		return snpReport, nil
	}

	// Try GCP metadata
	if snpReport, err := s.fetchFromGCP(ctx); err == nil {
		return snpReport, nil
	}

	return nil, fmt.Errorf("no attestation source available")
}

func (s *Service) buildReportData() string {
	h := sha256.New()
	h.Write([]byte(s.tlsFingerprint))
	h.Write([]byte(s.e2ePublicKey))
	h.Write([]byte(s.signingPubKey))
	return hex.EncodeToString(h.Sum(nil))
}

var measurementRegex = regexp.MustCompile(`(?i)measurement[:\s]+([0-9a-fA-F]+)`)
var reportDataRegex = regexp.MustCompile(`(?i)report_data[:\s]+([0-9a-fA-F]+)`)
var platformVersionRegex = regexp.MustCompile(`(?i)platform_version[:\s]+([0-9a-fA-F]+)`)

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

	// Step 1: Generate attestation report
	cmd := exec.CommandContext(ctx, snpguestPath, "report", reportPath, requestPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Retry with --random flag
		cmd = exec.CommandContext(ctx, snpguestPath, "report", reportPath, requestPath, "--random")
		output, err = cmd.CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("snpguest report: %w: %s", err, string(output))
		}
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

	outputStr := string(displayOutput)
	report := &SevSnpReport{
		Verified:      true,
		SevSnpEnabled: true,
		VcekVerified:  vcekVerified,
	}

	if matches := measurementRegex.FindStringSubmatch(outputStr); len(matches) > 1 {
		report.Measurement = matches[1]
	}
	if matches := reportDataRegex.FindStringSubmatch(outputStr); len(matches) > 1 {
		report.ReportData = matches[1]
	}
	if matches := platformVersionRegex.FindStringSubmatch(outputStr); len(matches) > 1 {
		report.PlatformVersion = matches[1]
	}

	return report, nil
}

func (s *Service) fetchAndCacheVCEK(ctx context.Context, reportPath, certsDir string) bool {
	// Check cache first
	vcekPath := filepath.Join(s.vcekCacheDir, "vcek.pem")
	if info, err := os.Stat(vcekPath); err == nil {
		if time.Since(info.ModTime()) < s.vcekTTL {
			// Copy cached certs to temp certsDir for verification
			if err := copyFile(vcekPath, filepath.Join(certsDir, "vcek.pem")); err != nil {
				s.logger.Warn("failed to copy cached VCEK cert to temp dir",
					zap.String("src", vcekPath),
					zap.Error(err),
				)
				return false
			}
			caPath := filepath.Join(s.vcekCacheDir, "ask.pem")
			if _, err := os.Stat(caPath); err == nil {
				if err := copyFile(caPath, filepath.Join(certsDir, "ask.pem")); err != nil {
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
	if err := copyFile(filepath.Join(certsDir, "vcek.pem"), vcekPath); err != nil {
		s.logger.Warn("failed to cache VCEK certificate (next boot will refetch)",
			zap.String("dst", vcekPath),
			zap.Error(err),
		)
	}
	askSrc := filepath.Join(certsDir, "ask.pem")
	if _, err := os.Stat(askSrc); err == nil {
		if err := copyFile(askSrc, filepath.Join(s.vcekCacheDir, "ask.pem")); err != nil {
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

func (s *Service) fetchFromAzure(ctx context.Context) (*SevSnpReport, error) {
	req, err := http.NewRequestWithContext(ctx, "GET",
		"http://169.254.169.254/metadata/attested/document?api-version=2021-01-01", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Metadata", "true")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("azure IMDS request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("azure IMDS returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Encoding  string `json:"encoding"`
		Signature string `json:"signature"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return &SevSnpReport{
		Verified:      true,
		SevSnpEnabled: true,
		VcekVerified:  false,
	}, nil
}

func (s *Service) fetchFromGCP(ctx context.Context) (*SevSnpReport, error) {
	req, err := http.NewRequestWithContext(ctx, "GET",
		"http://metadata.google.internal/computeMetadata/v1/instance/confidential-computing/attestation-report", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Metadata-Flavor", "Google")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gcp metadata request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gcp metadata returned %d", resp.StatusCode)
	}

	return &SevSnpReport{
		Verified:      true,
		SevSnpEnabled: true,
		VcekVerified:  false,
	}, nil
}

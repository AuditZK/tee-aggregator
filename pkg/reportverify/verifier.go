// Package reportverify is the canonical verifier for signed reports produced
// by the zero-knowledge-aggregator-go enclave.
//
// It implements the full end-to-end verification procedure documented at
// internal/signing/report.go:32-47, so integrators do not have to reimplement
// the six steps and get one wrong. Every method here is strict: any missing
// input, mismatched public key, unbound report_data, or out-of-allowlist
// measurement is a hard error.
//
// Typical usage:
//
//	v := &reportverify.Verifier{
//	    ExpectedSigningPublicKey: publishedKey,        // base64 DER SPKI
//	    AllowedMeasurements:      []string{"12345..."}, // hex launch measurements
//	    ExpectedTLSFingerprint:   tlsFPHex,
//	    ExpectedE2EPublicKey:     e2ePEM,
//	    Nonce:                    clientNonce, // optional — required for replay-safe verification
//	    RequireSevSnp:            true,
//	}
//	if err := v.Verify(report); err != nil {
//	    // report MUST NOT be trusted
//	}
//
// Designed so that a typo on any field fails closed, not open.
package reportverify

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"

	"github.com/trackrecord/enclave/internal/signing"
)

// Sentinel errors — callers can branch on these with errors.Is.
var (
	ErrNilReport             = errors.New("reportverify: nil report")
	ErrSignatureInvalid      = errors.New("reportverify: signature or hash did not validate")
	ErrPublicKeyMismatch     = errors.New("reportverify: report public key does not match expected")
	ErrNoAttestation         = errors.New("reportverify: report has no enclaveAttestation block")
	ErrNotAttested           = errors.New("reportverify: attestation.attested != true or platform != sev-snp")
	ErrReportDataNotBound    = errors.New("reportverify: attestation.reportDataBoundToRequest == false (snpguest --random fallback)")
	ErrMeasurementNotAllowed = errors.New("reportverify: measurement not in AllowedMeasurements")
	ErrReportDataMismatch    = errors.New("reportverify: reportData hash does not match expected (tls_fp || e2e_pk || signing_pk [|| nonce])")
	ErrVcekUnverified        = errors.New("reportverify: attestation.vcekVerified == false and no VCEKChecker supplied")
	ErrMissingExpectedInput  = errors.New("reportverify: a required expected input is empty")
)

// VCEKChecker is an optional hook that a caller can plug in to verify the
// SEV-SNP VCEK certificate chain against the AMD KDS out-of-band. When nil,
// the Verifier falls back to the attestation.vcekVerified flag that the
// enclave populated at signing time. In high-assurance integrations, plug in
// your own chain validator (e.g. fetching the VCEK via AMD KDS and validating
// against the AMD root) and leave nothing to the enclave's self-report.
type VCEKChecker interface {
	VerifyVCEK(measurement string) error
}

// Verifier bundles the expectations a caller must pin before trusting a
// signed report. Every field is inspected at Verify time; zero-value fields
// where a value was required produce ErrMissingExpectedInput.
type Verifier struct {
	// ExpectedSigningPublicKey is the enclave's signing public key (base64
	// DER SPKI), obtained out-of-band from /api/v1/attestation on a fresh
	// connection and cross-checked against a measurement allowlist. Required.
	ExpectedSigningPublicKey string

	// AllowedMeasurements is the list of audited SEV-SNP launch measurements
	// (hex, lowercase). Required when RequireSevSnp is true.
	AllowedMeasurements []string

	// ExpectedTLSFingerprint is SHA-256 of the enclave's TLS certificate, hex.
	// Required whenever RequireSevSnp is true — used in the report_data hash
	// to prove the enclave's keys are bound to this quote.
	ExpectedTLSFingerprint string

	// ExpectedE2EPublicKey is the enclave's ECIES public key (PEM-encoded,
	// exactly as returned by /api/v1/attestation). Required whenever
	// RequireSevSnp is true.
	ExpectedE2EPublicKey string

	// Nonce is the 1..64-byte random bytes the caller submitted via
	// POST /api/v1/attestation. Pass the same bytes here; the Verifier will
	// include them in the report_data hash. nil for the deprecated no-nonce
	// flow (only safe for same-TLS-session use).
	Nonce []byte

	// RequireSevSnp toggles the hardware-attestation checks (steps 4–6 of the
	// verification procedure). Set to true in production. When false, only
	// the signature + public-key pinning is enforced.
	RequireSevSnp bool

	// VCEK is an optional out-of-band VCEK chain validator. When nil, the
	// Verifier reads the vcekVerified flag embedded in the signed payload.
	VCEK VCEKChecker
}

// Verify performs all six end-to-end checks. Returns nil only when the
// report can be trusted.
func (v *Verifier) Verify(report *signing.SignedReport) error {
	if report == nil {
		return ErrNilReport
	}
	if v.ExpectedSigningPublicKey == "" {
		return fmt.Errorf("%w: ExpectedSigningPublicKey", ErrMissingExpectedInput)
	}

	// Steps 1-3: pin the signing key, recompute hash, verify ECDSA signature.
	// VerifyReportStrict uses constant-time comparison for the pubkey check.
	ok, err := signing.VerifyReportStrict(report, v.ExpectedSigningPublicKey, v.RequireSevSnp)
	if err != nil {
		switch {
		case errors.Is(err, signing.ErrPublicKeyMismatch):
			return ErrPublicKeyMismatch
		case errors.Is(err, signing.ErrAttestationNotBound):
			return ErrReportDataNotBound
		default:
			return fmt.Errorf("reportverify: %w", err)
		}
	}
	if !ok {
		return ErrSignatureInvalid
	}

	// Everything below is RequireSevSnp territory.
	if !v.RequireSevSnp {
		return nil
	}

	att := report.EnclaveAttestation
	if att == nil {
		return ErrNoAttestation
	}
	if att.Platform != "sev-snp" || !att.Attested {
		return ErrNotAttested
	}
	if !att.ReportDataBoundToRequest {
		return ErrReportDataNotBound
	}

	// Step 4: measurement allowlist.
	if err := v.checkMeasurement(att.Measurement); err != nil {
		return err
	}

	// Step 5: report_data binding. Recompute SHA256 and compare byte-for-byte.
	if v.ExpectedTLSFingerprint == "" {
		return fmt.Errorf("%w: ExpectedTLSFingerprint", ErrMissingExpectedInput)
	}
	if v.ExpectedE2EPublicKey == "" {
		return fmt.Errorf("%w: ExpectedE2EPublicKey", ErrMissingExpectedInput)
	}
	expected := expectedReportData(
		v.ExpectedTLSFingerprint,
		v.ExpectedE2EPublicKey,
		v.ExpectedSigningPublicKey,
		v.Nonce,
	)
	if !constantTimeHexEqual(att.ReportData, expected) {
		return ErrReportDataMismatch
	}

	// Step 6: VCEK chain.
	if v.VCEK != nil {
		if err := v.VCEK.VerifyVCEK(att.Measurement); err != nil {
			return fmt.Errorf("reportverify: VCEK verification failed: %w", err)
		}
	} else if !att.VcekVerified {
		return ErrVcekUnverified
	}

	return nil
}

func (v *Verifier) checkMeasurement(got string) error {
	if len(v.AllowedMeasurements) == 0 {
		return fmt.Errorf("%w: AllowedMeasurements", ErrMissingExpectedInput)
	}
	got = lowerHex(got)
	for _, allowed := range v.AllowedMeasurements {
		if lowerHex(allowed) == got {
			return nil
		}
	}
	return ErrMeasurementNotAllowed
}

// ExpectedReportData exposes the canonical SHA-256 of
// (tlsFingerprint || e2ePublicKey || signingPublicKey [|| nonce])
// for callers that want to compute it without pulling in the crypto directly.
func ExpectedReportData(tlsFingerprint, e2ePublicKey, signingPublicKey string, nonce []byte) string {
	return expectedReportData(tlsFingerprint, e2ePublicKey, signingPublicKey, nonce)
}

// expectedReportData mirrors attestation.Service.buildReportData exactly —
// SEC-102 length-prefixed concatenation. Must stay in lock-step with the
// enclave-side implementation or every signed report fails verification.
func expectedReportData(tlsFingerprint, e2ePublicKey, signingPublicKey string, nonce []byte) string {
	h := sha256.New()
	writeLenPrefixed(h, []byte(tlsFingerprint))
	writeLenPrefixed(h, []byte(e2ePublicKey))
	writeLenPrefixed(h, []byte(signingPublicKey))
	writeLenPrefixed(h, nonce)
	return hex.EncodeToString(h.Sum(nil))
}

func writeLenPrefixed(h hash.Hash, b []byte) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(b)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(b)
}

func lowerHex(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'F' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}

// constantTimeHexEqual compares two hex strings in constant time wrt length.
// Not strictly required (report_data is public), but consistent with the rest
// of the strict-verify pipeline.
func constantTimeHexEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		ac := a[i]
		bc := b[i]
		if ac >= 'A' && ac <= 'F' {
			ac += 'a' - 'A'
		}
		if bc >= 'A' && bc <= 'F' {
			bc += 'a' - 'A'
		}
		diff |= ac ^ bc
	}
	return diff == 0
}

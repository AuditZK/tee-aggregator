package reportverify

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/signing"
)

// helper: build a freshly signed report with optional attestation.
func buildSignedReport(t *testing.T, signer *signing.ReportSigner, tlsFP, e2ePK, measurement string, nonce []byte, attested, bound, vcek bool) *signing.SignedReport {
	t.Helper()
	if attested {
		signer.SetAttestation(&signing.EnclaveAttestation{
			Measurement:              measurement,
			ReportData:               expectedReportData(tlsFP, e2ePK, signer.PublicKey(), nonce),
			Platform:                 "sev-snp",
			Attested:                 true,
			ReportDataBoundToRequest: bound,
			VcekVerified:             vcek,
		})
	}
	input := &signing.ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Test",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.10,
		DataPoints:  100,
	}
	report, err := signer.Sign(input)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return report
}

func TestVerifier_HappyPath(t *testing.T) {
	signer := signing.MustNewReportSignerGenerate()
	tlsFP := hex.EncodeToString(sha256.New().Sum([]byte("tls-cert")))
	e2ePK := "-----BEGIN PUBLIC KEY-----\nAAAAAA\n-----END PUBLIC KEY-----\n"
	measurement := "12345abcdef0000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
	nonce := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	report := buildSignedReport(t, signer, tlsFP, e2ePK, measurement, nonce, true, true, true)

	v := &Verifier{
		ExpectedSigningPublicKey: signer.PublicKey(),
		AllowedMeasurements:      []string{measurement},
		ExpectedTLSFingerprint:   tlsFP,
		ExpectedE2EPublicKey:     e2ePK,
		Nonce:                    nonce,
		RequireSevSnp:            true,
	}
	if err := v.Verify(report); err != nil {
		t.Fatalf("happy path should verify, got: %v", err)
	}
}

func TestVerifier_RejectForgedSigner(t *testing.T) {
	honest := signing.MustNewReportSignerGenerate()
	forger := signing.MustNewReportSignerGenerate()

	tlsFP := "aa"
	e2ePK := "pem"
	measurement := "cafe"
	report := buildSignedReport(t, forger, tlsFP, e2ePK, measurement, nil, true, true, true)

	v := &Verifier{
		ExpectedSigningPublicKey: honest.PublicKey(),
		AllowedMeasurements:      []string{measurement},
		ExpectedTLSFingerprint:   tlsFP,
		ExpectedE2EPublicKey:     e2ePK,
		RequireSevSnp:            true,
	}
	err := v.Verify(report)
	if !errors.Is(err, ErrPublicKeyMismatch) {
		t.Fatalf("expected ErrPublicKeyMismatch, got: %v", err)
	}
}

func TestVerifier_RejectUnboundReportData(t *testing.T) {
	signer := signing.MustNewReportSignerGenerate()
	tlsFP := "aa"
	e2ePK := "pem"
	measurement := "cafe"

	// bound = false simulates snpguest --random fallback
	report := buildSignedReport(t, signer, tlsFP, e2ePK, measurement, nil, true, false, true)

	v := &Verifier{
		ExpectedSigningPublicKey: signer.PublicKey(),
		AllowedMeasurements:      []string{measurement},
		ExpectedTLSFingerprint:   tlsFP,
		ExpectedE2EPublicKey:     e2ePK,
		RequireSevSnp:            true,
	}
	err := v.Verify(report)
	if !errors.Is(err, ErrReportDataNotBound) {
		t.Fatalf("expected ErrReportDataNotBound, got: %v", err)
	}
}

func TestVerifier_RejectMeasurementOutsideAllowlist(t *testing.T) {
	signer := signing.MustNewReportSignerGenerate()
	tlsFP := "aa"
	e2ePK := "pem"
	measurement := "cafe"

	report := buildSignedReport(t, signer, tlsFP, e2ePK, measurement, nil, true, true, true)

	v := &Verifier{
		ExpectedSigningPublicKey: signer.PublicKey(),
		AllowedMeasurements:      []string{"deadbeef"}, // different
		ExpectedTLSFingerprint:   tlsFP,
		ExpectedE2EPublicKey:     e2ePK,
		RequireSevSnp:            true,
	}
	err := v.Verify(report)
	if !errors.Is(err, ErrMeasurementNotAllowed) {
		t.Fatalf("expected ErrMeasurementNotAllowed, got: %v", err)
	}
}

func TestVerifier_RejectReportDataTampered(t *testing.T) {
	signer := signing.MustNewReportSignerGenerate()
	tlsFP := "aa"
	e2ePK := "pem"
	measurement := "cafe"

	report := buildSignedReport(t, signer, tlsFP, e2ePK, measurement, nil, true, true, true)

	v := &Verifier{
		ExpectedSigningPublicKey: signer.PublicKey(),
		AllowedMeasurements:      []string{measurement},
		ExpectedTLSFingerprint:   "bb", // different
		ExpectedE2EPublicKey:     e2ePK,
		RequireSevSnp:            true,
	}
	err := v.Verify(report)
	if !errors.Is(err, ErrReportDataMismatch) {
		t.Fatalf("expected ErrReportDataMismatch, got: %v", err)
	}
}

func TestVerifier_VcekFallbackFailsWhenFlagFalse(t *testing.T) {
	signer := signing.MustNewReportSignerGenerate()
	tlsFP := "aa"
	e2ePK := "pem"
	measurement := "cafe"

	// vcek = false
	report := buildSignedReport(t, signer, tlsFP, e2ePK, measurement, nil, true, true, false)

	v := &Verifier{
		ExpectedSigningPublicKey: signer.PublicKey(),
		AllowedMeasurements:      []string{measurement},
		ExpectedTLSFingerprint:   tlsFP,
		ExpectedE2EPublicKey:     e2ePK,
		RequireSevSnp:            true,
	}
	err := v.Verify(report)
	if !errors.Is(err, ErrVcekUnverified) {
		t.Fatalf("expected ErrVcekUnverified, got: %v", err)
	}
}

type fakeVCEK struct {
	err error
}

func (f *fakeVCEK) VerifyVCEK(string) error { return f.err }

func TestVerifier_VCEKCheckerOverridesFlag(t *testing.T) {
	signer := signing.MustNewReportSignerGenerate()
	tlsFP := "aa"
	e2ePK := "pem"
	measurement := "cafe"

	// flag false but our external checker says OK
	report := buildSignedReport(t, signer, tlsFP, e2ePK, measurement, nil, true, true, false)

	v := &Verifier{
		ExpectedSigningPublicKey: signer.PublicKey(),
		AllowedMeasurements:      []string{measurement},
		ExpectedTLSFingerprint:   tlsFP,
		ExpectedE2EPublicKey:     e2ePK,
		RequireSevSnp:            true,
		VCEK:                     &fakeVCEK{},
	}
	if err := v.Verify(report); err != nil {
		t.Fatalf("VCEK checker ok should pass, got: %v", err)
	}

	v.VCEK = &fakeVCEK{err: errors.New("chain broken")}
	err := v.Verify(report)
	if err == nil {
		t.Fatal("VCEK checker returning err should fail Verify")
	}
}

func TestVerifier_NoSevSnp_OnlyChecksSignature(t *testing.T) {
	signer := signing.MustNewReportSignerGenerate()
	// no attestation set on signer — report.EnclaveAttestation is nil
	input := &signing.ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Test",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.10,
		DataPoints:  100,
	}
	report, _ := signer.Sign(input)

	v := &Verifier{
		ExpectedSigningPublicKey: signer.PublicKey(),
		RequireSevSnp:            false,
	}
	if err := v.Verify(report); err != nil {
		t.Fatalf("no-sev-snp path should accept, got: %v", err)
	}
}

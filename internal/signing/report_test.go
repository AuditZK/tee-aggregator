package signing

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"
)

func TestSignAndVerify(t *testing.T) {
	signer := MustNewReportSignerGenerate()

	input := &ReportInput{
		UserUID:          "user_abc1234567890",
		ReportName:       "Test Report",
		PeriodStart:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:        time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
		TotalReturn:      0.25,
		AnnualizedReturn: 0.25,
		SharpeRatio:      1.5,
		SortinoRatio:     2.0,
		CalmarRatio:      1.2,
		MaxDrawdown:      0.10,
		Volatility:       0.15,
		WinRate:          0.55,
		ProfitFactor:     1.8,
		DataPoints:       252,
		BaseCurrency:     "USD",
		BenchmarkUsed:    "SPY",
		Exchanges:        []string{"binance", "bybit"},
		ExchangeDetails: []ExchangeInfo{
			{Name: "binance", KYCLevel: "basic", IsPaper: false},
			{Name: "bybit", KYCLevel: "", IsPaper: true},
		},
	}

	report, err := signer.Sign(input)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}

	if report.ReportID == "" {
		t.Error("ReportID should not be empty")
	}
	if report.Signature == "" {
		t.Error("Signature should not be empty")
	}
	if report.ReportHash == "" {
		t.Error("ReportHash should not be empty")
	}
	if report.PublicKey == "" {
		t.Error("PublicKey should not be empty")
	}
	if report.SignatureAlgorithm != "ECDSA-P256-SHA256" {
		t.Errorf("SignatureAlgorithm = %s, want ECDSA-P256-SHA256", report.SignatureAlgorithm)
	}
	if len(report.ExchangeDetails) != 2 {
		t.Fatalf("ExchangeDetails count = %d, want 2", len(report.ExchangeDetails))
	}
	if report.ExchangeDetails[1].Name != "bybit" || !report.ExchangeDetails[1].IsPaper {
		t.Fatalf("unexpected exchange detail: %+v", report.ExchangeDetails[1])
	}
	if _, err := base64.StdEncoding.DecodeString(report.PublicKey); err != nil {
		t.Fatalf("PublicKey should be base64 DER, decode failed: %v", err)
	}

	// Verify the signature
	valid, err := Verify(report.ReportHash, report.Signature, report.PublicKey, SignatureAlgorithm)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !valid {
		t.Error("Verify() = false, want true")
	}
}

func TestVerifyTamperedHash(t *testing.T) {
	signer := MustNewReportSignerGenerate()

	input := &ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Test",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.10,
		DataPoints:  100,
	}

	report, _ := signer.Sign(input)

	// Tamper with the hash
	tampered := "aa" + report.ReportHash[2:]
	valid, err := Verify(tampered, report.Signature, report.PublicKey, SignatureAlgorithm)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if valid {
		t.Error("Verify() should return false for tampered hash")
	}
}

func TestVerifyWrongPublicKey(t *testing.T) {
	signer1 := MustNewReportSignerGenerate()
	signer2 := MustNewReportSignerGenerate()

	input := &ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Test",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.10,
		DataPoints:  100,
	}

	report, _ := signer1.Sign(input)

	// Verify with wrong public key
	valid, err := Verify(report.ReportHash, report.Signature, signer2.PublicKey(), SignatureAlgorithm)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if valid {
		t.Error("Verify() should return false for wrong public key")
	}
}

func TestSignWithAttestation(t *testing.T) {
	signer := MustNewReportSignerGenerate()

	att := &EnclaveAttestation{
		Measurement: "12068361369cf9179bb6ac08572b7e15ed0bc8abb698cb04d4f584f7ff512a4c2081c1f5b105351dbd45c035a7d6a3a5",
		ReportData:  "deadbeef0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
		Platform:    "sev-snp",
		Attested:    true,
	}
	signer.SetAttestation(att)

	input := &ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Attested Report",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.25,
		DataPoints:  252,
	}

	report, err := signer.Sign(input)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}

	if report.EnclaveAttestation == nil {
		t.Fatal("EnclaveAttestation should be populated")
	}
	if report.EnclaveAttestation.Measurement != att.Measurement {
		t.Errorf("measurement = %s, want %s", report.EnclaveAttestation.Measurement, att.Measurement)
	}
	if report.EnclaveAttestation.Platform != "sev-snp" {
		t.Errorf("platform = %s, want sev-snp", report.EnclaveAttestation.Platform)
	}
	if !report.EnclaveAttestation.Attested {
		t.Error("Attested should be true")
	}
	if report.PayloadVersion != PayloadVersion {
		t.Errorf("PayloadVersion = %s, want %s", report.PayloadVersion, PayloadVersion)
	}

	valid, err := Verify(report.ReportHash, report.Signature, report.PublicKey, SignatureAlgorithm)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !valid {
		t.Error("signature should verify with attestation included")
	}

	// Tampering with the measurement after signing changes what the payload
	// hash SHOULD be. Rebuild the payload with a forged measurement, recompute
	// the hash, and confirm it no longer matches the stored report hash.
	forged := *report
	forged.EnclaveAttestation = &EnclaveAttestation{
		Measurement: "ff" + att.Measurement[2:],
		ReportData:  att.ReportData,
		Platform:    att.Platform,
		Attested:    true,
	}
	forgedPayload := buildFinancialPayload(&forged)
	forgedJSON, err := marshalSortedJSON(forgedPayload)
	if err != nil {
		t.Fatalf("marshalSortedJSON error = %v", err)
	}
	forgedHash := sha256.Sum256(forgedJSON)
	if hex.EncodeToString(forgedHash[:]) == report.ReportHash {
		t.Error("forged measurement produced identical hash — measurement is NOT in the signed payload")
	}
}

func TestVerifyReportEndToEnd(t *testing.T) {
	signer := MustNewReportSignerGenerate()
	signer.SetAttestation(&EnclaveAttestation{
		Measurement: "12068361369cf9179bb6ac08572b7e15",
		ReportData:  "deadbeef",
		Platform:    "sev-snp",
		Attested:    true,
	})

	input := &ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "E2E",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.42,
		SharpeRatio: 1.3,
		DataPoints:  252,
	}

	report, err := signer.Sign(input)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}

	valid, err := VerifyReport(report)
	if err != nil {
		t.Fatalf("VerifyReport() error = %v", err)
	}
	if !valid {
		t.Fatal("VerifyReport() should return true for an untampered report")
	}

	// Tamper with a signed field — measurement — after signing. VerifyReport
	// must detect this because the hash is recomputed from the payload.
	tampered := *report
	tampered.EnclaveAttestation = &EnclaveAttestation{
		Measurement: "ff068361369cf9179bb6ac08572b7e15",
		ReportData:  "deadbeef",
		Platform:    "sev-snp",
		Attested:    true,
	}
	valid, err = VerifyReport(&tampered)
	if err != nil {
		t.Fatalf("VerifyReport(tampered) error = %v", err)
	}
	if valid {
		t.Error("VerifyReport() should return false when measurement is tampered")
	}

	// Tamper with total return
	tampered2 := *report
	tampered2.TotalReturn = 99.99
	valid, err = VerifyReport(&tampered2)
	if err != nil {
		t.Fatalf("VerifyReport(tampered2) error = %v", err)
	}
	if valid {
		t.Error("VerifyReport() should return false when TotalReturn is tampered")
	}
}

func TestSignWithoutAttestation(t *testing.T) {
	signer := MustNewReportSignerGenerate()

	input := &ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Unattested Report",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.25,
		DataPoints:  252,
	}

	report, err := signer.Sign(input)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}

	if report.EnclaveAttestation != nil {
		t.Error("EnclaveAttestation should be nil when not set")
	}

	valid, err := Verify(report.ReportHash, report.Signature, report.PublicKey, SignatureAlgorithm)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !valid {
		t.Error("signature should still verify without attestation")
	}
}

func TestReportContainsExtendedData(t *testing.T) {
	signer := MustNewReportSignerGenerate()

	input := &ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Full Report",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.25,
		DataPoints:  252,
		DailyReturns: []DailyReturn{
			{Date: "2025-01-02", NetReturn: 0.01, NAV: 1.01},
			{Date: "2025-01-03", NetReturn: -0.005, NAV: 1.005},
		},
		MonthlyReturns: []MonthlyReturn{
			{Date: "2025-01", NetReturn: 0.05, AUM: 1.05},
		},
		RiskMetrics: &RiskMetrics{
			VaR95: 0.02, VaR99: 0.04, ExpectedShortfall: 0.05,
		},
		DrawdownData: &DrawdownData{
			CurrentDrawdown: 0.03,
			Periods: []*DrawdownPeriod{
				{StartDate: "2025-03-01", EndDate: "2025-03-15", Depth: 0.05, Duration: 14, Recovered: true},
			},
		},
		BenchmarkMetrics: &BenchmarkMetrics{
			BenchmarkName: "SPY", Alpha: 0.05, Beta: 0.8,
		},
	}

	report, err := signer.Sign(input)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}

	if len(report.DailyReturns) != 2 {
		t.Errorf("DailyReturns count = %d, want 2", len(report.DailyReturns))
	}
	if len(report.MonthlyReturns) != 1 {
		t.Errorf("MonthlyReturns count = %d, want 1", len(report.MonthlyReturns))
	}
	if report.RiskMetrics == nil {
		t.Error("RiskMetrics should not be nil")
	}
	if report.DrawdownData == nil {
		t.Error("DrawdownData should not be nil")
	}
	if report.BenchmarkMetrics == nil {
		t.Error("BenchmarkMetrics should not be nil")
	}
}

// TestVerifyReportStrict_KeyPinning checks that VerifyReportStrict refuses a
// report whose embedded PublicKey does not match the expected enclave key
// (SEC-109). Without this helper a self-consistent forgery (attacker signs
// with their own key and embeds it) would pass VerifyReport.
func TestVerifyReportStrict_KeyPinning(t *testing.T) {
	enclaveSigner := MustNewReportSignerGenerate()
	attackerSigner := MustNewReportSignerGenerate()

	input := &ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Test",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.10,
		DataPoints:  100,
	}

	// Attacker produces a self-consistent forgery.
	forged, _ := attackerSigner.Sign(input)

	// Strict verify with the legitimate enclave key must refuse it.
	valid, err := VerifyReportStrict(forged, enclaveSigner.PublicKey(), false)
	if valid {
		t.Fatal("VerifyReportStrict should refuse a forgery under a different signer key")
	}
	if err != ErrPublicKeyMismatch {
		t.Fatalf("expected ErrPublicKeyMismatch, got %v", err)
	}

	// A legitimately-signed report should pass.
	legit, _ := enclaveSigner.Sign(input)
	valid, err = VerifyReportStrict(legit, enclaveSigner.PublicKey(), false)
	if err != nil {
		t.Fatalf("VerifyReportStrict returned error on legit report: %v", err)
	}
	if !valid {
		t.Fatal("VerifyReportStrict should accept a legit report")
	}
}

// TestVerify_NoAlgorithmConfusion checks that passing an explicit Ed25519
// algorithm with ECDSA inputs does NOT accidentally succeed (SEC-108).
func TestVerify_NoAlgorithmConfusion(t *testing.T) {
	signer := MustNewReportSignerGenerate()
	input := &ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Test",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.10,
		DataPoints:  100,
	}
	report, _ := signer.Sign(input)

	// Claiming Ed25519 over ECDSA inputs must fail, not fall through.
	valid, err := Verify(report.ReportHash, report.Signature, report.PublicKey, "Ed25519")
	if valid {
		t.Fatal("Verify must not accept ECDSA output under Ed25519 algorithm label")
	}
	if err == nil {
		t.Fatal("Verify must return a decode error, not nil")
	}
}

package signing

import (
	"encoding/base64"
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
	valid, err := Verify(report.ReportHash, report.Signature, report.PublicKey)
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
	valid, err := Verify(tampered, report.Signature, report.PublicKey)
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
	valid, err := Verify(report.ReportHash, report.Signature, signer2.PublicKey())
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if valid {
		t.Error("Verify() should return false for wrong public key")
	}
}

func TestSignFromSeed(t *testing.T) {
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}

	signer1, err := NewReportSigner(seed)
	if err != nil {
		t.Fatalf("NewReportSigner() error = %v", err)
	}

	signer2, err := NewReportSigner(seed)
	if err != nil {
		t.Fatalf("NewReportSigner() error = %v", err)
	}

	if signer1.PublicKey() != signer2.PublicKey() {
		t.Error("same seed should produce same public key")
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

package grpc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/signing"
	"go.uber.org/zap"
)

func signedTestReport(t *testing.T) *signing.SignedReport {
	t.Helper()
	report, err := signing.MustNewReportSignerGenerate().Sign(&signing.ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Synthetic <&> report",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.125,
		SharpeRatio: 1.5,
		DataPoints:  3,
		DailyReturns: []signing.DailyReturn{
			{Date: "2025-01-02", NetReturn: 0.0000001, NAV: 1000},
			{Date: "2025-01-03", NetReturn: -0.01, NAV: 990},
		},
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return report
}

func TestSignedPayloadHashesToReportHash(t *testing.T) {
	s := &Server{logger: zap.NewNop()}
	report := signedTestReport(t)

	payload := s.signedPayload(report)
	if payload == "" {
		t.Fatal("signed payload is empty")
	}
	sum := sha256.Sum256([]byte(payload))
	if got := hex.EncodeToString(sum[:]); got != report.ReportHash {
		t.Fatalf("sha256(payload) = %s, report hash %s", got, report.ReportHash)
	}

	var decoded struct {
		ReportName string `json:"reportName"`
		Metrics    struct {
			TotalReturn float64 `json:"totalReturn"`
		} `json:"metrics"`
	}
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	if decoded.Metrics.TotalReturn != report.TotalReturn || decoded.ReportName != report.ReportName {
		t.Fatalf("payload figures do not match the report: %+v", decoded)
	}
}

func TestSignedPayloadEmptyWhenReportWasAltered(t *testing.T) {
	s := &Server{logger: zap.NewNop()}
	report := signedTestReport(t)
	report.TotalReturn = 0.5

	if payload := s.signedPayload(report); payload != "" {
		t.Fatal("a payload that no longer matches its hash was served")
	}
}

func TestPercentSignedPayloadHashesToPercentHash(t *testing.T) {
	s := &Server{logger: zap.NewNop()}
	report := signedTestReport(t)

	payload := s.percentSignedPayload(report)
	if payload == "" {
		t.Fatal("percent payload is empty")
	}
	sum := sha256.Sum256([]byte(payload))
	if got := hex.EncodeToString(sum[:]); got != report.PercentReportHash {
		t.Fatalf("sha256(percent payload) = %s, percent hash %s", got, report.PercentReportHash)
	}
	ok, err := signing.Verify(report.PercentReportHash, report.PercentSignature, report.PublicKey, report.SignatureAlgorithm)
	if err != nil || !ok {
		t.Fatalf("percent signature does not verify: ok=%v err=%v", ok, err)
	}
}

func TestPercentSignedPayloadEmptyWhenReportWasAltered(t *testing.T) {
	s := &Server{logger: zap.NewNop()}
	report := signedTestReport(t)
	report.TotalReturn = 0.5

	if payload := s.percentSignedPayload(report); payload != "" {
		t.Fatal("a percent payload that no longer matches its hash was served")
	}
}

func TestPercentSignedPayloadEmptyForReportSignedBeforeIt(t *testing.T) {
	s := &Server{logger: zap.NewNop()}
	report := signedTestReport(t)
	report.PercentReportHash, report.PercentSignature = "", ""

	if payload := s.percentSignedPayload(report); payload != "" {
		t.Fatal("a percent payload was served without a percent signature")
	}
}

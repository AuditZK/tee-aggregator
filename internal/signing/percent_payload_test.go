package signing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func signedReportWithAmounts(t *testing.T) *SignedReport {
	t.Helper()
	report, err := MustNewReportSignerGenerate().Sign(&ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Synthetic report",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.042,
		SharpeRatio: 1.3,
		MaxDrawdown: -0.08,
		DataPoints:  3,
		DailyReturns: []DailyReturn{
			{Date: "2025-01-02", NetReturn: 0.01, CumulativeReturn: 0.01, NAV: 123456.78, VerifiabilityClass: VerifiabilityClassLive},
			{Date: "2025-02-03", NetReturn: -0.005, CumulativeReturn: 0.005, NAV: 122839.5, VerifiabilityClass: VerifiabilityClassLive},
		},
		MonthlyReturns: []MonthlyReturn{
			{Date: "2025-01", NetReturn: 0.01, AUM: 123456.78},
			{Date: "2025-02", NetReturn: -0.005, AUM: 122839.5},
		},
		DrawdownData: &DrawdownData{CurrentDrawdown: -0.005},
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return report
}

func TestPercentPayloadWithholdsEveryAmount(t *testing.T) {
	report := signedReportWithAmounts(t)

	payload, err := PercentPayload(report)
	if err != nil {
		t.Fatalf("percent payload: %v", err)
	}
	for _, amount := range []string{"123456.78", "122839.5", `"nav"`, `"aum"`} {
		if strings.Contains(string(payload), amount) {
			t.Fatalf("percent payload carries %s: %s", amount, payload)
		}
	}

	var decoded struct {
		AmountsWithheld bool   `json:"amountsWithheld"`
		ReportID        string `json:"reportId"`
		Metrics         struct {
			TotalReturn float64 `json:"totalReturn"`
		} `json:"metrics"`
		DailyReturns   []map[string]any `json:"dailyReturns"`
		MonthlyReturns []map[string]any `json:"monthlyReturns"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("percent payload is not JSON: %v", err)
	}
	if !decoded.AmountsWithheld {
		t.Fatal("percent payload does not say its amounts are withheld")
	}
	if decoded.ReportID != report.ReportID || decoded.Metrics.TotalReturn != report.TotalReturn {
		t.Fatalf("percent payload lost the report's figures: %+v", decoded)
	}
	if len(decoded.DailyReturns) != 2 || decoded.DailyReturns[1]["netReturn"] != -0.005 {
		t.Fatalf("percent payload lost the daily returns: %+v", decoded.DailyReturns)
	}
	if len(decoded.MonthlyReturns) != 2 {
		t.Fatalf("percent payload lost the monthly returns: %+v", decoded.MonthlyReturns)
	}
}

func TestPercentPayloadIsSigned(t *testing.T) {
	report := signedReportWithAmounts(t)

	payload, err := PercentPayload(report)
	if err != nil {
		t.Fatalf("percent payload: %v", err)
	}
	sum := sha256.Sum256(payload)
	if got := hex.EncodeToString(sum[:]); got != report.PercentReportHash {
		t.Fatalf("sha256(percent payload) = %s, percent hash %s", got, report.PercentReportHash)
	}
	if report.PercentReportHash == report.ReportHash {
		t.Fatal("the percent view shares the full report's hash")
	}

	ok, err := Verify(report.PercentReportHash, report.PercentSignature, report.PublicKey, report.SignatureAlgorithm)
	if err != nil || !ok {
		t.Fatalf("percent signature does not verify: ok=%v err=%v", ok, err)
	}
	if ok, err := VerifyPercentReport(report); err != nil || !ok {
		t.Fatalf("VerifyPercentReport: ok=%v err=%v", ok, err)
	}
	if ok, err := VerifyReport(report); err != nil || !ok {
		t.Fatalf("the full report no longer verifies: ok=%v err=%v", ok, err)
	}
}

func TestVerifyPercentReportRejectsAlteredFigures(t *testing.T) {
	report := signedReportWithAmounts(t)
	report.DailyReturns[0].NetReturn = 0.02

	if ok, _ := VerifyPercentReport(report); ok {
		t.Fatal("an altered daily return still verifies under the percent signature")
	}
}

func TestPercentSignatureDoesNotCoverAmounts(t *testing.T) {
	report := signedReportWithAmounts(t)
	report.DailyReturns[0].NAV = 1

	if ok, _ := VerifyPercentReport(report); !ok {
		t.Fatal("the percent view depends on an amount it withholds")
	}
	if ok, _ := VerifyReport(report); ok {
		t.Fatal("an altered NAV still verifies under the full signature")
	}
}

func TestVerifyPercentReportWithoutPercentSignature(t *testing.T) {
	report := signedReportWithAmounts(t)
	report.PercentReportHash, report.PercentSignature = "", ""

	if ok, err := VerifyPercentReport(report); ok || err != nil {
		t.Fatalf("a report signed before the percent view verified: ok=%v err=%v", ok, err)
	}
}

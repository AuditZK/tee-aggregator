package signing

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func f64(v float64) *float64 { return &v }
func i(v int) *int           { return &v }

func reportWithActivity(t *testing.T, version string) *SignedReport {
	t.Helper()
	report, err := MustNewReportSignerGenerate().Sign(&ReportInput{
		UserUID:     "user_abc1234567890",
		ReportName:  "Synthetic report",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC),
		TotalReturn: 0.01,
		DataPoints:  2,
		DailyReturns: []DailyReturn{
			{
				Date: "2025-01-02", NetReturn: 0.01, CumulativeReturn: 0.01, NAV: 1.01,
				VerifiabilityClass: VerifiabilityClassLive,
				Committed:          f64(0.42), Turnover: f64(0.18), Trades: i(7),
				Volume: f64(98765.43), TradingFees: f64(34.56), FundingFees: f64(-12.34),
			},
			{
				// a day rebuilt from broker statements: nothing about its trades
				Date: "2025-01-03", NetReturn: 0, CumulativeReturn: 0.01, NAV: 1.01,
				VerifiabilityClass: VerifiabilityClassInEnclave,
				Committed:          f64(0.4),
			},
		},
		DrawdownData: &DrawdownData{},
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if version != "" {
		report.PayloadVersion = version
	}
	return report
}

func dailyOf(t *testing.T, payload []byte) []map[string]any {
	t.Helper()
	var decoded struct {
		DailyReturns []map[string]any `json:"dailyReturns"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	return decoded.DailyReturns
}

func TestPayloadSignsEachDaysActivity(t *testing.T) {
	report := reportWithActivity(t, "")
	if report.PayloadVersion != "1.10" {
		t.Fatalf("payload version %q, want 1.10", report.PayloadVersion)
	}
	payload, err := CanonicalPayload(report)
	if err != nil {
		t.Fatalf("canonical payload: %v", err)
	}
	days := dailyOf(t, payload)

	first := days[0]
	for key, want := range map[string]float64{
		"committed": 0.42, "turnover": 0.18, "trades": 7,
		"volume": 98765.43, "tradingFees": 34.56, "fundingFees": -12.34,
	} {
		if got, ok := first[key].(float64); !ok || got != want {
			t.Fatalf("day 1 %s = %v, want %v", key, first[key], want)
		}
	}

	// An unknown figure is signed as null, present and empty, not left out
	// and not written as a zero.
	second := days[1]
	for _, key := range []string{"turnover", "trades", "volume", "tradingFees", "fundingFees"} {
		v, present := second[key]
		if !present || v != nil {
			t.Fatalf("day 2 %s = %v (present %v), want an explicit null", key, v, present)
		}
	}
	if second["committed"] != 0.4 {
		t.Fatalf("day 2 committed = %v, want 0.4", second["committed"])
	}
}

func TestPercentPayloadWithholdsTheDaysAmounts(t *testing.T) {
	report := reportWithActivity(t, "")
	payload, err := PercentPayload(report)
	if err != nil {
		t.Fatalf("percent payload: %v", err)
	}
	for _, amount := range []string{"98765.43", "34.56", "12.34", `"volume"`, `"tradingFees"`, `"fundingFees"`} {
		if strings.Contains(string(payload), amount) {
			t.Fatalf("percent payload carries %s: %s", amount, payload)
		}
	}
	// The shares and the count say how the account behaved, not how large it is.
	first := dailyOf(t, payload)[0]
	if first["committed"] != 0.42 || first["turnover"] != 0.18 || first["trades"] != float64(7) {
		t.Fatalf("percent payload lost the shares: %v", first)
	}
	ok, err := VerifyPercentReport(report)
	if err != nil || !ok {
		t.Fatalf("percent report does not verify: %v %v", ok, err)
	}
}

// A report signed at 1.9 must reproduce its hash byte for byte, so its days
// carry none of the new keys even when the struct holds them.
func TestPayloadBefore110KeepsItsShape(t *testing.T) {
	report := reportWithActivity(t, "1.9")
	payload, err := CanonicalPayload(report)
	if err != nil {
		t.Fatalf("canonical payload: %v", err)
	}
	for _, key := range []string{"committed", "turnover", "trades", "volume", "tradingFees", "fundingFees"} {
		if _, present := dailyOf(t, payload)[0][key]; present {
			t.Fatalf("a 1.9 payload carries %s", key)
		}
	}
}

func TestReportSignedAt110Verifies(t *testing.T) {
	report := reportWithActivity(t, "")
	ok, err := VerifyReport(report)
	if err != nil || !ok {
		t.Fatalf("report does not verify: %v %v", ok, err)
	}
	// and an altered day breaks it
	v := 0.99
	report.DailyReturns[0].Committed = &v
	ok, _ = VerifyReport(report)
	if ok {
		t.Fatal("a report whose committed share was altered still verifies")
	}
}

package signing

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

// TestMarshalSortedJSONMatchesReference is the non-regression test for
// PERF-001. The fast marshalSortedJSON must produce byte-for-byte the
// same output as the legacy double-roundtrip implementation, otherwise
// the report hash changes and every cached signed report becomes
// unverifiable.
//
// The fixture covers every payload shape produced by buildFinancialPayload:
//   - empty / minimal
//   - full report with all optional sections (risk, benchmark, drawdown)
//   - very long daily/monthly arrays
//   - unicode strings
//   - integer + float numeric fields
//   - nil and zero-value optional sections
func TestMarshalSortedJSONMatchesReference(t *testing.T) {
	cases := []struct {
		name string
		fn   func() any
	}{
		{
			name: "empty_map",
			fn:   func() any { return map[string]any{} },
		},
		{
			name: "minimal_signed_report",
			fn: func() any {
				signer := MustNewReportSignerGenerate()
				report, err := signer.Sign(&ReportInput{
					UserUID:      "u1",
					ReportName:   "r",
					PeriodStart:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					PeriodEnd:    time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC),
					DataPoints:   30,
					BaseCurrency: "USD",
				})
				if err != nil {
					t.Fatal(err)
				}
				return buildFinancialPayload(report)
			},
		},
		{
			name: "full_signed_report",
			fn: func() any {
				signer := MustNewReportSignerGenerate()
				signer.SetAttestation(&EnclaveAttestation{
					Measurement:              "abcd",
					ReportData:               "deadbeef",
					Platform:                 "sev-snp",
					Attested:                 true,
					ReportDataBoundToRequest: true,
					VcekVerified:             true,
				})
				dailyReturns := make([]DailyReturn, 30)
				for i := range dailyReturns {
					dailyReturns[i] = DailyReturn{
						Date:             time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i).Format("2006-01-02"),
						NetReturn:        0.001 * float64(i),
						BenchmarkReturn:  0.0008 * float64(i),
						Outperformance:   0.0002 * float64(i),
						CumulativeReturn: 0.05 + 0.001*float64(i),
						NAV:              1.0 + 0.001*float64(i),
					}
				}
				monthlyReturns := []MonthlyReturn{
					{Date: "2025-01", NetReturn: 0.03, BenchmarkReturn: 0.02, Outperformance: 0.01, AUM: 1_000_000.5},
				}
				report, err := signer.Sign(&ReportInput{
					UserUID:          "user_abc",
					ReportName:       "Q1 2025 perf",
					PeriodStart:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					PeriodEnd:        time.Date(2025, 3, 31, 0, 0, 0, 0, time.UTC),
					TotalReturn:      0.42,
					AnnualizedReturn: 0.31,
					SharpeRatio:      1.5,
					SortinoRatio:     2.1,
					CalmarRatio:      0.9,
					MaxDrawdown:      -0.18,
					Volatility:       0.22,
					WinRate:          0.6,
					ProfitFactor:     1.8,
					DataPoints:       len(dailyReturns),
					BaseCurrency:     "USD",
					BenchmarkUsed:    "SPY",
					Exchanges:        []string{"binance", "ibkr", "kraken"},
					ExchangeDetails: []ExchangeInfo{
						{Name: "binance", KYCLevel: "basic", IsPaper: false},
						{Name: "ibkr", KYCLevel: "advanced", IsPaper: false},
						{Name: "kraken", KYCLevel: "", IsPaper: true},
					},
					DailyReturns:   dailyReturns,
					MonthlyReturns: monthlyReturns,
					RiskMetrics: &RiskMetrics{
						VaR95:             -0.02,
						VaR99:             -0.04,
						ExpectedShortfall: -0.05,
						Skewness:          -0.3,
						Kurtosis:          3.5,
					},
					DrawdownData: &DrawdownData{
						CurrentDrawdown:     -0.05,
						MaxDrawdownDuration: 12,
						Periods: []*DrawdownPeriod{
							{StartDate: "2025-02-10", EndDate: "2025-02-22", Depth: -0.18, Duration: 12, Recovered: true},
						},
					},
					BenchmarkMetrics: &BenchmarkMetrics{
						BenchmarkName:    "SPY",
						BenchmarkReturn:  0.10,
						Alpha:            0.05,
						Beta:             1.1,
						InformationRatio: 0.8,
						TrackingError:    0.04,
						Correlation:      0.9,
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				return buildFinancialPayload(report)
			},
		},
		{
			name: "unicode_strings",
			fn: func() any {
				return map[string]any{
					"reportName": "rapport perf — Q1 2025 ✓",
					"manager":    "Jürgen Müller",
					"firm":       "Acme & Co.",
					"exchanges":  []string{"binance", "kraken", "🚀"},
				}
			},
		},
		{
			name: "nested_with_nils",
			fn: func() any {
				return map[string]any{
					"a": nil,
					"b": map[string]any{"c": nil, "d": 1.0},
					"e": []map[string]any{{"x": 1}, {"y": 2}},
				}
			},
		},
		{
			name: "numbers_int_and_float",
			fn: func() any {
				// PERF-001 sanity: int 365 and float64 365.0 must produce
				// the same JSON literal "365".
				return map[string]any{
					"asInt":   365,
					"asFloat": 365.0,
					"frac":    0.5,
					"neg":     -1.25,
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := tc.fn()
			fast, err := marshalSortedJSON(payload)
			if err != nil {
				t.Fatalf("marshalSortedJSON: %v", err)
			}
			ref, err := marshalSortedJSONReference(payload)
			if err != nil {
				t.Fatalf("marshalSortedJSONReference: %v", err)
			}
			if !bytes.Equal(fast, ref) {
				t.Errorf("output mismatch\nfast: %s\n ref: %s", fast, ref)
			}
		})
	}
}

// TestVerifiabilityClassInPayload pins the PayloadVersion-gated behaviour of
// the verifiabilityClass field on daily_returns. The contract is:
//
//   - Pre-1.3 reports (1.0/1.1/1.2) carry no verifiabilityClass in the
//     signed payload, so VerifyReport on legacy reports still reproduces
//     their hash byte-for-byte.
//   - 1.3+ reports always emit the key (even when empty), so a verifier
//     can rely on its presence to decide trust policy.
func TestVerifiabilityClassInPayload(t *testing.T) {
	dailyReturns := []DailyReturn{
		{
			Date:               "2026-01-01",
			NetReturn:          0.01,
			NAV:                100,
			VerifiabilityClass: VerifiabilityClassLive,
		},
		{
			Date:               "2026-01-02",
			NetReturn:          0.02,
			NAV:                101,
			VerifiabilityClass: VerifiabilityClassRebuilderService,
		},
	}

	cases := []struct {
		name           string
		payloadVersion string
		wantKey        bool
	}{
		{name: "legacy_1.2", payloadVersion: "1.2", wantKey: false},
		{name: "current_1.3", payloadVersion: "1.3", wantKey: true},
		{name: "empty_version_treated_as_legacy", payloadVersion: "", wantKey: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := toDailyReturnsPayload(dailyReturns, tc.payloadVersion)
			if len(payload) != len(dailyReturns) {
				t.Fatalf("got %d entries, want %d", len(payload), len(dailyReturns))
			}
			_, hasKey := payload[0]["verifiabilityClass"]
			if hasKey != tc.wantKey {
				t.Errorf("payloadVersion=%q: verifiabilityClass present=%v, want=%v", tc.payloadVersion, hasKey, tc.wantKey)
			}
			if tc.wantKey {
				if payload[0]["verifiabilityClass"] != VerifiabilityClassLive {
					t.Errorf("entry 0: got class=%v, want %q", payload[0]["verifiabilityClass"], VerifiabilityClassLive)
				}
				if payload[1]["verifiabilityClass"] != VerifiabilityClassRebuilderService {
					t.Errorf("entry 1: got class=%v, want %q", payload[1]["verifiabilityClass"], VerifiabilityClassRebuilderService)
				}
			}
		})
	}
}

// TestVerifiabilityClassSignAndVerifyRoundtrip pins the end-to-end signature
// invariant for 1.3 reports: a verifier MUST reject any tampering with the
// verifiabilityClass field (since it's part of the canonical payload).
func TestVerifiabilityClassSignAndVerifyRoundtrip(t *testing.T) {
	signer := MustNewReportSignerGenerate()
	signer.SetAttestation(&EnclaveAttestation{
		Platform:                 "sev-snp",
		Attested:                 true,
		ReportDataBoundToRequest: true,
		VcekVerified:             true,
	})

	report, err := signer.Sign(&ReportInput{
		UserUID:      "u1",
		ReportName:   "r",
		PeriodStart:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:    time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		DataPoints:   2,
		BaseCurrency: "USD",
		DailyReturns: []DailyReturn{
			{Date: "2026-01-01", NetReturn: 0.01, NAV: 100, VerifiabilityClass: VerifiabilityClassRebuilderService},
			{Date: "2026-01-02", NetReturn: 0.02, NAV: 101, VerifiabilityClass: VerifiabilityClassLive},
		},
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if report.PayloadVersion != "1.3" {
		t.Fatalf("expected new reports to carry PayloadVersion 1.3, got %q", report.PayloadVersion)
	}

	ok, err := VerifyReport(report)
	if err != nil || !ok {
		t.Fatalf("verify untampered report: ok=%v err=%v", ok, err)
	}

	// Tamper: swap the rebuilder-service class to live and re-verify. The
	// signature was computed over the canonical payload that includes the
	// class, so the hash recomputation must mismatch and VerifyReport must
	// return (false, nil).
	report.DailyReturns[0].VerifiabilityClass = VerifiabilityClassLive
	ok, err = VerifyReport(report)
	if err != nil {
		t.Fatalf("verify tampered report errored unexpectedly: %v", err)
	}
	if ok {
		t.Fatalf("tampered verifiabilityClass slipped past VerifyReport")
	}
}

// marshalSortedJSONReference is the legacy double-roundtrip implementation
// (Marshal → Unmarshal-into-any → writeSortedJSON). It exists only as the
// byte-for-byte oracle for TestMarshalSortedJSONMatchesReference — the
// production path is marshalSortedJSON.
func marshalSortedJSONReference(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := writeSortedJSON(&buf, normalized); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

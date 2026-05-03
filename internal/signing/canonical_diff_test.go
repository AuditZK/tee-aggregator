package signing

import (
	"bytes"
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

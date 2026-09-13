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
	if report.PayloadVersion != PayloadVersion {
		t.Fatalf("expected new reports to carry PayloadVersion %s, got %q", PayloadVersion, report.PayloadVersion)
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

// TestRiskFreeRateInPayload pins the PayloadVersion-gated behaviour of
// metrics.riskFreeRate. The contract is:
//
//   - Pre-1.4 reports carry no riskFreeRate in the signed metrics block, so
//     VerifyReport on legacy reports still reproduces their hash.
//   - 1.4+ reports always emit the key (even at 0), so a verifier can rely
//     on its presence to read the Sharpe/Sortino assumption.
func TestRiskFreeRateInPayload(t *testing.T) {
	cases := []struct {
		name           string
		payloadVersion string
		wantKey        bool
	}{
		{name: "legacy_1.3", payloadVersion: "1.3", wantKey: false},
		{name: "current_1.4", payloadVersion: "1.4", wantKey: true},
		{name: "empty_version_treated_as_legacy", payloadVersion: "", wantKey: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report := &SignedReport{PayloadVersion: tc.payloadVersion, RiskFreeRate: 2.5}
			payload := buildFinancialPayload(report)
			metrics := payload["metrics"].(map[string]any)
			rf, hasKey := metrics["riskFreeRate"]
			if hasKey != tc.wantKey {
				t.Errorf("payloadVersion=%q: riskFreeRate present=%v, want=%v", tc.payloadVersion, hasKey, tc.wantKey)
			}
			if tc.wantKey && rf != 2.5 {
				t.Errorf("riskFreeRate = %v, want 2.5", rf)
			}
		})
	}
}

// TestRiskFreeRateSignAndVerifyRoundtrip pins the signature invariant for
// 1.4 reports: tampering with the risk-free rate assumption must invalidate
// the signature, since it changes the meaning of the signed Sharpe/Sortino.
func TestRiskFreeRateSignAndVerifyRoundtrip(t *testing.T) {
	signer := MustNewReportSignerGenerate()
	report, err := signer.Sign(&ReportInput{
		UserUID:      "u",
		ReportName:   "r",
		PeriodStart:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:    time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		DataPoints:   2,
		BaseCurrency: "USD",
		RiskFreeRate: 2.5,
		SharpeRatio:  1.1,
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	ok, err := VerifyReport(report)
	if err != nil || !ok {
		t.Fatalf("verify untampered report: ok=%v err=%v", ok, err)
	}

	report.RiskFreeRate = 0
	ok, err = VerifyReport(report)
	if err != nil {
		t.Fatalf("verify tampered report errored unexpectedly: %v", err)
	}
	if ok {
		t.Fatalf("tampered riskFreeRate slipped past VerifyReport")
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

// TestReportNameInPayload pins the PayloadVersion-gated behaviour of
// reportName (SEC-14). Pre-1.5 reports omit it so VerifyReport still
// reproduces their hash; 1.5+ reports always emit it, so renaming a report
// invalidates its signature.
func TestReportNameInPayload(t *testing.T) {
	cases := []struct {
		payloadVersion string
		wantKey        bool
	}{
		{payloadVersion: "1.4", wantKey: false},
		{payloadVersion: "1.5", wantKey: true},
		{payloadVersion: "", wantKey: false},
	}

	for _, tc := range cases {
		t.Run("v"+tc.payloadVersion, func(t *testing.T) {
			report := &SignedReport{PayloadVersion: tc.payloadVersion, ReportName: "Demo account"}
			payload := buildFinancialPayload(report)
			name, hasKey := payload["reportName"]
			if hasKey != tc.wantKey {
				t.Fatalf("payloadVersion=%q: reportName present=%v, want=%v", tc.payloadVersion, hasKey, tc.wantKey)
			}
			if tc.wantKey && name != "Demo account" {
				t.Errorf("reportName = %v, want %q", name, "Demo account")
			}
		})
	}
}

// The whole point of SEC-14: a renamed report must stop verifying.
func TestRenamedReportFailsVerification(t *testing.T) {
	signer, err := NewReportSignerGenerate()
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	report, err := signer.Sign(&ReportInput{
		UserUID:     "uid-1",
		ReportName:  "Demo account — test",
		PeriodStart: time.Now().AddDate(0, -1, 0),
		PeriodEnd:   time.Now(),
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if ok, err := VerifyReport(report); err != nil || !ok {
		t.Fatalf("freshly signed report does not verify: ok=%v err=%v", ok, err)
	}

	report.ReportName = "Audited track record 2026"
	if ok, _ := VerifyReport(report); ok {
		t.Fatal("a renamed report still verified — reportName is outside the signature")
	}
}

// TestAnnualizationDaysInPayload pins the PayloadVersion-gated behaviour of
// metrics.annualizationDays. Same contract as riskFreeRate at 1.4:
//
//   - Pre-1.7 reports carry no annualizationDays in the signed metrics block,
//     so VerifyReport on already-issued reports still reproduces their hash.
//   - 1.7+ reports always emit the key, so a verifier can read the year
//     length that produced the ratios instead of assuming one.
func TestAnnualizationDaysInPayload(t *testing.T) {
	cases := []struct {
		name           string
		payloadVersion string
		wantKey        bool
	}{
		{name: "legacy_1.6", payloadVersion: "1.6", wantKey: false},
		{name: "current_1.7", payloadVersion: "1.7", wantKey: true},
		{name: "empty_version_treated_as_legacy", payloadVersion: "", wantKey: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report := &SignedReport{PayloadVersion: tc.payloadVersion, AnnualizationDays: AnnualizationDays}
			payload := buildFinancialPayload(report)
			metrics := payload["metrics"].(map[string]any)
			days, hasKey := metrics["annualizationDays"]
			if hasKey != tc.wantKey {
				t.Errorf("payloadVersion=%q: annualizationDays present=%v, want=%v", tc.payloadVersion, hasKey, tc.wantKey)
			}
			if tc.wantKey && days != 365 {
				t.Errorf("annualizationDays = %v, want 365", days)
			}
		})
	}
}

// TestAnnualizationDaysSignAndVerifyRoundtrip pins the signature invariant
// for 1.7 reports. Relabelling the basis rescales every signed ratio by
// sqrt(365/252) ~= 1.2 without changing a single number on the page, so it
// must invalidate the signature.
func TestAnnualizationDaysSignAndVerifyRoundtrip(t *testing.T) {
	signer := MustNewReportSignerGenerate()
	report, err := signer.Sign(&ReportInput{
		UserUID:      "u",
		ReportName:   "r",
		PeriodStart:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:    time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		DataPoints:   2,
		BaseCurrency: "USD",
		SharpeRatio:  1.1,
		Volatility:   0.22,
		BenchmarkMetrics: &BenchmarkMetrics{
			BenchmarkName: "SPY", Alpha: 0.05, Beta: 1.1,
			InformationRatio: 0.8, TrackingError: 0.04, Correlation: 0.9,
		},
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if report.PayloadVersion != "1.7" {
		t.Fatalf("new reports must be issued at payload 1.7, got %q", report.PayloadVersion)
	}
	if report.AnnualizationDays != 365 {
		t.Fatalf("annualization_days = %d, want 365", report.AnnualizationDays)
	}

	ok, err := VerifyReport(report)
	if err != nil || !ok {
		t.Fatalf("verify untampered 1.7 report: ok=%v err=%v", ok, err)
	}

	// Tamper: relabel the basis as the 252-day equity-market year.
	report.AnnualizationDays = 252
	ok, err = VerifyReport(report)
	if err != nil {
		t.Fatalf("verify tampered report errored unexpectedly: %v", err)
	}
	if ok {
		t.Fatal("a report whose declared annualization basis was swapped to 252 still verified")
	}
}

// legacyReport16JSON is a report actually issued at PayloadVersion 1.6,
// captured before annualizationDays entered the signed payload. It carries no
// annualization_days key at all, exactly like every report already stored in
// signed_reports. The bytes are frozen: if the pre-1.7 canonical shape ever
// drifts, this stops verifying and the reports in the database go with it.
const legacyReport16JSON = `{"report_id":"92418d52-7a78-49dd-ba01-526ea5e538d8","user_uid":"user_legacy_16","report_name":"Legacy 1.6 report","generated_at":"2026-09-13T20:12:05.891Z","period_start":"2025-01-01T00:00:00.000Z","period_end":"2025-12-31T00:00:00.000Z","total_return":0.42,"annualized_return":0.31,"annualized":true,"period_days":364,"sharpe_ratio":1.5,"sortino_ratio":2.1,"calmar_ratio":0.9,"max_drawdown":-0.18,"volatility":0.22,"win_rate":0.6,"profit_factor":1.8,"data_points":3,"base_currency":"USD","benchmark":"SPY","risk_free_rate":2.5,"exchanges":["binance","ibkr"],"exchange_details":[{"name":"binance","kyc_level":"basic","is_paper":false},{"name":"ibkr","kyc_level":"advanced","is_paper":false}],"daily_returns":[{"date":"2025-01-02","net_return":0.01,"benchmark_return":0.004,"outperformance":0.006,"cumulative_return":0.01,"nav":101,"verifiability_class":"live"},{"date":"2025-01-03","net_return":-0.005,"benchmark_return":0.001,"outperformance":-0.006,"cumulative_return":0.005,"nav":100.5,"verifiability_class":"rebuilder-service"}],"monthly_returns":[{"date":"2025-01","net_return":0.05,"benchmark_return":0.02,"outperformance":0.03,"aum":100.5}],"risk_metrics":{"var_95":-0.02,"var_99":-0.04,"expected_shortfall":-0.05,"skewness":-0.3,"kurtosis":3.5},"drawdown_data":{"current_drawdown":-0.05,"max_drawdown_duration":12,"periods":[{"start_date":"2025-02-10","end_date":"2025-02-22","depth":-0.18,"duration":12,"recovered":true}]},"benchmark_metrics":{"benchmark_name":"SPY","benchmark_return":0.1,"alpha":0.05,"beta":1.1,"information_ratio":0.8,"tracking_error":0.04,"correlation":0.9},"enclave_attestation":{"measurement":"9f1c2b","report_data":"deadbeef","platform":"sev-snp","attested":true,"report_data_bound_to_request":true,"vcek_verified":true},"signature":"MEUCIQDS8asS5gEq8xf+EO5Wzzyxl0Ik5+G6d/dwlm50T7ILjwIgcGNhtEOCCYOKzlCFsQ9ha9qLXdOpjZkdIOtnXZZwLeI=","public_key":"MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEdwV6CVHxHqMWXzcKTH4BX/4pp8Ru+5DmXnQbmsYHXAJZvxm9W/F4xKgA+6q2jXllTRAzUbmuNpLjpgKNUzYfOA==","signature_algorithm":"ECDSA-P256-SHA256","report_hash":"8683a6700b004253ac9f6aa26b9b7b780a853859f12747e2a0366d0a7d1b1efe","enclave_version":"1.1.2-go","payload_version":"1.6"}`

// A report issued at 1.6 must keep verifying byte-for-byte after 1.7 adds a
// field to the payload — that is the entire point of the legacy gate.
func TestLegacy16ReportStillVerifiesUnderPayload17(t *testing.T) {
	var report SignedReport
	if err := json.Unmarshal([]byte(legacyReport16JSON), &report); err != nil {
		t.Fatalf("unmarshal legacy fixture: %v", err)
	}
	if report.PayloadVersion != "1.6" {
		t.Fatalf("fixture payload_version = %q, want 1.6", report.PayloadVersion)
	}
	// A pre-1.7 report has no basis field, so it decodes to the zero value.
	// The gate must ignore it rather than sign a 0 into the payload.
	if report.AnnualizationDays != 0 {
		t.Fatalf("legacy fixture carries annualization_days = %d, want it absent", report.AnnualizationDays)
	}

	ok, err := VerifyReport(&report)
	if err != nil {
		t.Fatalf("verify legacy 1.6 report: %v", err)
	}
	if !ok {
		t.Fatal("a report issued at payload 1.6 no longer verifies — the 1.7 gate does not reproduce the old canonical shape")
	}

	// And the gate is keyed on the report's own version, not on the package
	// constant: the legacy report's payload must still omit the key.
	metrics := buildFinancialPayload(&report)["metrics"].(map[string]any)
	if _, present := metrics["annualizationDays"]; present {
		t.Fatal("the 1.6 canonical payload gained an annualizationDays key")
	}
}

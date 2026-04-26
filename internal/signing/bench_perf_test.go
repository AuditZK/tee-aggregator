package signing

import (
	"testing"
	"time"
)

// PERF-AUDIT: probe Sign() allocation cost. The hot loop is
// marshalSortedJSON(buildFinancialPayload(report)) which performs
// MarshalJSON → UnmarshalJSON → re-marshal-sorted on every call. The
// benchmarks below let the audit size the win from a structural sort
// (avoiding the unmarshal-into-any roundtrip).

func makeInputForBench() *ReportInput {
	dailyReturns := make([]DailyReturn, 365)
	for i := range dailyReturns {
		dailyReturns[i] = DailyReturn{
			Date:             time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i).Format("2006-01-02"),
			NetReturn:        0.001 * float64(i%30),
			BenchmarkReturn:  0.0008 * float64(i%30),
			Outperformance:   0.0002 * float64(i%30),
			CumulativeReturn: 0.05 + 0.001*float64(i),
			NAV:              1.0 + 0.001*float64(i),
		}
	}
	return &ReportInput{
		UserUID:          "u_perf",
		ReportName:       "perf",
		PeriodStart:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:        time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
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
		BenchmarkUsed:    "spy",
		Exchanges:        []string{"binance", "ibkr", "kraken"},
		DailyReturns:     dailyReturns,
	}
}

func BenchmarkSign_365Days(b *testing.B) {
	signer := MustNewReportSignerGenerate()
	in := makeInputForBench()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := signer.Sign(in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalSortedJSON_365Days(b *testing.B) {
	signer := MustNewReportSignerGenerate()
	in := makeInputForBench()
	report, err := signer.Sign(in)
	if err != nil {
		b.Fatal(err)
	}
	payload := buildFinancialPayload(report)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshalSortedJSON(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVerifyReport_365Days(b *testing.B) {
	signer := MustNewReportSignerGenerate()
	in := makeInputForBench()
	report, err := signer.Sign(in)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := VerifyReport(report); err != nil {
			b.Fatal(err)
		}
	}
}

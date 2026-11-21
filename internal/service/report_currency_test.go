package service

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func ymd(s string) time.Time {
	t, _ := time.Parse(dateFormat, s)
	return t
}

func eurOnly(byDay map[string]float64) fxRates {
	return fxRates{"EUR": byDay}
}

func usdRow(exchange, ts string, equity, deposits float64) *repository.Snapshot {
	return &repository.Snapshot{Exchange: exchange, Timestamp: ymd(ts), TotalEquity: equity, RealizedBalance: equity, Deposits: deposits}
}

// A row is valued at the rate of the day its equity was measured: the day
// before its date for a live sync at midnight, its own date for IBKR.
func TestSnapshotsInCurrencyValuesEachRowAtItsMeasuredDay(t *testing.T) {
	rates := eurOnly(map[string]float64{"2026-09-01": 1.10, "2026-09-02": 1.20})
	rows := []*repository.Snapshot{usdRow("bybit", "2026-09-02", 1100, 110), usdRow("ibkr", "2026-09-02", 1200, 0)}

	out, err := snapshotsInCurrency(rows, rates, "EUR", ymd("2026-09-03"))
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	near := func(a, b float64) bool { return math.Abs(a-b) < 1e-9 }
	if !near(out[0].TotalEquity, 1000) || !near(out[0].Deposits, 100) || !near(out[0].RealizedBalance, 1000) {
		t.Fatalf("bybit row = %+v, want valued at 1.10 (measured 09-01)", out[0])
	}
	if !near(out[1].TotalEquity, 1000) {
		t.Fatalf("ibkr row equity = %v, want 1000 (measured on its own date at 1.20)", out[1].TotalEquity)
	}
	if rows[0].TotalEquity != 1100 {
		t.Fatal("conversion mutated the stored snapshot")
	}
}

// A row converted from an account held in EUR carries the rate it was
// converted at: valuing it back in EUR returns its native figures exactly.
func TestSnapshotsInCurrencyGivesNativeRowsBackExactly(t *testing.T) {
	row := usdRow("ibkr", "2026-09-02", 1170, 0)
	row.Breakdown = &repository.MarketBreakdown{Global: &repository.MarketMetrics{NativeCurrency: "EUR", FXRateToUSD: 1.17}}

	out, err := snapshotsInCurrency([]*repository.Snapshot{row}, eurOnly(map[string]float64{"2026-09-02": 1.20}), "EUR", ymd("2026-09-03"))
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if math.Abs(out[0].TotalEquity-1000) > 1e-9 {
		t.Fatalf("equity = %v, want the native 1000", out[0].TotalEquity)
	}
}

// The last days lack a final rate until a later close exists: the report ends
// at the last day it can value, a day dropped whole across connections.
func TestSnapshotsInCurrencyDropsRecentDaysWithoutARate(t *testing.T) {
	rates := eurOnly(map[string]float64{"2026-09-01": 1.1, "2026-09-02": 1.1})
	rows := []*repository.Snapshot{
		usdRow("bybit", "2026-09-02", 100, 0),
		usdRow("bybit", "2026-09-03", 100, 0),
		usdRow("ibkr", "2026-09-03", 100, 0),
		usdRow("bybit", "2026-09-04", 100, 0),
	}

	out, err := snapshotsInCurrency(rows, rates, "EUR", ymd("2026-09-05"))
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if len(out) != 1 || !out[0].Timestamp.Equal(ymd("2026-09-02")) {
		t.Fatalf("kept %d rows, want only 09-02 (09-03 has an IBKR row without a rate)", len(out))
	}
}

// A day without a rate deep in the past is a hole in the series, not a day
// waiting for its close: the report is refused.
func TestSnapshotsInCurrencyRefusesAnOldHole(t *testing.T) {
	rates := eurOnly(map[string]float64{"2026-08-01": 1.1, "2026-08-03": 1.1})
	rows := []*repository.Snapshot{usdRow("bybit", "2026-08-02", 100, 0), usdRow("bybit", "2026-08-03", 100, 0), usdRow("bybit", "2026-08-04", 100, 0)}

	_, err := snapshotsInCurrency(rows, rates, "EUR", ymd("2026-09-20"))
	if err == nil || !strings.Contains(err.Error(), "no final exchange rate") {
		t.Fatalf("err = %v, want a missing-rate refusal", err)
	}
}

// On a day without flows the return in EUR is the USD return carried through
// the rate move: (1+r)·X(d-1)/X(d) - 1, the currency effect a EUR investor
// lives with.
func TestReturnsInCurrencyCarryTheRateMove(t *testing.T) {
	rates := eurOnly(map[string]float64{"2026-09-01": 1.10, "2026-09-02": 1.20})
	rows := []*repository.Snapshot{usdRow("bybit", "2026-09-02", 1000, 0), usdRow("bybit", "2026-09-03", 1100, 0)}

	out, err := snapshotsInCurrency(rows, rates, "EUR", ymd("2026-09-04"))
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	usd := convertSnapshotsToDailyReturns(rows)
	eur := convertSnapshotsToDailyReturns(out)
	last := func(d []dailyReturn) float64 { return d[len(d)-1].netReturn }
	want := (1+last(usd))*1.10/1.20 - 1
	if math.Abs(last(usd)-0.10) > 1e-9 || math.Abs(last(eur)-want) > 1e-9 {
		t.Fatalf("usd = %v, eur = %v, want 0.10 and %v", last(usd), last(eur), want)
	}
}

// Benchmark closes are valued at each close's own rate, a close without one
// left out rather than valued at a neighbour's.
func TestBenchPointsInValuesClosesAtTheirDay(t *testing.T) {
	rates := eurOnly(map[string]float64{"2026-09-01": 1.25, "2026-09-03": 1.0})
	got := benchPointsIn([]benchPoint{{"2026-09-01", 500}, {"2026-09-02", 510}, {"2026-09-03", 520}}, rates, "EUR")
	if len(got) != 2 || got[0].close != 400 || got[1].close != 520 {
		t.Fatalf("points = %+v, want 09-01 at 400 and 09-03 at 520", got)
	}
}

func TestReportCurrencyDefaultsToUSD(t *testing.T) {
	if reportCurrency("") != "USD" || reportCurrency(" eur ") != "EUR" {
		t.Fatal("currency normalization drifted")
	}
}

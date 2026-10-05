package service

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

func fxDay(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func fxNear(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

// EUR/USD over a week, plus one GBP day for a foreign-currency wire.
var testRates = fxRates{
	"EUR": {"2026-09-28": 1.10, "2026-09-29": 1.12, "2026-09-30": 1.15},
	"GBP": {"2026-09-29": 1.30},
}

func TestFiatToConvertLeavesDollarsAndStablecoinsAlone(t *testing.T) {
	for _, c := range []string{"", "USD", "usd", "USDT", "USDC", "XYZ"} {
		if _, ok := fiatToConvert(c); ok {
			t.Errorf("%q would be converted", c)
		}
	}
	if c, ok := fiatToConvert(" eur "); !ok || c != "EUR" {
		t.Errorf("EUR not converted: %q %v", c, ok)
	}
}

func TestFlowCurrencyTrustsOnlyARealCode(t *testing.T) {
	cf := func(c string) *connector.Cashflow { return &connector.Cashflow{Amount: 1, Currency: c} }
	if got := flowCurrency(cf("USD"), "EUR", true); got != "USD" {
		t.Errorf("IBKR USD wire read as %s", got)
	}
	// IBKR's base-currency summary rows say BASE_SUMMARY, not a code.
	if got := flowCurrency(cf("BASE_SUMMARY"), "EUR", true); got != "EUR" {
		t.Errorf("BASE_SUMMARY read as %s", got)
	}
	// cTrader and MetaTrader label every flow USD whatever the account.
	if got := flowCurrency(cf("USD"), "EUR", false); got != "EUR" {
		t.Errorf("untrusted label read as %s", got)
	}
}

func TestConvertFlowsUsesEachFlowsOwnDay(t *testing.T) {
	flows := []*connector.Cashflow{
		{Amount: 1000, Currency: "EUR", Timestamp: fxDay("2026-09-28").Add(10 * time.Hour)},
		{Amount: -200, Currency: "EUR", Timestamp: fxDay("2026-09-30").Add(9 * time.Hour)},
		{Amount: 500, Currency: "USD", Timestamp: fxDay("2026-09-29")},
		{Amount: 100, Currency: "GBP", Timestamp: fxDay("2026-09-29")},
	}
	dep, wd, missing := convertFlows(flows, "EUR", true, fxDay("2026-09-30"), testRates)
	if missing != "" {
		t.Fatalf("missing %s", missing)
	}
	fxNear(t, "deposits", dep, 1000*1.10+500+100*1.30)
	fxNear(t, "withdrawals", wd, 200*1.15)
}

func TestConvertFlowsAfterTheLatestRateTakeIt(t *testing.T) {
	// A flow a few seconds into today, read by the midnight sync, has no final
	// rate of its own yet; it takes the newest one rather than block the day.
	flows := []*connector.Cashflow{{Amount: 100, Timestamp: fxDay("2026-10-01").Add(time.Minute)}}
	dep, _, missing := convertFlows(flows, "EUR", false, fxDay("2026-09-30"), testRates)
	if missing != "" {
		t.Fatalf("missing %s", missing)
	}
	fxNear(t, "deposits", dep, 115)
}

func TestApplyLiveFXConvertsBalancesAndRebuildsFlows(t *testing.T) {
	balance := &connector.Balance{Equity: 10000, Available: 4000, UnrealizedPnL: -300, Currency: "EUR"}
	act := liveActivity{breakdown: &aggregatedBreakdown{}, deposits: 999, cashflows: []*connector.Cashflow{
		{Amount: 1000, Currency: "USD", Timestamp: fxDay("2026-09-29")},
	}}
	stocks := act.breakdown.getOrCreateMarket(connector.MarketStocks)
	stocks.equity, stocks.availableMargin, stocks.volume = 6000, 4000, 777

	fx, missing := applyLiveFX(balance, &act, "EUR", true, fxDay("2026-09-30"), testRates)
	if missing != "" {
		t.Fatalf("missing %s", missing)
	}
	if fx.currency != "EUR" || fx.rate != 1.15 {
		t.Errorf("fx = %+v", fx)
	}
	fxNear(t, "equity", balance.Equity, 11500)
	fxNear(t, "available", balance.Available, 4600)
	fxNear(t, "unrealized", balance.UnrealizedPnL, -345)
	if balance.Currency != "USD" {
		t.Errorf("currency left at %s", balance.Currency)
	}
	fxNear(t, "stocks equity", stocks.equity, 6900)
	fxNear(t, "stocks margin", stocks.availableMargin, 4600)
	// Activity keeps the connector's units: a balance rate is wrong for it.
	fxNear(t, "stocks volume", stocks.volume, 777)
	// Rebuilt from the flows, not the summed native amount.
	fxNear(t, "deposits", act.deposits, 1000)
}

func TestApplyLiveFXLeavesEverythingWhenTheRateIsMissing(t *testing.T) {
	balance := &connector.Balance{Equity: 10000, Currency: "EUR"}
	act := liveActivity{breakdown: &aggregatedBreakdown{}}
	if _, missing := applyLiveFX(balance, &act, "EUR", false, fxDay("2026-10-01"), testRates); missing != "EUR 2026-10-01" {
		t.Fatalf("missing = %q", missing)
	}
	if balance.Equity != 10000 || balance.Currency != "EUR" {
		t.Errorf("balance touched: %+v", balance)
	}
}

func TestLiveMeasuredOnFollowsTheStatement(t *testing.T) {
	startOfDay := fxDay("2026-10-01")
	if got := liveMeasuredOn(connector.NewMock(), startOfDay); !got.Equal(fxDay("2026-09-30")) {
		t.Errorf("non-statement broker measured on %s", got)
	}
}

func TestConvertHistoryRow(t *testing.T) {
	rows := []*connector.HistoricalSnapshot{
		// IBKR-like: dated on its close, flows one by one.
		{Date: fxDay("2026-09-29"), Currency: "EUR", TotalEquity: 1000, RealizedBalance: 900, Deposits: 600,
			Breakdown: map[string]*connector.MarketBalance{connector.MarketStocks: {Equity: 700, AvailableMargin: 300}},
			Cashflows: []*connector.Cashflow{
				{Amount: 500, Currency: "EUR", Timestamp: fxDay("2026-09-28")},
				{Amount: 100, Currency: "GBP", Timestamp: fxDay("2026-09-29")},
			}},
		// cTrader-like: dated D, holding the close of D-1, flows summed.
		{Date: fxDay("2026-10-01"), MeasuredOn: fxDay("2026-09-30"), Currency: "EUR", TotalEquity: 2000, RealizedBalance: 2000, Withdrawals: 100},
	}
	out, err := convertHistory(rows, true, testRates)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("%d rows", len(out))
	}
	ibkr, ctr := out[0], out[1]
	fxNear(t, "ibkr equity", ibkr.TotalEquity, 1120)
	fxNear(t, "ibkr realized", ibkr.RealizedBalance, 1008)
	fxNear(t, "ibkr stocks equity", ibkr.Breakdown[connector.MarketStocks].Equity, 784)
	fxNear(t, "ibkr deposits", ibkr.Deposits, 500*1.10+100*1.30)
	fxNear(t, "ibkr rate", ibkr.FXRateToUSD, 1.12)
	fxNear(t, "ctrader equity", ctr.TotalEquity, 2300)
	fxNear(t, "ctrader withdrawals", ctr.Withdrawals, 115)
}

func TestConvertHistoryHoldsBackOnlyTheNewestDays(t *testing.T) {
	row := func(d string) *connector.HistoricalSnapshot {
		return &connector.HistoricalSnapshot{Date: fxDay(d), Currency: "EUR", TotalEquity: 100}
	}
	out, err := convertHistory([]*connector.HistoricalSnapshot{row("2026-09-29"), row("2026-09-30"), row("2026-10-01"), row("2026-10-02")}, false, testRates)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || !out[1].Date.Equal(fxDay("2026-09-30")) {
		t.Fatalf("kept %d rows", len(out))
	}

	// A hole in the middle is not something tomorrow fixes: refuse the batch.
	_, err = convertHistory([]*connector.HistoricalSnapshot{row("2026-09-27"), row("2026-09-29")}, false, testRates)
	if err == nil || !strings.Contains(err.Error(), "EUR 2026-09-27") {
		t.Fatalf("err = %v", err)
	}
}

func TestConvertHistoryLeavesUSDRowsAlone(t *testing.T) {
	r := &connector.HistoricalSnapshot{Date: fxDay("2026-09-29"), TotalEquity: 100, Deposits: 10}
	out, err := convertHistory([]*connector.HistoricalSnapshot{r}, false, fxRates{})
	if err != nil || len(out) != 1 || r.TotalEquity != 100 || r.FXRateToUSD != 0 {
		t.Fatalf("out=%v err=%v row=%+v", out, err, r)
	}
}

func TestBuildHistoricalSnapshotsStampsTheConversion(t *testing.T) {
	conn := &repository.ExchangeConnection{UserUID: "u", Exchange: "ibkr", Label: "l"}
	h := &connector.HistoricalSnapshot{Date: fxDay("2026-09-29"), TotalEquity: 1120, RealizedBalance: 1120, Currency: "eur", FXRateToUSD: 1.12}
	snaps, _ := buildHistoricalSnapshots(conn, []*connector.HistoricalSnapshot{h}, fxDay("2026-10-05"), false)
	if len(snaps) != 1 {
		t.Fatalf("%d snapshots", len(snaps))
	}
	g := snaps[0].Breakdown.Global
	if g == nil || g.NativeCurrency != "EUR" || g.FXRateToUSD != 1.12 {
		t.Fatalf("global = %+v", g)
	}

	usd := &connector.HistoricalSnapshot{Date: fxDay("2026-09-29"), TotalEquity: 100, RealizedBalance: 100}
	snaps, _ = buildHistoricalSnapshots(conn, []*connector.HistoricalSnapshot{usd}, fxDay("2026-10-05"), false)
	if g := snaps[0].Breakdown.Global; g.NativeCurrency != "" || g.FXRateToUSD != 0 {
		t.Fatalf("USD row stamped: %+v", g)
	}
}

func TestParseFXRatesKeepsOnlyFinalRates(t *testing.T) {
	body := `{"success":true,"data":{"currency":"EUR","quote":"USD","rates":[
		{"date":"2026-10-02","rate":1.12,"observedDate":"2026-10-02","final":true},
		{"date":"2026-10-03","rate":1.12,"observedDate":"2026-10-02","final":true},
		{"date":"2026-10-05","rate":1.13,"observedDate":"2026-10-02","final":false},
		{"date":"2026-10-06","rate":0,"final":true}]}}`
	got, err := parseFXRates([]byte(body), "EUR")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["2026-10-03"] != 1.12 {
		t.Fatalf("got %v", got)
	}
	if _, err := parseFXRates([]byte(`{"success":true,"data":{"currency":"GBP","quote":"USD","rates":[]}}`), "EUR"); err == nil {
		t.Fatal("accepted a GBP series for EUR")
	}
}

func TestFinalRatesToUSDCachesFinalDaysOnly(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !strings.HasPrefix(r.URL.Path, "/api/v1/benchmarks/fx/EUR/daily") {
			http.NotFound(w, r)
			return
		}
		start := r.URL.Query().Get("startDate")
		var rows []string
		for d := fxDay(start); !d.After(fxDay(r.URL.Query().Get("endDate"))); d = d.Add(24 * time.Hour) {
			final := d.Before(fxDay("2026-10-05"))
			rows = append(rows, fmt.Sprintf(`{"date":%q,"rate":1.1,"final":%v}`, dayKey(d), final))
		}
		fmt.Fprintf(w, `{"success":true,"data":{"currency":"EUR","quote":"USD","rates":[%s]}}`, strings.Join(rows, ","))
	}))
	defer srv.Close()

	b := NewBenchmarkService(srv.URL, "")
	ctx := context.Background()
	got, err := b.FinalRatesToUSD(ctx, "EUR", fxDay("2026-10-01"), fxDay("2026-10-05"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d rates, want the 4 final ones", len(got))
	}
	if _, err := b.FinalRatesToUSD(ctx, "EUR", fxDay("2026-10-01"), fxDay("2026-10-04")); err != nil || calls.Load() != 1 {
		t.Fatalf("a fully cached window went back to the service (%d calls, %v)", calls.Load(), err)
	}
	// The non-final day is asked for again.
	if _, err := b.FinalRatesToUSD(ctx, "EUR", fxDay("2026-10-03"), fxDay("2026-10-05")); err != nil || calls.Load() != 2 {
		t.Fatalf("calls = %d, err = %v", calls.Load(), err)
	}
}

func TestPlanFXBackfillConvertsOnceAtEachDaysRate(t *testing.T) {
	stamped := &repository.Snapshot{Timestamp: fxDay("2026-09-29"), TotalEquity: 999,
		Breakdown: &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: 999, NativeCurrency: "EUR", FXRateToUSD: 1.12}}}
	rows := []*repository.Snapshot{
		{Timestamp: fxDay("2026-09-29"), TotalEquity: 1000, RealizedBalance: 800, UnrealizedPnL: 200, Deposits: 100,
			Breakdown: &repository.MarketBreakdown{
				Stocks: &repository.MarketMetrics{Equity: 600, AvailableMargin: 400, Volume: 50},
				Global: &repository.MarketMetrics{Equity: 1000, AvailableMargin: 400, Volume: 50},
			}},
		stamped,
		{Timestamp: fxDay("2026-10-01"), TotalEquity: 1000},
	}

	days, changed := planFXBackfill(rows, "ibkr", "EUR", testRates)
	if len(changed) != 1 || len(days) != 3 {
		t.Fatalf("changed %d of %d", len(changed), len(days))
	}
	r := rows[0]
	fxNear(t, "equity", r.TotalEquity, 1120)
	fxNear(t, "realized", r.RealizedBalance, 896)
	fxNear(t, "unrealized", r.UnrealizedPnL, 224)
	fxNear(t, "deposits", r.Deposits, 112)
	fxNear(t, "stocks equity", r.Breakdown.Stocks.Equity, 672)
	fxNear(t, "global equity", r.Breakdown.Global.Equity, 1120)
	fxNear(t, "global margin", r.Breakdown.Global.AvailableMargin, 448)
	fxNear(t, "volume untouched", r.Breakdown.Global.Volume, 50)
	if r.Breakdown.Global.NativeCurrency != "EUR" || r.Breakdown.Global.FXRateToUSD != 1.12 {
		t.Errorf("not stamped: %+v", r.Breakdown.Global)
	}
	if stamped.TotalEquity != 999 || !strings.HasPrefix(days[1].Kept, "already converted") {
		t.Errorf("a stamped row was converted again: %+v", days[1])
	}
	if days[2].Kept != "no final rate yet" || rows[2].TotalEquity != 1000 {
		t.Errorf("a day without a rate was touched: %+v", days[2])
	}

	// Run it again over the result: nothing left to convert.
	if _, again := planFXBackfill(rows, "ibkr", "EUR", testRates); len(again) != 0 {
		t.Fatalf("second pass converted %d rows", len(again))
	}
}

func TestStoredMeasuredOnMatchesTheWriters(t *testing.T) {
	ts := fxDay("2026-10-01")
	if got := storedMeasuredOn("ibkr", ts); !got.Equal(ts) {
		t.Errorf("ibkr measured on %s", got)
	}
	if got := storedMeasuredOn("ctrader", ts); !got.Equal(fxDay("2026-09-30")) {
		t.Errorf("ctrader measured on %s", got)
	}
}

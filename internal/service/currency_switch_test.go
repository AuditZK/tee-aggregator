package service

import (
	"testing"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// A EUR account whose broker starts stating it in USD: the euros are the same,
// the statement now values them at its own rate. These pin what the sync does
// with that, whichever way the setting flips.

func storedRow(day string, equity float64, denom string, rate float64) *repository.Snapshot {
	return &repository.Snapshot{
		Exchange: "ibkr", Label: "main", Timestamp: fxDay(day), TotalEquity: equity,
		Breakdown: &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: equity, NativeCurrency: denom, FXRateToUSD: rate}},
	}
}

func rebasedDay(day string, usd, venueEURRate float64, flows ...*connector.Cashflow) *connector.HistoricalSnapshot {
	return &connector.HistoricalSnapshot{
		Date: fxDay(day), TotalEquity: usd, RealizedBalance: usd, Currency: "USD",
		BaseRates: map[string]float64{"EUR": venueEURRate}, Cashflows: flows,
		Breakdown: map[string]*connector.MarketBalance{connector.MarketStocks: {MarketType: connector.MarketStocks, Equity: usd, AvailableMargin: usd}},
	}
}

// With the statement's own rate, a USD statement is stated back in EUR, then
// converted as every stored day was: the history reproduces to the cent.
func TestRestateHistoryKeepsTheStoredCurrency(t *testing.T) {
	// 1000 EUR held; IBKR values them at 1.13 then 1.16, our feed at 1.12 then 1.15.
	deposit := &connector.Cashflow{Amount: 116, Currency: "USD", Timestamp: fxDay("2026-09-30")}
	rows := []*connector.HistoricalSnapshot{
		rebasedDay("2026-09-29", 1130, 1.13),
		rebasedDay("2026-09-30", 1276, 1.16, deposit),
	}

	n, from := restateHistory(rows, "EUR")
	if n != 2 || from != "USD" {
		t.Fatalf("restated %d rows from %q, want 2 from USD", n, from)
	}
	fxNear(t, "day 1 equity in EUR", rows[0].TotalEquity, 1000)
	fxNear(t, "day 2 equity in EUR", rows[1].TotalEquity, 1100)
	fxNear(t, "breakdown in EUR", rows[1].Breakdown[connector.MarketStocks].Equity, 1100)
	if deposit.Currency != "EUR" {
		t.Fatalf("deposit currency = %s, want EUR", deposit.Currency)
	}
	fxNear(t, "deposit in EUR", deposit.Amount, 100)

	out, err := convertHistory(rows, true, testRates)
	if err != nil || len(out) != 2 {
		t.Fatalf("convert: %v (%d rows)", err, len(out))
	}
	fxNear(t, "day 1 in USD at our rate", out[0].TotalEquity, 1120)
	fxNear(t, "day 2 in USD at our rate", out[1].TotalEquity, 1265)
	fxNear(t, "deposit in USD at our rate", out[1].Deposits, 115)
	if out[1].Currency != "EUR" || out[1].FXRateToUSD != 1.15 {
		t.Fatalf("row = %+v, want stamped EUR at 1.15", out[1])
	}
}

// Without the statement's rate nothing is guessed: the rows stay in USD,
// stamped USD, and the history is rewritten in USD rather than mixed.
func TestRestateHistoryWithoutTheVenueRateKeepsTheNewCurrency(t *testing.T) {
	row := rebasedDay("2026-09-29", 1130, 0)
	row.BaseRates = nil
	if n, _ := restateHistory([]*connector.HistoricalSnapshot{row}, "EUR"); n != 0 {
		t.Fatal("restated a row without a rate to restate it at")
	}
	out, err := convertHistory([]*connector.HistoricalSnapshot{row}, true, testRates)
	if err != nil || out[0].TotalEquity != 1130 || out[0].Currency != "USD" || out[0].FXRateToUSD != 1 {
		t.Fatalf("row = %+v, err %v; want 1130 stamped USD", out[0], err)
	}
}

// A EUR wire into a USD row that the statement did not value is valued at our
// rate, instead of being added to the dollars as if it were some.
func TestUSDRowValuesItsForeignFlows(t *testing.T) {
	row := &connector.HistoricalSnapshot{
		Date: fxDay("2026-09-29"), TotalEquity: 1120, Currency: "USD", Deposits: 1000,
		Cashflows: []*connector.Cashflow{{Amount: 1000, Currency: "EUR", Timestamp: fxDay("2026-09-29")}},
	}
	currencies, _, _ := historyFXWindow([]*connector.HistoricalSnapshot{row}, true)
	if !currencies["EUR"] {
		t.Fatal("the rates of a USD row's EUR flow are not read")
	}
	out, err := convertHistory([]*connector.HistoricalSnapshot{row}, true, testRates)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	fxNear(t, "deposit", out[0].Deposits, 1120)
}

// The gate holds a reconstruction to the days stored in its own currency. A
// day stored in another one, or in none, is rewritten instead: compared, every
// rebuild of a rebased account failed by the gap between two rate sources.
func TestContradictedDayComparesOnlyTheSameCurrency(t *testing.T) {
	rebuilt := []*repository.Snapshot{storedRow("2026-09-29", 1130, "USD", 1)}

	if _, _, bad := contradictedDay(rebuilt, []*repository.Snapshot{storedRow("2026-09-29", 1120, "EUR", 1.12)}); bad {
		t.Fatal("a day stored in EUR contradicted a reconstruction in USD")
	}
	// An unstamped day is a dollar day: it still witnesses a USD rebuild (a
	// USD account keeps its gate the night its rows get their first stamp),
	// and is rewritten by one in EUR.
	unstamped := storedRow("2026-09-29", 1100, "", 0)
	if _, _, bad := contradictedDay(rebuilt, []*repository.Snapshot{unstamped}); !bad {
		t.Fatal("an unstamped dollar day 2.7 % off did not hold a USD reconstruction")
	}
	inEUR := []*repository.Snapshot{storedRow("2026-09-29", 1000, "EUR", 1.12)}
	if _, _, bad := contradictedDay(inEUR, []*repository.Snapshot{unstamped}); bad {
		t.Fatal("an unstamped dollar day contradicted a reconstruction in EUR")
	}
	if _, _, bad := contradictedDay(rebuilt, []*repository.Snapshot{storedRow("2026-09-29", 1100, "USD", 1)}); !bad {
		t.Fatal("a day in the same currency 2.7 % off passed the gate")
	}

	// Venues whose rows carry no stamp keep the gate exactly as it was.
	plain := []*repository.Snapshot{{Exchange: "bybit", Timestamp: fxDay("2026-09-29"), TotalEquity: 1130}}
	if _, _, bad := contradictedDay(plain, []*repository.Snapshot{{Exchange: "bybit", Timestamp: fxDay("2026-09-29"), TotalEquity: 1100}}); !bad {
		t.Fatal("an unstamped venue lost its gate")
	}
}

// The live reading is stated in the stored currency at the statement's rate,
// flows included, so the new day continues the stored ones.
func TestRestateLiveStatesBalanceAndFlowsInTheStoredCurrency(t *testing.T) {
	bal := &connector.Balance{Equity: 1160, Available: 1160, Currency: "USD", BaseRates: map[string]float64{"EUR": 1.16}}
	act := &liveActivity{
		breakdown: &aggregatedBreakdown{},
		cashflows: []*connector.Cashflow{{Amount: 116, Currency: "USD", Timestamp: fxDay("2026-09-30")}},
	}
	act.breakdown.stocks.equity = 1160

	restateLive(bal, act, "USD", "EUR", 1.16)
	fxNear(t, "equity", bal.Equity, 1000)
	fxNear(t, "stocks", act.breakdown.stocks.equity, 1000)
	fxNear(t, "flow", act.cashflows[0].Amount, 100)
	if bal.Currency != "EUR" || act.cashflows[0].Currency != "EUR" {
		t.Fatalf("currency = %s / %s, want EUR", bal.Currency, act.cashflows[0].Currency)
	}
}

// A USD reading is stamped USD and its EUR flows valued.
func TestApplyLiveUSDStampsAndValuesForeignFlows(t *testing.T) {
	act := &liveActivity{cashflows: []*connector.Cashflow{
		{Amount: 100, Currency: "EUR", Timestamp: fxDay("2026-09-29")},
		{Amount: -50, Currency: "USD", Timestamp: fxDay("2026-09-29")},
	}}
	fx, reason := applyLiveUSD(act, true, fxDay("2026-09-30"), testRates)
	if reason != "" || fx.currency != "USD" || fx.rate != 1 {
		t.Fatalf("fx = %+v, reason %q", fx, reason)
	}
	fxNear(t, "deposits", act.deposits, 112)
	fxNear(t, "withdrawals", act.withdrawals, 50)
}

// The stored currency is the newest stamped row's; an unstamped row says
// nothing, and another connection's rows do not count.
func TestNewestDenomination(t *testing.T) {
	other := storedRow("2026-10-05", 1, "USD", 1)
	other.Label = "other"
	rows := []*repository.Snapshot{
		storedRow("2026-10-01", 1, "EUR", 1.13),
		storedRow("2026-10-03", 1, "EUR", 1.12),
		storedRow("2026-10-04", 1, "", 0),
		other,
	}
	if got := newestDenomination(rows, "ibkr", "main"); got != "EUR" {
		t.Fatalf("denomination = %q, want EUR", got)
	}
	if got := newestDenomination(nil, "ibkr", "main"); got != "" {
		t.Fatalf("denomination of nothing = %q", got)
	}
}

// The backfill reads an unstamped row as one in the account's currency. On
// IBKR that multiplied a dollar row by the rate; its full-window rebuild does
// the job instead. A USD-stamped row is never converted, whatever the venue.
func TestBackfillNeverReadsADollarRowAsForeign(t *testing.T) {
	if backfillsStoredRows("ibkr") || !backfillsStoredRows("ctrader") {
		t.Fatal("backfill scope drifted")
	}
	usd := storedRow("2026-09-29", 1000, "USD", 1)
	days, changed := planFXBackfill([]*repository.Snapshot{usd}, "ctrader", "EUR", testRates)
	if len(changed) != 0 || usd.TotalEquity != 1000 || days[0].Kept == "" {
		t.Fatalf("a USD-stamped row was converted: %+v", days)
	}
}

// The whole path for a rebased account: stored days in EUR, a reconstruction
// from a statement now in USD. Stated back in EUR at the statement's rates and
// converted at ours, it reproduces every stored day and passes the gate.
func TestRebasedStatementReproducesTheStoredHistory(t *testing.T) {
	conn := &repository.ExchangeConnection{UserUID: "u", Exchange: "ibkr", Label: "main"}
	stored := []*repository.Snapshot{
		storedRow("2026-09-29", 1120, "EUR", 1.12),
		storedRow("2026-09-30", 1150, "EUR", 1.15),
	}
	rows := []*connector.HistoricalSnapshot{
		rebasedDay("2026-09-29", 1130, 1.13),
		rebasedDay("2026-09-30", 1160, 1.16),
	}

	restateHistory(rows, newestDenomination(stored, "ibkr", "main"))
	converted, err := convertHistory(rows, true, testRates)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	rebuilt, _ := buildHistoricalSnapshots(conn, converted, fxDay("2026-10-05"), false)
	if day, measured, bad := contradictedDay(rebuilt, stored); bad {
		t.Fatalf("rebuilt %s at %v, stored %v", day.Timestamp.Format("2006-01-02"), day.TotalEquity, measured)
	}
	for i, r := range rebuilt {
		fxNear(t, "day equity", r.TotalEquity, stored[i].TotalEquity)
		if r.Breakdown.Global.NativeCurrency != "EUR" {
			t.Fatalf("day %d stamped %q, want EUR", i, r.Breakdown.Global.NativeCurrency)
		}
	}
}

// Rewriting a EUR history in USD has no witness: the stored days are in EUR,
// and the dollar day the live sync wrote since holds the statement before the
// one rebuilt for its date. Holding the rebuild to it would refuse it every
// night, and the live sync, waiting for the history in USD, would stop.
func TestRewritesInAnotherCurrency(t *testing.T) {
	stored := []*repository.Snapshot{
		storedRow("2026-10-05", 1150, "EUR", 1.15),
		storedRow("2026-10-07", 1163, "", 0),
	}
	usd := []*repository.Snapshot{storedRow("2026-10-05", 1160, "USD", 1), storedRow("2026-10-07", 1171, "USD", 1)}
	from, to, ok := rewritesInAnotherCurrency(usd, stored, "ibkr", "main")
	if !ok || from != "EUR" || to != "USD" {
		t.Fatalf("got %q -> %q (%v), want EUR -> USD", from, to, ok)
	}

	eur := []*repository.Snapshot{storedRow("2026-10-05", 1150, "EUR", 1.15)}
	if _, _, ok := rewritesInAnotherCurrency(eur, stored, "ibkr", "main"); ok {
		t.Fatal("a reconstruction in the stored currency was waved through the gate")
	}
	plain := []*repository.Snapshot{{Exchange: "bybit", Timestamp: fxDay("2026-10-05"), TotalEquity: 1}}
	if _, _, ok := rewritesInAnotherCurrency(plain, nil, "bybit", "main"); ok {
		t.Fatal("an unstamped venue skipped its gate")
	}
	if _, _, ok := rewritesInAnotherCurrency(usd, []*repository.Snapshot{storedRow("2026-10-05", 1, "", 0)}, "ibkr", "main"); ok {
		t.Fatal("a USD account whose rows get their first stamp skipped its gate")
	}
}

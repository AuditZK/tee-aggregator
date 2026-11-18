package connector

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func igDay(d int, hour int) time.Time {
	return time.Date(2026, 7, d, hour, 0, 0, 0, time.UTC)
}

func igTx(kind, name, size, open, closeLevel, pnl string, at, opened time.Time) IGRawTransaction {
	tx := IGRawTransaction{
		TransactionType: kind,
		InstrumentName:  name,
		Size:            size,
		OpenLevel:       open,
		CloseLevel:      closeLevel,
		ProfitAndLoss:   pnl,
		Currency:        "E",
		DateUTC:         at.Format(igTimeFormat),
	}
	if !opened.IsZero() {
		tx.OpenDateUTC = opened.Format(igTimeFormat)
	}
	return tx
}

func igDeposit(at time.Time, amount string) IGRawTransaction {
	return igTx("DEPO", "Virement bancaire", "-", "-", "0", amount, at, time.Time{})
}

func igDealTx(name, size, open, closeLevel, pnl string, opened, closed time.Time) IGRawTransaction {
	return igTx("DEAL", name, size, open, closeLevel, pnl, closed, opened)
}

func mustLines(t *testing.T, txs ...IGRawTransaction) []igLedgerLine {
	t.Helper()
	lines, err := igLedgerLines(txs)
	if err != nil {
		t.Fatal(err)
	}
	return lines
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestIGHistoryWalksCashBackFromTodaysBalance(t *testing.T) {
	lines := mustLines(t,
		igDeposit(igDay(1, 9), "E1000"),
		igDealTx("Synthetic Index", "+1", "100", "150", "E50", igDay(2, 9), igDay(2, 15)),
		igTx("WITH", "Charge administrative journalière", "-", "-", "0", "E-2", igDay(3, 11), time.Time{}),
		igTx("WITH", "Virement bancaire", "-", "-", "0", "E-100", igDay(4, 9), time.Time{}),
	)
	held, _, err := igClosedSlices(lines)
	if err != nil {
		t.Fatal(err)
	}

	got, err := buildIGHistoricalSnapshots(lines, held, nil, 948, igDay(6, 12))
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		day                   int
		equity, dep, wd, fund float64
		trades                int
	}{
		{2, 1000, 1000, 0, 0, 0},
		{3, 1050, 0, 0, 0, 1},
		{4, 1048, 0, 0, 2, 0},
		{5, 948, 0, 100, 0, 0},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for k, w := range want {
		g := got[k]
		if !g.Date.Equal(igDay(w.day, 0)) || !near(g.TotalEquity, w.equity) || !near(g.Deposits, w.dep) ||
			!near(g.Withdrawals, w.wd) || !near(g.FundingFees, w.fund) || g.TotalTrades != w.trades {
			t.Errorf("row %d = %+v, want %+v", k, g, w)
		}
	}
}

func TestIGHistoryMarksPositionsHeldAtMidnight(t *testing.T) {
	lines := mustLines(t,
		igDeposit(igDay(1, 9), "E10000"),
		igDealTx("Long Index", "+2", "100", "110", "E200", igDay(2, 10), igDay(4, 10)),
		igDealTx("Short Index", "-1", "50", "40", "E50", igDay(2, 11), igDay(4, 11)),
	)
	held, _, err := igClosedSlices(lines)
	if err != nil {
		t.Fatal(err)
	}
	held[0].epic, held[1].epic = "IX.LONG", "IX.SHORT"
	bars := map[string][]igBar{
		"IX.LONG":  {{day: igDay(2, 0), bid: 103, ask: 104}},
		"IX.SHORT": {{day: igDay(2, 0), bid: 46, ask: 47}, {day: igDay(3, 0), bid: 44, ask: 45}},
	}

	got, err := buildIGHistoricalSnapshots(lines, held, bars, 10250, igDay(5, 12))
	if err != nil {
		t.Fatal(err)
	}
	// Day 3: long marked at day 2's bid, short at day 2's ask.
	// Day 4: no day-3 bar for the long, so day 2's still holds; short at day 3's ask.
	want := map[int]float64{
		2: 10000,
		3: 10000 + 10*(103-100)*2 + 5*(47-50)*-1,
		4: 10000 + 10*(103-100)*2 + 5*(45-50)*-1,
	}
	for _, g := range got {
		if w := want[g.Date.Day()]; !near(g.TotalEquity, w) {
			t.Errorf("day %d equity = %v, want %v", g.Date.Day(), g.TotalEquity, w)
		}
		if !near(g.RealizedBalance, 10000) {
			t.Errorf("day %d cash = %v, want 10000", g.Date.Day(), g.RealizedBalance)
		}
	}
}

func TestIGHistoryRefusesAPositionItCannotPrice(t *testing.T) {
	lines := mustLines(t,
		igDeposit(igDay(1, 9), "E1000"),
		igDealTx("Synthetic Index", "+1", "100", "110", "E10", igDay(2, 10), igDay(4, 10)),
	)
	held, _, err := igClosedSlices(lines)
	if err != nil {
		t.Fatal(err)
	}
	held[0].epic = "IX.TEST"
	if _, err := buildIGHistoricalSnapshots(lines, held, nil, 1010, igDay(5, 12)); err == nil {
		t.Fatal("a position held at midnight with no price was valued at cash")
	}
}

func TestIGClosedSlicesTakeTheMedianPointForAFlatDeal(t *testing.T) {
	lines := mustLines(t,
		igDealTx("Synthetic Index", "+1", "100", "110", "E100", igDay(1, 9), igDay(1, 10)),
		igDealTx("Synthetic Index", "+1", "100", "110", "E120", igDay(1, 11), igDay(1, 12)),
		igDealTx("Synthetic Index", "+1", "100", "100", "E0", igDay(1, 13), igDay(3, 10)),
	)
	held, points, err := igClosedSlices(lines)
	if err != nil {
		t.Fatal(err)
	}
	if !near(points["Synthetic Index"], 11) || !near(held[2].point, 11) {
		t.Fatalf("flat deal point = %v, instrument point = %v, want 11", held[2].point, points["Synthetic Index"])
	}
}

func TestIGOpenSlicesAddUpToTheReportedProfitLoss(t *testing.T) {
	open := []igOpenPosition{
		{epic: "IX.KNOWN", name: "Known Index", long: true, size: 2, level: 100, mark: 105, contractSize: 1},
		{epic: "FX.NEW", name: "New Pair", long: false, size: 3, level: 50, mark: 48, contractSize: 2},
	}
	points := map[string]float64{"Known Index": 10}
	held := igOpenSlices(open, points, 154)

	total := 0.0
	for k, h := range held {
		total += h.point * (open[k].mark - h.openLevel) * h.size
	}
	if !near(total, 154) {
		t.Fatalf("open positions are worth %v, want the reported 154", total)
	}
	if !held[0].long() || held[1].long() {
		t.Fatal("sides lost: the long must exit at the bid and the short at the ask")
	}
}

func TestIGHistoryRowsStopBeforeToday(t *testing.T) {
	lines := mustLines(t, igDeposit(igDay(5, 9), "E1000"))
	if _, _, ok := igHistoryRows(lines, igDay(5, 18)); ok {
		t.Fatal("a ledger opened today has no past midnight to reconstruct")
	}
}

func TestIGBarDayTakesTheLocalTradingDate(t *testing.T) {
	got, err := igBarDay("2026/07/03 00:00:00", "2026-07-02T16:00:00")
	if err != nil || !got.Equal(igDay(3, 0)) {
		t.Fatalf("bar day = %v, %v; want 2026-07-03", got, err)
	}
}

// End to end against the fake: ledger, open position, instrument codes from
// the activity history and daily closes, read the way a reconstruction does.
func TestIGGetHistoricalSnapshotsEndToEnd(t *testing.T) {
	now := time.Now().UTC()
	day := func(back int, hour int) time.Time {
		return truncUTCDay(now).Add(time.Duration(-back)*24*time.Hour + time.Duration(hour)*time.Hour)
	}
	ts := func(t time.Time) string { return t.Format(igTimeFormat) }
	bar := func(t time.Time, bid, ask float64) map[string]any {
		return map[string]any{"snapshotTime": t.Format(igPriceTimeFormat), "snapshotTimeUTC": ts(t), "closePrice": map[string]any{"bid": bid, "ask": ask}}
	}

	f := &igFakeServer{
		ledger: []map[string]any{
			{"transactionType": "DEAL", "instrumentName": "Synthetic Index", "size": "+1", "openLevel": "100", "closeLevel": "110",
				"profitAndLoss": "E100", "currency": "E", "dateUtc": ts(day(2, 10)), "openDateUtc": ts(day(4, 10))},
			{"transactionType": "DEPO", "instrumentName": "Virement bancaire", "size": "-", "openLevel": "-", "closeLevel": "0",
				"profitAndLoss": "E10000", "currency": "E", "dateUtc": ts(day(5, 9))},
		},
		balance: map[string]any{"accountId": "ACC1", "accountType": "CFD", "currency": "EUR",
			"balance": map[string]any{"balance": 10100.0, "profitLoss": 30.0, "available": 9000.0}},
		positionsJSON: fmt.Sprintf(`{"positions":[{"market":{"epic":"FX.OPEN","instrumentName":"Open Pair","bid":1.30,"offer":1.31},
			"position":{"dealId":"D1","direction":"BUY","size":1,"level":1.00,"contractSize":100,"createdDateUTC":%q}}]}`, ts(day(3, 8))),
		activities: []map[string]any{{"epic": "IX.SYNTH", "details": map[string]any{"marketName": "Synthetic Index"}}},
		bars: map[string][]map[string]any{
			"IX.SYNTH": {bar(day(4, 0), 104, 105), bar(day(3, 0), 107, 108)},
			"FX.OPEN":  {bar(day(3, 0), 1.10, 1.11), bar(day(2, 0), 1.20, 1.21), bar(day(1, 0), 1.25, 1.26)},
		},
	}
	ig := newIGFake(t, f)

	got, err := ig.GetHistoricalSnapshots(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d rows, want the 4 midnights from the deposit's next day to yesterday", len(got))
	}
	// Open Pair: 30 of unrealised P&L over a 0.30 move on a contract of 100,
	// so 100 per point.
	want := []float64{
		10000,
		10000 + 10*(104-100),
		10000 + 10*(107-100) + 100*(1.10-1.00),
		10100 + 100*(1.20-1.00),
	}
	for k, g := range got {
		if !near(g.TotalEquity, want[k]) {
			t.Errorf("row %s equity = %v, want %v", g.Date.Format(time.DateOnly), g.TotalEquity, want[k])
		}
		if g.Currency != "EUR" || !g.MeasuredOn.Equal(g.Date.Add(-24*time.Hour)) {
			t.Errorf("row %s currency %q measured %v", g.Date.Format(time.DateOnly), g.Currency, g.MeasuredOn)
		}
	}
	c := ig.HistoryCoverage()
	if c.LedgerLines != 2 || c.HeldPositions != 2 || c.Instruments != 2 || c.PriceAllowanceLeft != 9000 {
		t.Errorf("coverage = %+v", c)
	}
	if strings.Contains(fmt.Sprintf("%+v", c), "Synthetic") {
		t.Error("coverage carries an instrument name")
	}
}

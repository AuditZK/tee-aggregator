package service

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

func recountRow(day time.Time, equity float64, trades int, rebuilt bool) *repository.Snapshot {
	return &repository.Snapshot{
		ID: day.Format("2006-01-02"), Timestamp: day, TotalEquity: equity, FromExternalRebuilder: rebuilt,
		Breakdown: &repository.MarketBreakdown{
			Swap:   &repository.MarketMetrics{Equity: equity, AvailableMargin: 400, Trades: trades, Volume: 99999, TradingFees: -20},
			Global: &repository.MarketMetrics{Equity: equity, AvailableMargin: 400, Trades: trades, Volume: 99999, TradingFees: -20},
		},
	}
}

// A live day is rebuilt from the fills and funding of its own window only;
// equity and margin are carried over untouched.
func TestRecountDayReadsItsWindowOnly(t *testing.T) {
	d1 := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	rows := []*repository.Snapshot{recountRow(d1, 1000, 5, true), recountRow(d2, 1100, 68, false)}
	trades := []*connector.Trade{
		{ID: "before", Price: 10, Quantity: 1, Fee: 1, Side: "buy", MarketType: connector.MarketSwap, Timestamp: d1.Add(-time.Hour)},
		{ID: "in1", Price: 10, Quantity: 2, Fee: 0.2, Side: "buy", MarketType: connector.MarketSwap, Timestamp: d1.Add(time.Hour)},
		{ID: "in2", Price: 10, Quantity: 3, Fee: 0.3, Side: "sell", MarketType: connector.MarketSwap, Timestamp: d2.Add(-time.Minute)},
		{ID: "at-day", Price: 10, Quantity: 4, Fee: 0.4, Side: "sell", MarketType: connector.MarketSwap, Timestamp: d2},
	}
	funding := []*connector.FundingFee{
		{Amount: -6, Timestamp: d1.Add(8 * time.Hour)},
		{Amount: 2, Timestamp: d1.Add(16 * time.Hour)},
		{Amount: -100, Timestamp: d2.Add(time.Hour)},
	}

	d := (&SyncService{}).recountDay(rows, 1, "bybit", []string{connector.MarketSwap}, trades, funding, true)
	if d.Kept != "" {
		t.Fatalf("live day kept: %s", d.Kept)
	}
	if d.NewTrades != 2 || d.NewLongTrades != 1 || d.NewShortTrades != 1 {
		t.Fatalf("trades=%d long=%d short=%d, want 2/1/1", d.NewTrades, d.NewLongTrades, d.NewShortTrades)
	}
	if d.NewVolume != 50 || d.NewFundingFees != 4 {
		t.Fatalf("volume=%v funding=%v, want 50 and 4 (paid, as a cost)", d.NewVolume, d.NewFundingFees)
	}
	if math.Abs(d.NewTradingFees-0.5) > 1e-9 {
		t.Fatalf("trading fees = %v, want 0.5", d.NewTradingFees)
	}
	if d.OldTrades != 68 || !d.changed() {
		t.Fatalf("old=%d changed=%v, want 68 and a change", d.OldTrades, d.changed())
	}
	for _, m := range []*repository.MarketMetrics{d.breakdown.Swap, d.breakdown.Global} {
		if m.Equity != 1100 || m.AvailableMargin != 400 {
			t.Fatalf("equity/margin moved: %+v", m)
		}
	}
	if rows[1].Breakdown.Global.Trades != 68 || rows[1].Breakdown.Swap.Trades != 68 {
		t.Fatal("recount mutated the stored row it read")
	}
}

func TestRecountDayKeepsAReconstructedDay(t *testing.T) {
	d1 := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	rows := []*repository.Snapshot{recountRow(d1, 1000, 39, true)}
	d := (&SyncService{}).recountDay(rows, 0, "bybit", []string{connector.MarketSwap}, nil, nil, true)
	if d.Kept == "" || d.changed() || d.breakdown != nil {
		t.Fatalf("reconstructed day touched: %+v", d)
	}
}

func TestRecountOnlyVenuesThatReadAPastRange(t *testing.T) {
	if _, ok := recountScopeFor("Bybit"); !ok {
		t.Fatal("bybit not recountable")
	}
	bn, ok := recountScopeFor("binance")
	if !ok || containsMarket(bn.markets, connector.MarketSpot) || bn.horizon == 0 {
		t.Fatalf("binance scope = %+v, want swap only with a horizon", bn)
	}
	for _, venue := range []string{"hyperliquid", "mexc"} {
		sc, ok := recountScopeFor(venue)
		if !ok || !sc.fundingOnly || len(sc.markets) != 0 {
			t.Fatalf("%s scope = %+v, want funding only: its fills are not read back", venue, sc)
		}
	}
	if _, ok := recountScopeFor("okx"); ok {
		t.Fatal("okx recountable without a checked past-range reader")
	}
}

// A funding-only venue has its funding rewritten from the settlements of the
// row's window. Trades, volume, fees, equity and margin stay as stored.
func TestRecountFundingDayRewritesFundingAlone(t *testing.T) {
	d1 := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	rows := []*repository.Snapshot{recountRow(d1, 1000, 5, false), recountRow(d2, 1100, 68, false)}
	rows[1].Breakdown.Swap.FundingFees, rows[1].Breakdown.Global.FundingFees = 9, 9
	funding := []*connector.FundingFee{
		{Amount: -50, Timestamp: d1.Add(-time.Hour)},
		{Amount: -6, Timestamp: d1},
		{Amount: 2, Timestamp: d1.Add(16 * time.Hour)},
		{Amount: -100, Timestamp: d2},
	}

	d := recountFundingDay(rows, 1, "hyperliquid", funding)
	if d.Kept != "" {
		t.Fatalf("live day kept: %s", d.Kept)
	}
	if d.OldFundingFees != 9 || d.NewFundingFees != 4 || !d.changed() {
		t.Fatalf("funding %v -> %v, want 9 -> 4 (paid, as a cost)", d.OldFundingFees, d.NewFundingFees)
	}
	if d.NewTrades != 68 || d.NewVolume != 99999 || d.NewTradingFees != -20 {
		t.Fatalf("activity moved: %+v", d)
	}
	for _, m := range []*repository.MarketMetrics{d.breakdown.Swap, d.breakdown.Global} {
		if m.FundingFees != 4 || m.Trades != 68 || m.Volume != 99999 || m.Equity != 1100 || m.AvailableMargin != 400 {
			t.Fatalf("entry = %+v, want funding 4 and the rest as stored", m)
		}
	}
	if rows[1].Breakdown.Swap.FundingFees != 9 {
		t.Fatal("the stored row was edited in place")
	}
}

// MEXC days were stored with no funding and sometimes no swap entry at all.
func TestRecountFundingDayCreatesTheFundingMarket(t *testing.T) {
	d1 := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	rows := []*repository.Snapshot{recountRow(d1, 1000, 0, false), recountRow(d2, 1100, 0, false)}
	rows[1].Breakdown.Swap = nil
	rows[1].Breakdown.Spot = &repository.MarketMetrics{Equity: 1100}

	d := recountFundingDay(rows, 1, "mexc", []*connector.FundingFee{{Amount: -0.5, Timestamp: d1.Add(8 * time.Hour)}})
	if d.NewFundingFees != 0.5 || d.breakdown.Swap == nil || d.breakdown.Swap.FundingFees != 0.5 {
		t.Fatalf("funding = %v, swap = %+v, want 0.5 on a new swap entry", d.NewFundingFees, d.breakdown.Swap)
	}
	if d.breakdown.Spot.Equity != 1100 || d.breakdown.Global.Equity != 1100 {
		t.Fatal("equity moved")
	}
}

// A read that does not reach back to the row's day says nothing about it: a
// stored figure is not replaced by a zero.
func TestRecountFundingDayKeepsADayTheReadDoesNotReach(t *testing.T) {
	d1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	rows := []*repository.Snapshot{recountRow(d1, 1000, 5, false), recountRow(d2, 1100, 68, false)}
	rows[1].Breakdown.Swap.FundingFees, rows[1].Breakdown.Global.FundingFees = 9, 9
	later := []*connector.FundingFee{{Amount: -3, Timestamp: d2.Add(48 * time.Hour)}}

	if d := recountFundingDay(rows, 1, "hyperliquid", later); d.Kept == "" || d.changed() {
		t.Fatalf("day rewritten from a read that starts after it: %+v", d)
	}
	rows[1].FromExternalRebuilder = true
	if d := recountFundingDay(rows, 1, "hyperliquid", nil); d.Kept != "reconstructed day" {
		t.Fatalf("reconstructed day not kept: %+v", d)
	}
}

// Binance spot cannot be read back: a recount of its swap fills leaves the
// spot market as its sync stored it, and global is the sum of both.
func TestRecountDayLeavesUnreadMarketsAlone(t *testing.T) {
	d1 := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	stored := &repository.MarketBreakdown{
		Spot:   &repository.MarketMetrics{Equity: 50, Trades: 2, Volume: 30, TradingFees: 0.03, LongTrades: 1, ShortTrades: 1},
		Swap:   &repository.MarketMetrics{Equity: 950, AvailableMargin: 700, Trades: 3, Volume: 300, TradingFees: 0.12, FundingFees: 1.5},
		Global: &repository.MarketMetrics{Equity: 1000, AvailableMargin: 700, Trades: 5, Volume: 330, TradingFees: 0.15, FundingFees: 1.5},
	}
	rows := []*repository.Snapshot{
		{Timestamp: d1, TotalEquity: 990, FromExternalRebuilder: true},
		{ID: "live", Timestamp: d2, TotalEquity: 1000, Breakdown: stored},
	}
	trades := []*connector.Trade{
		{ID: "a", Price: 100, Quantity: 1, Fee: 0.04, Side: "buy", MarketType: connector.MarketSwap, Timestamp: d1.Add(time.Hour)},
		{ID: "b", Price: 100, Quantity: 1, Fee: 0.04, Side: "sell", MarketType: connector.MarketSwap, Timestamp: d1.Add(2 * time.Hour)},
		{ID: "c", Price: 100, Quantity: 1, Fee: 0.04, Side: "sell", MarketType: connector.MarketSwap, Timestamp: d1.Add(3 * time.Hour)},
		{ID: "spot-today", Price: 1, Quantity: 1, Side: "buy", MarketType: connector.MarketSpot, Timestamp: d1.Add(time.Hour)},
	}

	d := (&SyncService{}).recountDay(rows, 1, "binance", []string{connector.MarketSwap}, trades, nil, false)
	b := d.breakdown
	if *b.Spot != *stored.Spot {
		t.Fatalf("spot rewritten: %+v", b.Spot)
	}
	if b.Swap.Trades != 3 || b.Swap.LongTrades != 1 || b.Swap.ShortTrades != 2 || b.Swap.Equity != 950 || b.Swap.AvailableMargin != 700 {
		t.Fatalf("swap = %+v, want 3 fills split 1/2, equity and margin kept", b.Swap)
	}
	if b.Swap.FundingFees != 1.5 {
		t.Fatalf("swap funding = %v, want 1.5 kept: the connector reads no funding", b.Swap.FundingFees)
	}
	if b.Global.Trades != 5 || b.Global.LongTrades != 2 || b.Global.ShortTrades != 3 || b.Global.Equity != 1000 || b.Global.FundingFees != 1.5 {
		t.Fatalf("global = %+v, want spot + swap", b.Global)
	}
}

// A global that does not add up to its markets came from before the
// per-market breakdown; recounting one market into it would double it.
func TestRecountDayKeepsARowWhoseGlobalDisagrees(t *testing.T) {
	d1 := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	rows := []*repository.Snapshot{
		{Timestamp: d1, FromExternalRebuilder: true},
		{Timestamp: d1.Add(24 * time.Hour), Breakdown: &repository.MarketBreakdown{
			Swap:   &repository.MarketMetrics{Trades: 3, Volume: 300},
			Global: &repository.MarketMetrics{Trades: 9, Volume: 900},
		}},
	}
	d := (&SyncService{}).recountDay(rows, 1, "binance", []string{connector.MarketSwap}, nil, nil, false)
	if d.Kept == "" || d.breakdown != nil {
		t.Fatalf("disagreeing row rewritten: %+v", d)
	}
}

// A classic Binance account reads no funding: its recount must keep the swap
// funding a row stores, not write the empty read over it as zero.
func TestRecountKeepsStoredFundingOfAClassicBinanceAccount(t *testing.T) {
	// No balance read and no fapi refusal seen: the connector is not in
	// Portfolio Margin, and answers without calling the venue.
	conn := connector.NewBinance(&connector.Credentials{APIKey: "k", APISecret: "s"})
	funding, readsFunding, err := recountFunding(context.Background(), conn, time.Now().Add(-24*time.Hour))
	if err != nil || readsFunding || funding != nil {
		t.Fatalf("recountFunding = %v, %v, %v; want not read", funding, readsFunding, err)
	}

	d1 := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	stored := &repository.MarketBreakdown{
		Swap:   &repository.MarketMetrics{Equity: 950, Trades: 1, Volume: 100, TradingFees: 0.04, FundingFees: 1.5},
		Global: &repository.MarketMetrics{Equity: 950, Trades: 1, Volume: 100, TradingFees: 0.04, FundingFees: 1.5},
	}
	rows := []*repository.Snapshot{
		{Timestamp: d1, TotalEquity: 900, FromExternalRebuilder: true},
		{ID: "live", Timestamp: d2, TotalEquity: 950, Breakdown: stored},
	}
	trades := []*connector.Trade{
		{ID: "a", Price: 100, Quantity: 1, Fee: 0.04, Side: "buy", MarketType: connector.MarketSwap, Timestamp: d1.Add(time.Hour)},
		{ID: "b", Price: 100, Quantity: 1, Fee: 0.04, Side: "sell", MarketType: connector.MarketSwap, Timestamp: d1.Add(2 * time.Hour)},
	}
	d := (&SyncService{}).recountDay(rows, 1, "binance", []string{connector.MarketSwap}, trades, funding, readsFunding)
	if d.breakdown.Swap.Trades != 2 {
		t.Fatalf("swap trades = %d, want the 2 fills recounted", d.breakdown.Swap.Trades)
	}
	if d.breakdown.Swap.FundingFees != 1.5 || d.NewFundingFees != 1.5 {
		t.Fatalf("funding = %v / %v, want the stored 1.5 kept", d.breakdown.Swap.FundingFees, d.NewFundingFees)
	}
}

// The recount rewrites swap alone, so it asks for swap fills alone: on a
// Binance Portfolio Margin account the margin fills it would drop are most
// of a long read.
func TestRecountAsksForTheMarketsItRewrites(t *testing.T) {
	scope, _ := recountScopeFor("binance")
	ctx := recountTradesContext(context.Background(), scope)
	if !connector.TradeMarketWanted(ctx, connector.MarketSwap) {
		t.Fatal("swap fills not wanted by the recount that rewrites them")
	}
	if connector.TradeMarketWanted(ctx, connector.MarketSpot) {
		t.Fatal("spot and margin fills asked for by a recount that drops them")
	}
	if !connector.TradeMarketWanted(context.Background(), connector.MarketSpot) {
		t.Fatal("an unscoped read must want every market")
	}
}

// The recount files funding on the same whole UTC days as the live read: a
// settlement stamped at D 00:00 belongs to the row of D+1, never to both.
func TestRecountDayFilesAMidnightSettlementOnTheNextRow(t *testing.T) {
	d1 := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	d2, d3 := d1.Add(24*time.Hour), d1.Add(48*time.Hour)
	rows := []*repository.Snapshot{recountRow(d1, 1000, 0, true), recountRow(d2, 1000, 0, false), recountRow(d3, 1000, 0, false)}
	funding := []*connector.FundingFee{
		{Amount: -1, Timestamp: d1},
		{Amount: -2, Timestamp: d2},
		{Amount: -4, Timestamp: d2.Add(16 * time.Hour)},
		{Amount: -8, Timestamp: d3},
	}
	s := &SyncService{}
	row2 := s.recountDay(rows, 1, "binance", []string{connector.MarketSwap}, nil, funding, true)
	row3 := s.recountDay(rows, 2, "binance", []string{connector.MarketSwap}, nil, funding, true)
	if row2.NewFundingFees != 1 || row3.NewFundingFees != 6 {
		t.Fatalf("funding = %v on D2, %v on D3; want 1 (D1 00:00) and 6 (D2 00:00 + 16:00)", row2.NewFundingFees, row3.NewFundingFees)
	}
	if lo, hi := fundingWindow(d1.Add(20*time.Second), d2.Add(25*time.Second)); !lo.Equal(reflowWindowStart(rows, 1)) || !hi.Equal(d2) {
		t.Fatalf("live window [%v, %v) differs from the recount's [%v, %v)", lo, hi, reflowWindowStart(rows, 1), d2)
	}
}

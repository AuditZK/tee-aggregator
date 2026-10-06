package service

import (
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
	if _, ok := recountScopeFor("hyperliquid"); ok {
		t.Fatal("hyperliquid recountable without a checked past-range reader")
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

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

	d := (&SyncService{}).recountDay(rows, 1, "bybit", trades, funding)
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
	d := (&SyncService{}).recountDay(rows, 0, "bybit", nil, nil)
	if d.Kept == "" || d.changed() || d.breakdown != nil {
		t.Fatalf("reconstructed day touched: %+v", d)
	}
}

func TestRecountOnlyVenuesThatReadAPastRange(t *testing.T) {
	if !recountsActivity("Bybit") || recountsActivity("binance") || recountsActivity("hyperliquid") {
		t.Fatal("recount allowlist drifted")
	}
}

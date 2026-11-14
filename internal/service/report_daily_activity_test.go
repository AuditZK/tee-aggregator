package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func activityDay(d, ex string, equity float64, breakdown *repository.MarketBreakdown) *repository.Snapshot {
	t, _ := time.Parse("2006-01-02", d)
	return &repository.Snapshot{Timestamp: t, Exchange: ex, Label: "acct", TotalEquity: equity, Breakdown: breakdown}
}

func TestDailyActivity_CommittedTurnoverAndAmounts(t *testing.T) {
	day1 := activityDay("2026-03-01", "bybit", 10000, &repository.MarketBreakdown{
		Spot: &repository.MarketMetrics{Equity: 4000, AvailableMargin: 4000},
		Swap: &repository.MarketMetrics{Equity: 6000, AvailableMargin: 2000, FundingFees: -3},
	})
	day2 := activityDay("2026-03-02", "bybit", 10000, &repository.MarketBreakdown{
		Spot: &repository.MarketMetrics{Equity: 4000, AvailableMargin: 1000},
		Swap: &repository.MarketMetrics{Equity: 6000, AvailableMargin: 3000, FundingFees: -2.5},
		// The TS aggregate of the markets above: counted once, through them.
		Global: &repository.MarketMetrics{Equity: 10000, AvailableMargin: 4000, FundingFees: -2.5},
	})
	day2.TotalVolume = 25000
	day2.TotalTrades = 12
	day2.TotalFees = 8.75

	returns := convertSnapshotsToDailyReturns([]*repository.Snapshot{day1, day2})
	if len(returns) != 1 {
		t.Fatalf("want one return, got %d", len(returns))
	}
	a := returns[0].activity
	if a.committed == nil || !near(*a.committed, 0.6, 1e-12) {
		t.Fatalf("committed = %v, want 0.6 (4000 free of 10000)", a.committed)
	}
	if a.turnover == nil || !near(*a.turnover, 2.5, 1e-12) {
		t.Fatalf("turnover = %v, want 2.5", a.turnover)
	}
	if a.trades == nil || *a.trades != 12 || *a.volume != 25000 || *a.tradingFees != 8.75 || *a.fundingFees != -2.5 {
		t.Fatalf("amounts = %+v", a)
	}
}

// A venue that publishes no balance would read as fully invested if a missing
// free margin counted as zero: the day is unknown instead.
func TestDailyActivity_NoBalancesIsUnknownNotFullyCommitted(t *testing.T) {
	day1 := activityDay("2026-03-01", "mt5", 10000, nil)
	day2 := activityDay("2026-03-02", "mt5", 10100, &repository.MarketBreakdown{
		CFD: &repository.MarketMetrics{Equity: 10100},
	})
	returns := convertSnapshotsToDailyReturns([]*repository.Snapshot{day1, day2})
	if returns[0].activity.committed != nil {
		t.Fatalf("committed = %v, want unknown", *returns[0].activity.committed)
	}

	noBreakdown := activityDay("2026-03-02", "mt5", 10100, nil)
	returns = convertSnapshotsToDailyReturns([]*repository.Snapshot{day1, noBreakdown})
	if returns[0].activity.committed != nil {
		t.Fatalf("committed without a breakdown = %v, want unknown", *returns[0].activity.committed)
	}
}

// IBKR Flex history carries the equity and nothing about the trades: a zero
// volume there is not a quiet day, it is an unmeasured one.
func TestDailyActivity_StatementHistoryHasNoTradeFigures(t *testing.T) {
	day1 := activityDay("2026-03-01", "ibkr", 50000, nil)
	day2 := activityDay("2026-03-02", "ibkr", 50500, &repository.MarketBreakdown{
		Stocks: &repository.MarketMetrics{Equity: 50500, AvailableMargin: 20200},
	})
	day2.IsHistorical = true

	a := convertSnapshotsToDailyReturns([]*repository.Snapshot{day1, day2})[0].activity
	if a.trades != nil || a.volume != nil || a.turnover != nil || a.tradingFees != nil || a.fundingFees != nil {
		t.Fatalf("statement history signed trade figures: %+v", a)
	}
	if a.committed == nil || !near(*a.committed, 0.6, 1e-12) {
		t.Fatalf("committed = %v, want 0.6 from the statement's cash", a.committed)
	}
}

// A connection silent for a day is forward-filled for the return, but there is
// no snapshot to read its balances from: the day is stated over the ones that
// reported.
func TestDailyActivity_SilentConnectionIsLeftOut(t *testing.T) {
	a1 := activityDay("2026-03-01", "bybit", 10000, &repository.MarketBreakdown{Swap: &repository.MarketMetrics{Equity: 10000, AvailableMargin: 5000}})
	b1 := activityDay("2026-03-01", "okx", 30000, &repository.MarketBreakdown{Swap: &repository.MarketMetrics{Equity: 30000, AvailableMargin: 30000}})
	a2 := activityDay("2026-03-02", "bybit", 10000, &repository.MarketBreakdown{Swap: &repository.MarketMetrics{Equity: 10000, AvailableMargin: 2500}})

	a := convertSnapshotsToDailyReturns([]*repository.Snapshot{a1, b1, a2})[0].activity
	if a.committed == nil || !near(*a.committed, 0.75, 1e-12) {
		t.Fatalf("committed = %v, want 0.75 over bybit alone", a.committed)
	}
}

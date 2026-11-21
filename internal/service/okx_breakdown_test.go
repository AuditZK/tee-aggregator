package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

// A connector that implements no BalanceByMarketFetcher has its equity filed
// under primaryMarketType, while its trades land in whatever market type the
// trades themselves carry. OKX's GetTrades reads the SWAP fills, so every trade
// is typed swap; left on the spot default, the equity filed away from the
// trades — one bucket held the equity with no trades, the other the trades with
// no equity, and per-market return divided by that zero. Equity must join the
// swap trades.
func TestOKXPrimaryMarketTypeIsSwap(t *testing.T) {
	if got := primaryMarketType("okx"); got != connector.MarketSwap {
		t.Fatalf("primaryMarketType(%q) = %q, want %q", "okx", got, connector.MarketSwap)
	}
}

// The property that matters is equity and trades meeting in the same aggregate,
// so assert it through the aggregation itself rather than trusting the table.
func TestOKXEquityAndTradesLandInOneBucket(t *testing.T) {
	svc := &SyncService{}
	trades := []*connector.Trade{
		{ID: "f1", Price: 100, Quantity: 2, Side: "buy", MarketType: connector.MarketSwap, Timestamp: time.Now()},
		{ID: "f2", Price: 101, Quantity: 1, Side: "sell", MarketType: connector.MarketSwap, Timestamp: time.Now()},
	}

	agg := svc.aggregateTrades(trades)
	tradeBucket := agg.getOrCreateMarket(connector.MarketSwap)
	if tradeBucket.trades != 2 {
		t.Fatalf("swap trades = %d, want 2", tradeBucket.trades)
	}

	equityBucket := agg.getOrCreateMarket(primaryMarketType("okx"))
	equityBucket.equity = 39142.95

	if tradeBucket.equity != 39142.95 {
		t.Fatal("OKX equity filed in a different bucket than its trades — spot holds equity with no trades, swap trades with no equity")
	}
}

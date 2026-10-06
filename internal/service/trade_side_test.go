package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

// The buy/sell split is read from these counters; a venue that spells the
// side in its own case must not leave every trade uncounted.
func TestAggregateTradesSplitsSideWhateverTheCase(t *testing.T) {
	svc := &SyncService{}
	now := time.Now()
	trades := []*connector.Trade{
		{ID: "1", Price: 10, Quantity: 1, Side: "Buy", MarketType: connector.MarketSwap, Timestamp: now},
		{ID: "2", Price: 10, Quantity: 2, Side: "BUY", MarketType: connector.MarketSwap, Timestamp: now},
		{ID: "3", Price: 10, Quantity: 3, Side: "Sell", MarketType: connector.MarketSwap, Timestamp: now},
		{ID: "4", Price: 10, Quantity: 4, Side: "short", MarketType: connector.MarketSwap, Timestamp: now},
	}

	m := svc.aggregateTrades(trades).getOrCreateMarket(connector.MarketSwap)
	if m.trades != 4 {
		t.Fatalf("trades = %d, want 4", m.trades)
	}
	if m.longTrades != 2 || m.shortTrades != 2 {
		t.Fatalf("long=%d short=%d, want 2/2", m.longTrades, m.shortTrades)
	}
	if m.longVolume != 30 || m.shortVolume != 70 {
		t.Fatalf("longVolume=%v shortVolume=%v, want 30/70", m.longVolume, m.shortVolume)
	}
}

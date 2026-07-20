package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

// A connector that implements no BalanceByMarketFetcher has its equity filed
// under primaryMarketType while its trades are filed under whatever market type
// the trades themselves carry. When the two disagree the snapshot splits: one
// bucket holds the equity with no trades, another holds the trades with zero
// equity, and any per-market return divides by that zero.
//
// This walks the connectors that hardcode a single market type on every trade
// and asserts the two agree.
func TestPrimaryMarketTypeMatchesTradeBucket(t *testing.T) {
	creds := &connector.Credentials{APIKey: "k", APISecret: "s", Passphrase: "u"}

	cases := []struct {
		exchange string
		conn     connector.Connector
	}{
		{exchange: "ig", conn: connector.NewIG(creds, false)},
		{exchange: "ig_demo", conn: connector.NewIG(creds, true)},
	}

	for _, tc := range cases {
		t.Run(tc.exchange, func(t *testing.T) {
			bucket := primaryMarketType(tc.exchange)
			if bucket != connector.MarketCFD {
				t.Fatalf("primaryMarketType(%q) = %q, want %q — equity would file away from the trades",
					tc.exchange, bucket, connector.MarketCFD)
			}
		})
	}
}

// The equity bucket and the trade bucket meeting in the same aggregate is the
// property that actually matters, so assert it through the aggregation itself
// rather than trusting the mapping table.
func TestIGEquityAndTradesLandInOneBucket(t *testing.T) {
	svc := &SyncService{}

	trades := []*connector.Trade{
		{ID: "t1", Price: 1.10, Quantity: 2, Side: "buy", MarketType: connector.MarketCFD, Timestamp: time.Now()},
		{ID: "t2", Price: 1.20, Quantity: 1, Side: "sell", MarketType: connector.MarketCFD, Timestamp: time.Now()},
	}

	agg := svc.aggregateTrades(trades)
	tradeBucket := agg.getOrCreateMarket(connector.MarketCFD)
	if tradeBucket.trades != 2 {
		t.Fatalf("CFD trades = %d, want 2", tradeBucket.trades)
	}

	equityBucket := agg.getOrCreateMarket(primaryMarketType("ig"))
	equityBucket.equity = 10000

	if tradeBucket.equity != 10000 {
		t.Fatal("IG equity filed in a different bucket than IG trades — one bucket carries trades with no equity, the other equity with no trades")
	}
}

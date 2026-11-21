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

// A CFD broker's overnight funding belongs with its CFD trades. Routed to the
// swap bucket it would sit alone in a market the account never traded — and
// since a funding-only bucket used to fail hasData(), it was then dropped at
// persistence: fetched, then thrown away.
func TestFundingFeesLandWithTheirTrades(t *testing.T) {
	cases := []struct {
		exchange string
		want     string
	}{
		{exchange: "ig", want: connector.MarketCFD},
		{exchange: "ig_demo", want: connector.MarketCFD},
		{exchange: "ctrader", want: connector.MarketCFD},
		{exchange: "mt5", want: connector.MarketCFD},
		// Perp venues fund swaps — today's behaviour, which must not move.
		{exchange: "hyperliquid", want: connector.MarketSwap},
		{exchange: "deribit", want: connector.MarketSwap},
		{exchange: "mexc", want: connector.MarketSwap},
	}

	for _, tc := range cases {
		t.Run(tc.exchange, func(t *testing.T) {
			if got := fundingMarketType(tc.exchange); got != tc.want {
				t.Fatalf("fundingMarketType(%q) = %q, want %q", tc.exchange, got, tc.want)
			}
		})
	}
}

// A position opened in an earlier window and still held charges funding with
// no trade and no equity of its own in that bucket. Dropping such a bucket
// discards a real cost that was already fetched.
func TestFundingOnlyBucketSurvivesPersistence(t *testing.T) {
	m := &marketAgg{fundingFees: -12.5}
	if !m.hasData() {
		t.Fatal("a bucket carrying only funding fees must persist — otherwise the cost is fetched and thrown away")
	}

	empty := &marketAgg{}
	if empty.hasData() {
		t.Fatal("a genuinely empty bucket must not persist")
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

package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

func TestAggregateSyncResults_PartialSuccessUsesLatestSnapshot(t *testing.T) {
	t1 := time.Date(2026, 2, 27, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC)

	results := []*SyncResult{
		{
			UserUID:           "user_abc",
			Exchange:          "binance",
			Label:             "main",
			Success:           true,
			TradeCount:        3,
			SnapshotEquity:    1000,
			SnapshotTimestamp: t1,
		},
		{
			UserUID:  "user_abc",
			Exchange: "binance",
			Label:    "secondary",
			Success:  false,
			Error:    "connector timeout",
		},
		{
			UserUID:           "user_abc",
			Exchange:          "binance",
			Label:             "tertiary",
			Success:           true,
			TradeCount:        5,
			SnapshotEquity:    1200,
			SnapshotTimestamp: t2,
		},
	}

	out := aggregateSyncResults("user_abc", "binance", results)
	if !out.Success {
		t.Fatalf("expected success=true")
	}
	if out.TradeCount != 8 {
		t.Fatalf("unexpected trade count: got %d want 8", out.TradeCount)
	}
	if !out.SnapshotTimestamp.Equal(t2) {
		t.Fatalf("expected latest timestamp %s, got %s", t2, out.SnapshotTimestamp)
	}
	if out.SnapshotEquity != 1200 {
		t.Fatalf("expected latest equity 1200, got %f", out.SnapshotEquity)
	}
	if out.Error == "" {
		t.Fatal("expected aggregated error for partial failures")
	}
}

func TestAggregateSyncResults_AllFailedReturnsError(t *testing.T) {
	results := []*SyncResult{
		{Exchange: "binance", Label: "main", Error: "e1"},
		{Exchange: "binance", Label: "secondary", Error: "e2"},
	}

	out := aggregateSyncResults("user_x", "binance", results)
	if out.Success {
		t.Fatalf("expected success=false")
	}
	if out.Error == "" {
		t.Fatalf("expected non-empty error")
	}
}

func TestIsDueByInterval(t *testing.T) {
	now := time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC)
	before45m := now.Add(-45 * time.Minute)
	before30m := now.Add(-30 * time.Minute)
	before1d := now.Add(-24 * time.Hour)
	sameDayEarlier := time.Date(2026, 2, 28, 1, 0, 0, 0, time.UTC) // same calendar day
	future := now.Add(10 * time.Minute)

	tests := []struct {
		name            string
		lastSync        *time.Time
		intervalMinutes int
		want            bool
	}{
		{
			name:            "no last sync is due",
			lastSync:        nil,
			intervalMinutes: 60,
			want:            true,
		},
		{
			name:            "elapsed greater than interval is due",
			lastSync:        &before45m,
			intervalMinutes: 30,
			want:            true,
		},
		{
			name:            "elapsed equal to interval is due",
			lastSync:        &before30m,
			intervalMinutes: 30,
			want:            true,
		},
		{
			name:            "elapsed lower than interval is not due",
			lastSync:        &before30m,
			intervalMinutes: 60,
			want:            false,
		},
		{
			name:            "default interval applies when non-positive",
			lastSync:        &before1d,
			intervalMinutes: 0,
			want:            true,
		},
		{
			name:            "default interval not reached (same calendar day)",
			lastSync:        &sameDayEarlier,
			intervalMinutes: -5,
			want:            false,
		},
		{
			name:            "future sync timestamp is not due",
			lastSync:        &future,
			intervalMinutes: 5,
			want:            false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isDueByInterval(tc.lastSync, tc.intervalMinutes, now)
			if got != tc.want {
				t.Fatalf("isDueByInterval() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAggregateTrades_SeparatesStocksFromSpot(t *testing.T) {
	svc := &SyncService{}
	trades := []*connector.Trade{
		{
			ID:         "t-stock",
			Price:      100,
			Quantity:   2,
			Fee:        1,
			MarketType: connector.MarketStocks,
		},
		{
			ID:         "t-spot",
			Price:      50,
			Quantity:   3,
			Fee:        0.5,
			MarketType: connector.MarketSpot,
		},
	}

	agg := svc.aggregateTrades(trades)
	repo := agg.toRepo(0, 0, 0)

	if repo.Stocks == nil {
		t.Fatal("expected stocks metrics to be populated")
	}
	if repo.Stocks.Trades != 1 {
		t.Fatalf("expected stocks trades=1, got %d", repo.Stocks.Trades)
	}
	if repo.Spot == nil {
		t.Fatal("expected spot metrics to be populated")
	}
	if repo.Spot.Trades != 1 {
		t.Fatalf("expected spot trades=1, got %d", repo.Spot.Trades)
	}
	if got := agg.totalVolume(); got != 350 {
		t.Fatalf("expected total volume=350, got %f", got)
	}
}

func TestAggregateTrades_LongShortTracking(t *testing.T) {
	svc := &SyncService{}
	trades := []*connector.Trade{
		{
			ID:         "buy-1",
			Price:      100,
			Quantity:   2,
			Side:       "buy",
			MarketType: connector.MarketSpot,
		},
		{
			ID:         "long-1",
			Price:      200,
			Quantity:   1,
			Side:       "long",
			MarketType: connector.MarketSpot,
		},
		{
			ID:         "sell-1",
			Price:      150,
			Quantity:   3,
			Side:       "sell",
			MarketType: connector.MarketSpot,
		},
		{
			ID:         "short-1",
			Price:      50,
			Quantity:   4,
			Side:       "short",
			MarketType: connector.MarketFutures,
		},
	}

	agg := svc.aggregateTrades(trades)

	// Spot: 2 long trades (buy + long), 1 short trade (sell)
	if agg.spot.longTrades != 2 {
		t.Fatalf("spot longTrades: got %d, want 2", agg.spot.longTrades)
	}
	if agg.spot.shortTrades != 1 {
		t.Fatalf("spot shortTrades: got %d, want 1", agg.spot.shortTrades)
	}
	// longVolume = 100*2 + 200*1 = 400
	if agg.spot.longVolume != 400 {
		t.Fatalf("spot longVolume: got %f, want 400", agg.spot.longVolume)
	}
	// shortVolume = 150*3 = 450
	if agg.spot.shortVolume != 450 {
		t.Fatalf("spot shortVolume: got %f, want 450", agg.spot.shortVolume)
	}

	// Futures: 1 short trade
	if agg.futures.shortTrades != 1 {
		t.Fatalf("futures shortTrades: got %d, want 1", agg.futures.shortTrades)
	}
	if agg.futures.shortVolume != 200 {
		t.Fatalf("futures shortVolume: got %f, want 200", agg.futures.shortVolume)
	}
	if agg.futures.longTrades != 0 {
		t.Fatalf("futures longTrades: got %d, want 0", agg.futures.longTrades)
	}
}

func TestAggregateTrades_FundingFees(t *testing.T) {
	svc := &SyncService{}
	trades := []*connector.Trade{
		{
			ID:         "t1",
			Price:      100,
			Quantity:   1,
			Fee:        0.5,
			Side:       "buy",
			MarketType: connector.MarketSwap,
		},
	}

	agg := svc.aggregateTrades(trades)

	// fundingFees is not populated by aggregateTrades (trades have no funding fee field),
	// so it should remain zero, but the struct should carry the field through toRepoMetrics.
	if agg.swap.fundingFees != 0 {
		t.Fatalf("swap fundingFees: got %f, want 0", agg.swap.fundingFees)
	}
	repo := agg.swap.toRepoMetrics()
	if repo.FundingFees != 0 {
		t.Fatalf("repo FundingFees: got %f, want 0", repo.FundingFees)
	}
}

func TestToRepoMetrics_IncludesEquityAndMargin(t *testing.T) {
	ma := marketAgg{
		equity:          5000,
		availableMargin: 3000,
		volume:          10000,
		trades:          5,
		fees:            25,
		fundingFees:     1.5,
		longTrades:      3,
		shortTrades:     2,
		longVolume:      7000,
		shortVolume:     3000,
	}

	repo := ma.toRepoMetrics()

	if repo.Equity != 5000 {
		t.Fatalf("Equity: got %f, want 5000", repo.Equity)
	}
	if repo.AvailableMargin != 3000 {
		t.Fatalf("AvailableMargin: got %f, want 3000", repo.AvailableMargin)
	}
	if repo.Volume != 10000 {
		t.Fatalf("Volume: got %f, want 10000", repo.Volume)
	}
	if repo.Trades != 5 {
		t.Fatalf("Trades: got %d, want 5", repo.Trades)
	}
	if repo.TradingFees != 25 {
		t.Fatalf("TradingFees: got %f, want 25", repo.TradingFees)
	}
	if repo.FundingFees != 1.5 {
		t.Fatalf("FundingFees: got %f, want 1.5", repo.FundingFees)
	}
	if repo.LongTrades != 3 {
		t.Fatalf("LongTrades: got %d, want 3", repo.LongTrades)
	}
	if repo.ShortTrades != 2 {
		t.Fatalf("ShortTrades: got %d, want 2", repo.ShortTrades)
	}
	if repo.LongVolume != 7000 {
		t.Fatalf("LongVolume: got %f, want 7000", repo.LongVolume)
	}
	if repo.ShortVolume != 3000 {
		t.Fatalf("ShortVolume: got %f, want 3000", repo.ShortVolume)
	}
}

func TestHasData_TradesOnly(t *testing.T) {
	ma := marketAgg{trades: 3}
	if !ma.hasData() {
		t.Fatal("expected hasData()=true when trades > 0")
	}
}

func TestHasData_EquityOnly(t *testing.T) {
	ma := marketAgg{equity: 1000}
	if !ma.hasData() {
		t.Fatal("expected hasData()=true when equity > 0")
	}
}

func TestHasData_Empty(t *testing.T) {
	ma := marketAgg{}
	if ma.hasData() {
		t.Fatal("expected hasData()=false when all zeros")
	}
}

func TestGetOrCreateMarket_AllTypes(t *testing.T) {
	tests := []struct {
		name       string
		marketType string
		checkField func(*aggregatedBreakdown) *marketAgg
	}{
		{"stocks", connector.MarketStocks, func(a *aggregatedBreakdown) *marketAgg { return &a.stocks }},
		{"spot", connector.MarketSpot, func(a *aggregatedBreakdown) *marketAgg { return &a.spot }},
		{"swap", connector.MarketSwap, func(a *aggregatedBreakdown) *marketAgg { return &a.swap }},
		{"futures", connector.MarketFutures, func(a *aggregatedBreakdown) *marketAgg { return &a.futures }},
		{"options", connector.MarketOptions, func(a *aggregatedBreakdown) *marketAgg { return &a.options }},
		{"margin", connector.MarketMargin, func(a *aggregatedBreakdown) *marketAgg { return &a.margin }},
		{"earn", connector.MarketEarn, func(a *aggregatedBreakdown) *marketAgg { return &a.earn }},
		{"cfd", connector.MarketCFD, func(a *aggregatedBreakdown) *marketAgg { return &a.cfd }},
		{"forex", connector.MarketForex, func(a *aggregatedBreakdown) *marketAgg { return &a.forex }},
		{"commodities", connector.MarketCommodities, func(a *aggregatedBreakdown) *marketAgg { return &a.commodities }},
		{"unknown defaults to spot", "unknown_market", func(a *aggregatedBreakdown) *marketAgg { return &a.spot }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agg := &aggregatedBreakdown{}
			got := agg.getOrCreateMarket(tc.marketType)
			expected := tc.checkField(agg)

			// Write a marker to verify they point to the same field
			got.equity = 42
			if expected.equity != 42 {
				t.Fatalf("getOrCreateMarket(%q) returned wrong field", tc.marketType)
			}
		})
	}
}

func TestEnrichBreakdownWithBalances(t *testing.T) {
	svc := &SyncService{}
	agg := &aggregatedBreakdown{}

	balances := []*connector.MarketBalance{
		{MarketType: connector.MarketFutures, Equity: 5000, AvailableMargin: 3000},
		{MarketType: connector.MarketSpot, Equity: 2000, AvailableMargin: 0},
		{MarketType: connector.MarketSwap, Equity: 0, AvailableMargin: 0}, // should be skipped
	}

	svc.enrichBreakdownWithBalances(agg, balances)

	if agg.futures.equity != 5000 {
		t.Fatalf("futures equity: got %f, want 5000", agg.futures.equity)
	}
	if agg.futures.availableMargin != 3000 {
		t.Fatalf("futures availableMargin: got %f, want 3000", agg.futures.availableMargin)
	}
	if agg.spot.equity != 2000 {
		t.Fatalf("spot equity: got %f, want 2000", agg.spot.equity)
	}
	if agg.spot.availableMargin != 0 {
		t.Fatalf("spot availableMargin: got %f, want 0", agg.spot.availableMargin)
	}
	// swap should not have been touched (both zero)
	if agg.swap.equity != 0 {
		t.Fatalf("swap equity: got %f, want 0 (should be skipped)", agg.swap.equity)
	}
}

func TestToRepo_EmitsMarketsWithEquityButNoTrades(t *testing.T) {
	agg := &aggregatedBreakdown{}
	// Set equity but no trades on futures
	agg.futures.equity = 10000
	agg.futures.availableMargin = 5000

	repo := agg.toRepo(0, 0, 0)

	if repo.Futures == nil {
		t.Fatal("expected Futures to be present when equity > 0 and trades == 0")
	}
	if repo.Futures.Equity != 10000 {
		t.Fatalf("Futures.Equity: got %f, want 10000", repo.Futures.Equity)
	}
	if repo.Futures.AvailableMargin != 5000 {
		t.Fatalf("Futures.AvailableMargin: got %f, want 5000", repo.Futures.AvailableMargin)
	}
	if repo.Futures.Trades != 0 {
		t.Fatalf("Futures.Trades: got %d, want 0", repo.Futures.Trades)
	}

	// Markets with no data should remain nil
	if repo.Stocks != nil {
		t.Fatal("expected Stocks to be nil when no data")
	}
}

func TestAppendUnique(t *testing.T) {
	s := appendUnique(nil, "a")
	s = appendUnique(s, "b")
	s = appendUnique(s, "a") // duplicate
	if len(s) != 2 {
		t.Fatalf("expected 2 unique, got %d", len(s))
	}
}

func TestAggregateTrades_FundingFeesCarry(t *testing.T) {
	svc := &SyncService{}
	trades := []*connector.Trade{
		{Price: 100, Quantity: 1, Fee: 0.5, MarketType: connector.MarketSwap, Side: "buy"},
	}
	agg := svc.aggregateTrades(trades)
	agg.swap.fundingFees = 12.5 // Set manually (populated by FundingFeesFetcher in sync)
	repo := agg.toRepo(0, 0, 0)
	if repo.Swap == nil {
		t.Fatal("expected swap metrics")
	}
	if repo.Swap.FundingFees != 12.5 {
		t.Fatalf("expected fundingFees=12.5, got %f", repo.Swap.FundingFees)
	}
}

func TestEarnBalanceEnrichment(t *testing.T) {
	agg := &aggregatedBreakdown{}
	agg.earn.equity = 5000
	repo := agg.toRepo(0, 0, 0)
	if repo.Earn == nil {
		t.Fatal("expected earn in breakdown when equity > 0")
	}
	if repo.Earn.Equity != 5000 {
		t.Fatalf("expected earn equity=5000, got %f", repo.Earn.Equity)
	}
}

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/rebuilderclient"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
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

// IBKR Flex backfill: the today bucket is owned by the live branch (full
// trade detail) and must never be overwritten from a Flex daily summary.
func TestBuildHistoricalSnapshots_SkipsToday(t *testing.T) {
	today := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	connMeta := &repository.ExchangeConnection{
		UserUID: "user_42", Exchange: "ibkr", Label: "main",
	}
	hs := []*connector.HistoricalSnapshot{
		{Date: today.AddDate(0, 0, -2), TotalEquity: 1000},
		{Date: today.AddDate(0, 0, -1), TotalEquity: 1100},
		{Date: today, TotalEquity: 1200}, // must be skipped
	}

	snapshots, skipped := buildHistoricalSnapshots(connMeta, hs, today, false)

	if skipped != 1 {
		t.Fatalf("expected 1 skipped (today), got %d", skipped)
	}
	if len(snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snapshots))
	}
	for _, s := range snapshots {
		if s.Timestamp.Equal(today) {
			t.Fatalf("today's bucket leaked into historical output")
		}
	}
}

// All snapshots produced by the Flex backfill carry IsHistorical=true so
// consumers can tell reconstructed equity-only days from live full-trade
// days.
func TestBuildHistoricalSnapshots_MarksHistorical(t *testing.T) {
	today := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	connMeta := &repository.ExchangeConnection{
		UserUID: "user_42", Exchange: "ibkr", Label: "",
	}
	hs := []*connector.HistoricalSnapshot{
		{Date: today.AddDate(0, 0, -3), TotalEquity: 900},
		{Date: today.AddDate(0, 0, -2), TotalEquity: 1000},
	}

	snapshots, _ := buildHistoricalSnapshots(connMeta, hs, today, false)

	for _, s := range snapshots {
		if !s.IsHistorical {
			t.Fatalf("expected IsHistorical=true on Flex-derived snapshot at %s", s.Timestamp)
		}
		if s.UserUID != "user_42" || s.Exchange != "ibkr" {
			t.Fatalf("connection metadata not propagated: %+v", s)
		}
	}
}

// Per-asset breakdown from Flex must be preserved end-to-end and the global
// aggregate must be populated (the TS frontend reads breakdown.global.equity
// for IBKR — without it the dashboard shows 0).
func TestBuildHistoricalSnapshots_PreservesBreakdown(t *testing.T) {
	today := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	connMeta := &repository.ExchangeConnection{
		UserUID: "user_42", Exchange: "ibkr", Label: "main",
	}
	day := today.AddDate(0, 0, -1)
	hs := []*connector.HistoricalSnapshot{
		{
			Date:        day,
			TotalEquity: 5000,
			Breakdown: map[string]*connector.MarketBalance{
				connector.MarketStocks:  {Equity: 3000, AvailableMargin: 1500},
				connector.MarketFutures: {Equity: 2000, AvailableMargin: 500},
			},
		},
	}

	snapshots, _ := buildHistoricalSnapshots(connMeta, hs, today, false)
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
	got := snapshots[0]
	if got.Breakdown.Stocks == nil || got.Breakdown.Stocks.Equity != 3000 {
		t.Fatalf("stocks breakdown lost")
	}
	if got.Breakdown.Futures == nil || got.Breakdown.Futures.Equity != 2000 {
		t.Fatalf("futures breakdown lost")
	}
	if got.Breakdown.Global == nil || got.Breakdown.Global.Equity != 5000 {
		t.Fatalf("global aggregate missing or wrong: %+v", got.Breakdown.Global)
	}
	// Sum of per-market AvailableMargin = 2000
	if got.Breakdown.Global.AvailableMargin != 2000 {
		t.Fatalf("global available margin: got %f want 2000", got.Breakdown.Global.AvailableMargin)
	}
}

func TestBuildHistoricalSnapshots_EmptyInput(t *testing.T) {
	connMeta := &repository.ExchangeConnection{UserUID: "u", Exchange: "ibkr"}
	snapshots, skipped := buildHistoricalSnapshots(connMeta, nil, time.Now().UTC(), false)
	if len(snapshots) != 0 || skipped != 0 {
		t.Fatalf("expected empty result for empty input, got snapshots=%d skipped=%d", len(snapshots), skipped)
	}
}

// SEC-001: snapshots reconstructed by the external rebuilder must be stamped
// FromExternalRebuilder=true so GetVerifiableByUserAndDateRange keeps them out
// of signed reports. In-enclave (IBKR Flex) history stays false.
func TestBuildHistoricalSnapshots_StampsExternalRebuilderOrigin(t *testing.T) {
	today := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	connMeta := &repository.ExchangeConnection{UserUID: "user_42", Exchange: "hyperliquid", Label: "main"}
	hs := []*connector.HistoricalSnapshot{
		{Date: today.AddDate(0, 0, -2), TotalEquity: 1000},
		{Date: today.AddDate(0, 0, -1), TotalEquity: 1100},
	}

	external, _ := buildHistoricalSnapshots(connMeta, hs, today, true)
	if len(external) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(external))
	}
	for _, s := range external {
		if !s.FromExternalRebuilder {
			t.Fatalf("rebuilder-sourced snapshot at %s must have FromExternalRebuilder=true", s.Timestamp)
		}
	}

	inEnclave, _ := buildHistoricalSnapshots(connMeta, hs, today, false)
	for _, s := range inEnclave {
		if s.FromExternalRebuilder {
			t.Fatalf("in-enclave snapshot at %s must have FromExternalRebuilder=false", s.Timestamp)
		}
	}
}

// History-reconstruction orchestration tests.
//
// reconstructHistory is the routing decision that governs whether plaintext
// credentials leave the SEV-SNP enclave: a connector implementing
// HistoricalSnapshotProvider reconstructs in-enclave, every other connector
// hands off to the external rebuilder. These tests exercise that routing
// with connector + rebuilder doubles (no DB). Snapshot-shaping correctness
// is covered by the TestBuildHistoricalSnapshots_* tests above.

// fakeHistConnector implements connector.Connector but NOT
// HistoricalSnapshotProvider — a non-IBKR exchange that must route to the
// external rebuilder.
type fakeHistConnector struct{ exchange string }

func (f *fakeHistConnector) GetBalance(context.Context) (*connector.Balance, error) {
	return &connector.Balance{}, nil
}
func (f *fakeHistConnector) GetPositions(context.Context) ([]*connector.Position, error) {
	return nil, nil
}
func (f *fakeHistConnector) GetTrades(context.Context, time.Time, time.Time) ([]*connector.Trade, error) {
	return nil, nil
}
func (f *fakeHistConnector) TestConnection(context.Context) error { return nil }
func (f *fakeHistConnector) Exchange() string                     { return f.exchange }

// fakeHistProvider also implements HistoricalSnapshotProvider — a ZK-native
// connector (IBKR) that must reconstruct in-enclave.
type fakeHistProvider struct {
	fakeHistConnector
	called bool
}

func (f *fakeHistProvider) GetHistoricalSnapshots(context.Context, time.Time) ([]*connector.HistoricalSnapshot, error) {
	f.called = true
	return nil, nil
}

var (
	_ connector.Connector                  = (*fakeHistConnector)(nil)
	_ connector.Connector                  = (*fakeHistProvider)(nil)
	_ connector.HistoricalSnapshotProvider = (*fakeHistProvider)(nil)
)

// A ZK-native provider must reconstruct in-enclave and must NEVER reach the
// external rebuilder — that would push IBKR-class credentials out of the
// SEV-SNP perimeter (SEC-ZK-001).
func TestReconstructHistory_ZKNativeProviderStaysInEnclave(t *testing.T) {
	var rebuilderHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rebuilderHits++
		_, _ = w.Write([]byte(`{"snapshots":[]}`))
	}))
	defer srv.Close()

	svc := &SyncService{
		logger:    zap.NewNop(),
		rebuilder: rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()),
	}
	provider := &fakeHistProvider{fakeHistConnector: fakeHistConnector{exchange: "ibkr"}}
	connMeta := &repository.ExchangeConnection{UserUID: "u1", Exchange: "ibkr", Label: "main"}

	svc.reconstructHistory(context.Background(), connMeta, provider, &Credentials{APIKey: "k", APISecret: "s"})

	if !provider.called {
		t.Error("ZK-native provider: in-enclave GetHistoricalSnapshots was never called")
	}
	if rebuilderHits != 0 {
		t.Errorf("SEC-ZK-001 violation: a HistoricalSnapshotProvider connection hit the external "+
			"rebuilder %d time(s) — IBKR-class credentials must never leave the enclave", rebuilderHits)
	}
}

// A non-provider connector with no usable rebuilder must be a silent no-op:
// enclave-only dev environments stay functional, no panic, no dispatch.
func TestReconstructHistory_NonProviderWithoutRebuilderIsNoop(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rebuilder *rebuilderclient.Client
	}{
		{"rebuilder nil", nil},
		{"rebuilder unconfigured", rebuilderclient.New("", "", zap.NewNop())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &SyncService{logger: zap.NewNop(), rebuilder: tc.rebuilder}
			conn := &fakeHistConnector{exchange: "hyperliquid"}
			connMeta := &repository.ExchangeConnection{UserUID: "u1", Exchange: "hyperliquid", Label: "main"}
			// Must return cleanly — no panic, no dispatch.
			svc.reconstructHistory(context.Background(), connMeta, conn, &Credentials{APIKey: "wallet"})
		})
	}
}

// A non-provider connector with a configured rebuilder must POST to it,
// carrying the X-Internal-Token and the plaintext credentials.
func TestReconstructHistory_NonProviderDispatchesToRebuilder(t *testing.T) {
	var gotToken string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-Internal-Token")
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exchange":"hyperliquid","count":0,"durationMs":5,"snapshots":[]}`))
	}))
	defer srv.Close()

	svc := &SyncService{
		logger:    zap.NewNop(),
		rebuilder: rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()),
	}
	conn := &fakeHistConnector{exchange: "hyperliquid"}
	connMeta := &repository.ExchangeConnection{UserUID: "u1", Exchange: "hyperliquid", Label: "main"}

	svc.reconstructHistory(context.Background(), connMeta, conn,
		&Credentials{APIKey: "0xWALLET", APISecret: "secret-xyz", Passphrase: "pp"})

	if gotToken != "internal-token-0123456789" {
		t.Errorf("X-Internal-Token not propagated to rebuilder: got %q", gotToken)
	}
	body := string(gotBody)
	if !strings.Contains(body, "0xWALLET") || !strings.Contains(body, "secret-xyz") {
		t.Errorf("rebuild request did not carry the credentials to the rebuilder")
	}
}

// buildHistoricalSnapshots must map every market type Flex / the rebuilder
// can return; the tests above only exercise stocks + futures.
func TestBuildHistoricalSnapshots_AllMarketTypes(t *testing.T) {
	today := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	connMeta := &repository.ExchangeConnection{UserUID: "u", Exchange: "ibkr", Label: "main"}
	hs := []*connector.HistoricalSnapshot{{
		Date:        today.AddDate(0, 0, -1),
		TotalEquity: 100, RealizedBalance: 70,
		TotalTrades: 9, TotalVolume: 1234, TotalFees: 5.5,
		Breakdown: map[string]*connector.MarketBalance{
			connector.MarketStocks:  {Equity: 1, AvailableMargin: 1},
			connector.MarketOptions: {Equity: 2, AvailableMargin: 2},
			connector.MarketFutures: {Equity: 3, AvailableMargin: 3},
			connector.MarketCFD:     {Equity: 4, AvailableMargin: 4},
			connector.MarketForex:   {Equity: 5, AvailableMargin: 5},
			connector.MarketSwap:    {Equity: 6, AvailableMargin: 6},
			connector.MarketSpot:    {Equity: 7, AvailableMargin: 7},
		},
	}}

	snapshots, _ := buildHistoricalSnapshots(connMeta, hs, today, false)
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
	b := snapshots[0].Breakdown
	if b.Stocks == nil || b.Options == nil || b.Futures == nil ||
		b.CFD == nil || b.Forex == nil || b.Swap == nil || b.Spot == nil {
		t.Fatalf("not all market types mapped: %+v", b)
	}
	if b.Global.AvailableMargin != 28 { // 1+2+3+4+5+6+7
		t.Errorf("global available margin: got %f want 28", b.Global.AvailableMargin)
	}
	if got := snapshots[0]; got.UnrealizedPnL != 30 { // TotalEquity - RealizedBalance
		t.Errorf("unrealized pnl: got %f want 30", got.UnrealizedPnL)
	}
	if got := snapshots[0]; got.TotalTrades != 9 || got.TotalVolume != 1234 || got.TotalFees != 5.5 {
		t.Errorf("trade aggregates not propagated: trades=%d volume=%f fees=%f",
			got.TotalTrades, got.TotalVolume, got.TotalFees)
	}
}

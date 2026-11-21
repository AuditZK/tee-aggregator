package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// A full cTrader reconstruction over a connection that already holds its
// history, on the production schema, where nothing marks which rows a
// reconstruction wrote. The broker's ledger no longer matches what is stored:
// it starts later, on a demo reset booked as a deposit of the whole new
// balance, and it also reaches further back. In production this deleted the
// reset day and wrote the raw deposit next to it. The stored history must come
// out untouched.
func TestDBCTraderFullReconstructionLeavesAStoredHistoryAlone(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ctrader", "main", dbKey, dbSecret)
	connMeta, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "ctrader", "main")
	if err != nil {
		t.Fatalf("read connection: %v", err)
	}

	today := startOfTodayUTC()
	d := func(n int) time.Time { return today.AddDate(0, 0, -10+n) }
	old := func(n int, equity, deposits float64) *repository.Snapshot {
		return &repository.Snapshot{
			UserUID: dbUser, Exchange: "ctrader", Label: "main", Timestamp: d(n),
			TotalEquity: equity, RealizedBalance: equity, Deposits: deposits,
			Breakdown: &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: equity}},
		}
	}
	live := func(n int, realized, upnl float64) *repository.Snapshot {
		return &repository.Snapshot{
			UserUID: dbUser, Exchange: "ctrader", Label: "main", Timestamp: d(n),
			TotalEquity: realized + upnl, RealizedBalance: realized, UnrealizedPnL: upnl,
			Breakdown: &repository.MarketBreakdown{
				CFD:    &repository.MarketMetrics{Equity: realized + upnl, AvailableMargin: 400},
				Global: &repository.MarketMetrics{Equity: realized + upnl},
			},
		}
	}
	// Synthetic account: 1000 funded on d(2), reset to 10000 on d(4) (net
	// 8900 after trading to 1100), live since d(6).
	seed := []*repository.Snapshot{
		old(2, 1000, 1000),
		old(3, 1100, 0),
		old(4, 10000, 8900),
		old(5, 10000, 0),
		live(6, 10000, 30),
		live(7, 10000, 0),
	}
	if err := h.snaps.UpsertBatch(h.ctx, seed); err != nil {
		t.Fatalf("seed history: %v", err)
	}

	rebuilt := []*connector.HistoricalSnapshot{
		{Date: d(0), TotalEquity: 500, RealizedBalance: 500, Deposits: 500},
		{Date: d(1), TotalEquity: 500, RealizedBalance: 500},
		{Date: d(5), TotalEquity: 10000, RealizedBalance: 10000, Deposits: 10000},
		{Date: d(6), TotalEquity: 10000, RealizedBalance: 10000},
		{Date: d(7), TotalEquity: 10000, RealizedBalance: 10000},
	}
	if err := h.sync.persistHistoricalSnapshots(h.ctx, connMeta, rebuilt, false, sourceInEnclave, reconstructOpts{}); err != nil {
		t.Fatalf("persist: %v", err)
	}

	rows, err := h.snaps.GetByUserAndDateRange(h.ctx, dbUser, d(0), today)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	byDay := map[time.Time]*repository.Snapshot{}
	for _, r := range rows {
		byDay[r.Timestamp.UTC()] = r
	}
	if len(byDay) != len(seed) {
		t.Fatalf("%d rows after the reconstruction, want the %d stored", len(byDay), len(seed))
	}
	for _, want := range seed {
		got := byDay[want.Timestamp]
		if got == nil {
			t.Fatalf("%s was deleted", want.Timestamp.Format(time.DateOnly))
		}
		if got.TotalEquity != want.TotalEquity || got.Deposits != want.Deposits || got.UnrealizedPnL != want.UnrealizedPnL {
			t.Fatalf("%s rewritten: equity %v deposits %v uPnL %v, stored %v / %v / %v",
				want.Timestamp.Format(time.DateOnly), got.TotalEquity, got.Deposits, got.UnrealizedPnL,
				want.TotalEquity, want.Deposits, want.UnrealizedPnL)
		}
	}
}

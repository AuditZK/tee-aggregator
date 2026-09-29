package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// A full cTrader reconstruction over a history written with the old dating
// (each row keyed by the day that had just closed) and live days since the
// connection. On the production schema nothing marks which rows are which, so
// this runs against it.
//
// It must vacate the old inception row, which would otherwise count the
// inception deposit a second time next to the new first row, rewrite the
// other reconstructed days, and keep what the live sync measured.
func TestDBCTraderFullReconstructionMigratesTheOldDating(t *testing.T) {
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
	// Synthetic account: 1000 deposited on d(0), 100 earned on d(2), connected
	// on d(3). The old dating put each day's close under that day's date.
	if err := h.snaps.UpsertBatch(h.ctx, []*repository.Snapshot{
		old(0, 1000, 1000),
		old(1, 1000, 0),
		old(2, 1100, 0),
		live(3, 1100, 30),
		live(4, 1100, 0),
	}); err != nil {
		t.Fatalf("seed history: %v", err)
	}

	// The same account under the new dating: a row holds the midnight that
	// opens it and the flows of the day before.
	rebuilt := []*connector.HistoricalSnapshot{
		{Date: d(1), TotalEquity: 1000, RealizedBalance: 1000, Deposits: 1000},
		{Date: d(2), TotalEquity: 1000, RealizedBalance: 1000},
		{Date: d(3), TotalEquity: 1100, RealizedBalance: 1100},
		{Date: d(4), TotalEquity: 1100, RealizedBalance: 1100},
	}
	if err := h.sync.persistHistoricalSnapshots(h.ctx, connMeta, rebuilt, false, sourceInEnclave, reconstructOpts{}); err != nil {
		t.Fatalf("persist: %v", err)
	}

	rows, err := h.snaps.GetByUserAndDateRange(h.ctx, dbUser, d(0), today)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	byDay := map[time.Time]*repository.Snapshot{}
	var deposits float64
	for _, r := range rows {
		byDay[r.Timestamp.UTC()] = r
		deposits += r.Deposits
	}

	if _, ok := byDay[d(0)]; ok {
		t.Fatal("the old inception row survived next to the new first row")
	}
	if deposits != 1000 {
		t.Fatalf("deposits across the history = %v, want the 1000 counted once", deposits)
	}
	if r := byDay[d(2)]; r == nil || r.TotalEquity != 1000 {
		t.Fatalf("d(2) must hold the balance at its opening midnight, 1000: %+v", r)
	}
	r := byDay[d(3)]
	if r == nil || r.TotalEquity != 1130 || r.UnrealizedPnL != 30 || r.Breakdown == nil || r.Breakdown.CFD == nil {
		t.Fatalf("the live measurement of d(3) was overwritten: %+v", r)
	}
}

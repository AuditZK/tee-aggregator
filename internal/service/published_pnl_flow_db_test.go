package service

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// pnlConnector reports no cashflows, only a cumulative PnL, the way predict.fun
// does.
type pnlConnector struct {
	scriptedConnector
	pnlAt  func(time.Time) (float64, error)
	asked  []time.Time
	pnlNow float64
}

func (c *pnlConnector) CumulativePnL(_ context.Context, at time.Time) (float64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if at.IsZero() {
		return c.pnlNow, nil
	}
	c.asked = append(c.asked, at)
	return c.pnlAt(at)
}

func TestDBFlowsFromPublishedPnL(t *testing.T) {
	today := startOfTodayUTC()
	yesterday := today.Add(-24 * time.Hour)
	cases := []struct {
		name        string
		prevEquity  float64
		equity      float64
		pnlBefore   float64
		pnlNow      float64
		pnlErr      error
		deposits    float64
		withdrawals float64
	}{
		{name: "equity moved with the pnl", prevEquity: 1000, equity: 1080, pnlBefore: 300, pnlNow: 380},
		{name: "drift between the hourly point and the read is noise", prevEquity: 1000, equity: 1082, pnlBefore: 300, pnlNow: 380},
		{name: "a withdrawal the venue never reported", prevEquity: 1000, equity: 600, pnlBefore: 300, pnlNow: 350, withdrawals: 450},
		{name: "a deposit on a losing day", prevEquity: 1000, equity: 1900, pnlBefore: 300, pnlNow: 280, deposits: 920},
		{name: "a pnl the venue cannot state books nothing", prevEquity: 1000, equity: 600, pnlErr: connector.ErrPnLNotCovered},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "predictfun", "main", dbKey, dbSecret)
			if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
				UserUID: dbUser, Exchange: "predictfun", Label: "main", Timestamp: yesterday,
				TotalEquity: tc.prevEquity, RealizedBalance: tc.prevEquity,
			}); err != nil {
				t.Fatalf("seed yesterday: %v", err)
			}
			c := &pnlConnector{
				scriptedConnector: scriptedConnector{exchange: "predictfun", balances: []connector.Balance{{Equity: tc.equity, Available: tc.equity}}},
				pnlNow:            tc.pnlNow,
				pnlAt: func(time.Time) (float64, error) {
					return tc.pnlBefore, tc.pnlErr
				},
			}
			h.plant(dbKey, dbSecret, c)

			if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "predictfun", "main"); !res.Success {
				t.Fatalf("sync failed: %+v", res)
			}
			snap := h.snapshotToday(t, "predictfun", "main")
			if math.Abs(snap.Deposits-tc.deposits) > 1e-6 || math.Abs(snap.Withdrawals-tc.withdrawals) > 1e-6 {
				t.Errorf("deposits, withdrawals = %v, %v, want %v, %v", snap.Deposits, snap.Withdrawals, tc.deposits, tc.withdrawals)
			}
			if len(c.asked) != 1 || !c.asked[0].Equal(yesterday) {
				t.Errorf("pnl asked at %v, want the previous row's read at %v", c.asked, yesterday)
			}
		})
	}
}

func TestDBFlowsFromPublishedPnL_ConnectRowIsReadAtItsSyncInstant(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "predictfun", "main", dbKey, dbSecret)
	yesterday := startOfTodayUTC().Add(-24 * time.Hour)
	connected := yesterday.Add(14*time.Hour + 5*time.Minute)
	if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
		UserUID: dbUser, Exchange: "predictfun", Label: "main", Timestamp: yesterday,
		TotalEquity: 1000, RealizedBalance: 1000, Deposits: 1000,
	}); err != nil {
		t.Fatalf("seed connect-day row: %v", err)
	}
	if err := h.statuses.Upsert(h.ctx, &repository.SyncStatus{
		UserUID: dbUser, Exchange: "predictfun", Label: "main", LastSyncTime: &connected, Status: "completed",
	}); err != nil {
		t.Fatalf("seed connect-day status: %v", err)
	}
	c := &pnlConnector{
		scriptedConnector: scriptedConnector{exchange: "predictfun", balances: []connector.Balance{{Equity: 1100, Available: 1100}}},
		pnlNow:            500,
		pnlAt: func(at time.Time) (float64, error) {
			if at.Before(connected) {
				return 0, errors.New("asked before the connect read")
			}
			return 400, nil
		},
	}
	h.plant(dbKey, dbSecret, c)

	if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "predictfun", "main"); !res.Success {
		t.Fatalf("sync failed: %+v", res)
	}
	if snap := h.snapshotToday(t, "predictfun", "main"); snap.Deposits != 0 || snap.Withdrawals != 0 {
		t.Errorf("deposits, withdrawals = %v, %v, want none: the day's PnL explains the move", snap.Deposits, snap.Withdrawals)
	}
	if len(c.asked) != 1 || !c.asked[0].Equal(connected) {
		t.Errorf("pnl asked at %v, want the connect read at %v", c.asked, connected)
	}
}

// Equity 1000 at X 00:00, 1100 when a manual sync reads it at X 14:00, the
// midnight pass writes row D at 1100, a manual sync rewrites row D at 1100.
// The rewrite used to measure the PnL from X 00:00 against X's 14:00 equity,
// and the morning's +100 came out as a withdrawal on row D.
func TestDBFlowsFromPublishedPnL_SameDayRewriteMeasuresFromThePreviousRead(t *testing.T) {
	for _, exchange := range []string{"hyperliquid", "predictfun"} {
		t.Run(exchange, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, exchange, "main", dbKey, dbSecret)
			x := startOfTodayUTC().Add(-24 * time.Hour)
			read := x.Add(14 * time.Hour)
			if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
				UserUID: dbUser, Exchange: exchange, Label: "main", Timestamp: x,
				TotalEquity: 1100, RealizedBalance: 1100,
			}); err != nil {
				t.Fatalf("seed row X: %v", err)
			}
			if err := h.statuses.Upsert(h.ctx, &repository.SyncStatus{
				UserUID: dbUser, Exchange: exchange, Label: "main", LastSyncTime: &read, Status: "completed",
			}); err != nil {
				t.Fatalf("seed status: %v", err)
			}
			c := &pnlConnector{
				scriptedConnector: scriptedConnector{exchange: exchange, balances: []connector.Balance{{Equity: 1100, Available: 1100}}},
				pnlNow:            100,
				pnlAt: func(at time.Time) (float64, error) {
					if at.Before(read) {
						return 0, nil
					}
					return 100, nil
				},
			}
			h.plant(dbKey, dbSecret, c)

			for _, pass := range []string{"midnight pass", "rewrite 1", "rewrite 2"} {
				if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, exchange, "main"); !res.Success {
					t.Fatalf("%s failed: %+v", pass, res)
				}
				if snap := h.snapshotToday(t, exchange, "main"); snap.Deposits != 0 || snap.Withdrawals != 0 {
					t.Errorf("%s: row D = +%v/-%v, want none: the PnL since X's read explains the move", pass, snap.Deposits, snap.Withdrawals)
				}
			}
			for i, at := range c.asked {
				if !at.Equal(read) {
					t.Errorf("pnl asked at %s on pass %d, want X's read %s", at.Format(time.RFC3339), i+1, read.Format(time.RFC3339))
				}
			}
		})
	}
}

// Connected at X 14:00 (row X live at 1100), midnight pass D stores the
// X 14:00 start, then the recalibration rewrites row X from the rebuilder at
// its X 00:00 equity of 1000. A rewrite of D must not measure the PnL from
// 14:00 against that midnight equity: the morning's +100 would read as a
// deposit.
func TestDBFlowsFromPublishedPnL_SameDayRewriteAfterRecalibrationKeepsTheMidnight(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "hyperliquid", "main", dbKey, dbSecret)
	x := startOfTodayUTC().Add(-24 * time.Hour)
	connected := x.Add(14 * time.Hour)
	if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
		UserUID: dbUser, Exchange: "hyperliquid", Label: "main", Timestamp: x,
		TotalEquity: 1100, RealizedBalance: 1100, Deposits: 1100,
	}); err != nil {
		t.Fatalf("seed connect row: %v", err)
	}
	if err := h.statuses.Upsert(h.ctx, &repository.SyncStatus{
		UserUID: dbUser, Exchange: "hyperliquid", Label: "main", LastSyncTime: &connected, Status: "completed",
	}); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	c := &pnlConnector{
		scriptedConnector: scriptedConnector{exchange: "hyperliquid", balances: []connector.Balance{{Equity: 1100, Available: 1100}}},
		pnlNow:            100,
		pnlAt: func(at time.Time) (float64, error) {
			if at.Before(connected) {
				return 0, nil
			}
			return 100, nil
		},
	}
	h.plant(dbKey, dbSecret, c)
	if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "hyperliquid", "main"); !res.Success {
		t.Fatalf("midnight pass failed: %+v", res)
	}
	if st := h.status(t, "hyperliquid", "main"); st.LastWindowStart == nil || !st.LastWindowStart.Equal(connected) {
		t.Fatalf("midnight pass stored start %v, want %s", st.LastWindowStart, connected.Format(time.RFC3339))
	}

	if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
		UserUID: dbUser, Exchange: "hyperliquid", Label: "main", Timestamp: x,
		TotalEquity: 1000, RealizedBalance: 1000, Deposits: 1000, FromExternalRebuilder: true,
	}); err != nil {
		t.Fatalf("recalibrate row X: %v", err)
	}
	if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "hyperliquid", "main"); !res.Success {
		t.Fatalf("rewrite failed: %+v", res)
	}
	if snap := h.snapshotToday(t, "hyperliquid", "main"); snap.Deposits != 0 || snap.Withdrawals != 0 {
		t.Errorf("row D = +%v/-%v, want none: the PnL since X 00:00 explains the move from the rebuilt row", snap.Deposits, snap.Withdrawals)
	}
}

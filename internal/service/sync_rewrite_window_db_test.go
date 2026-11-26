package service

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// seedMidDayRow writes row X the way a manual sync at X 14:00 left it, having
// booked a deposit made at X 09:00, with the status that sync leaves.
func seedMidDayRow(t *testing.T, h *dbHarness, exchange string, x time.Time) (deposit, read time.Time) {
	t.Helper()
	deposit = x.Add(9 * time.Hour)
	read = x.Add(14 * time.Hour)
	if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
		UserUID: dbUser, Exchange: exchange, Label: "main", Timestamp: x,
		TotalEquity: 1300, RealizedBalance: 1300, Deposits: 300,
	}); err != nil {
		t.Fatalf("seed row X: %v", err)
	}
	ws := x.Add(-24 * time.Hour)
	if err := h.statuses.Upsert(h.ctx, &repository.SyncStatus{
		UserUID: dbUser, Exchange: exchange, Label: "main",
		LastSyncTime: &read, LastWindowStart: &ws, Status: "completed",
	}); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	return deposit, read
}

// Deposit at X 09:00, manual sync at X 14:00 books it on row X, the midnight
// pass writes row D, a manual sync at D 14:00 rewrites row D. The rewrite used
// to start at row X's midnight, because the midnight pass had overwritten the
// X 14:00 stamp, and booked the deposit on row D a second time.
func TestDBSameDayRewriteDoesNotRebookTheMidDayRowsFlows(t *testing.T) {
	today := startOfTodayUTC()
	for _, gap := range []int{1, 3} {
		for _, exchange := range []string{"binance", "bybit", "okx"} {
			t.Run(fmt.Sprintf("%s/X=D-%d", exchange, gap), func(t *testing.T) {
				h := newDBHarness(t)
				h.seedConnection(t, exchange, "main", dbKey, dbSecret)
				x := today.Add(-time.Duration(gap) * 24 * time.Hour)
				deposit, read := seedMidDayRow(t, h, exchange, x)
				h.plant(dbKey, dbSecret, &scriptedConnector{
					exchange:  exchange,
					balances:  []connector.Balance{{Equity: 1300, Available: 1300}},
					cashflows: []*connector.Cashflow{{Amount: 300, Currency: "USDT", Timestamp: deposit}},
				})

				// The midnight pass resumes where X's read stopped.
				if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, exchange, "main"); !res.Success {
					t.Fatalf("midnight pass failed: %+v", res)
				}
				if snap := h.snapshotToday(t, exchange, "main"); snap.Deposits != 0 {
					t.Fatalf("midnight pass booked +%v on row D, want 0", snap.Deposits)
				}
				if st := h.status(t, exchange, "main"); st.LastWindowStart == nil || !st.LastWindowStart.Equal(read) {
					t.Errorf("midnight pass recorded window start %v, want %s", st.LastWindowStart, read.Format(time.RFC3339))
				}

				// Two rewrites: both keep the deposit on row X only.
				for i := range 2 {
					if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, exchange, "main"); !res.Success {
						t.Fatalf("rewrite %d failed: %+v", i+1, res)
					}
					snap := h.snapshotToday(t, exchange, "main")
					if snap.Deposits != 0 || snap.Withdrawals != 0 {
						t.Errorf("rewrite %d: row D = +%v/-%v, want +0/-0: the X 09:00 deposit is already on row X",
							i+1, snap.Deposits, snap.Withdrawals)
					}
					if st := h.status(t, exchange, "main"); st.LastWindowStart == nil || !st.LastWindowStart.Equal(read) {
						t.Errorf("rewrite %d recorded window start %v, want %s", i+1, st.LastWindowStart, read.Format(time.RFC3339))
					}
				}
				prev, err := h.snaps.GetLatestByUserExchangeLabelBefore(h.ctx, dbUser, exchange, "main", today)
				if err != nil || prev == nil || !prev.Timestamp.Equal(x) || prev.Deposits != 300 {
					t.Errorf("row X = %+v (%v), want its +300 untouched", prev, err)
				}
			})
		}
	}
}

// The first pass after deploy finds a NULL start, and a production database
// the column was not yet added to has none at all: both keep the previous
// row's midnight.
func TestDBSameDayRewriteWithoutAStoredStartUsesThePreviousRow(t *testing.T) {
	for _, dropColumn := range []bool{false, true} {
		name := "NULL column"
		if dropColumn {
			name = "column absent"
		}
		t.Run(name, func(t *testing.T) {
			h := newDBHarness(t)
			if dropColumn {
				if _, err := h.pool.Exec(h.ctx, `ALTER TABLE sync_statuses DROP COLUMN last_window_start`); err != nil {
					t.Fatalf("drop column: %v", err)
				}
			}
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)
			today := startOfTodayUTC()
			dMinus3 := today.Add(-72 * time.Hour)
			seedSameDayRows(t, h, "binance",
				&repository.Snapshot{Timestamp: dMinus3, TotalEquity: 1000, RealizedBalance: 1000},
				&repository.Snapshot{Timestamp: today, TotalEquity: 1000, RealizedBalance: 1000},
			)
			connMeta, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "binance", "main")
			if err != nil {
				t.Fatalf("read connection: %v", err)
			}
			if got := h.sync.activityWindowStart(h.ctx, connMeta, today); !got.Equal(dMinus3) {
				t.Errorf("window starts %s, want row D-3's midnight %s", got.Format(time.RFC3339), dMinus3.Format(time.RFC3339))
			}

			h.plant(dbKey, dbSecret, &scriptedConnector{exchange: "binance", balances: []connector.Balance{{Equity: 1000, Available: 1000}}})
			if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "binance", "main"); !res.Success {
				t.Fatalf("sync failed: %+v", res)
			}
			st := h.status(t, "binance", "main")
			if dropColumn {
				if st.LastWindowStart != nil {
					t.Errorf("window start %v read from an absent column", st.LastWindowStart)
				}
			} else if st.LastWindowStart == nil || !st.LastWindowStart.Equal(dMinus3) {
				t.Errorf("recorded window start %v, want %s", st.LastWindowStart, dMinus3.Format(time.RFC3339))
			}
		})
	}
}

// A day-dated ledger (IBKR) keeps the previous row's midnight whatever start
// is stored: a mid-day start would drop a flow stamped at that midnight.
func TestDBSameDayRewriteOfADayLedgerKeepsTheMidnightStart(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	today := startOfTodayUTC()
	yesterday := today.Add(-24 * time.Hour)
	seedSameDayRows(t, h, "ibkr",
		&repository.Snapshot{Timestamp: yesterday, TotalEquity: 1000, RealizedBalance: 1000},
		&repository.Snapshot{Timestamp: today, TotalEquity: 1000, RealizedBalance: 1000},
	)
	read, ws := today.Add(time.Second), yesterday.Add(14*time.Hour)
	if err := h.statuses.Upsert(h.ctx, &repository.SyncStatus{
		UserUID: dbUser, Exchange: "ibkr", Label: "main",
		LastSyncTime: &read, LastWindowStart: &ws, Status: "completed",
	}); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	connMeta, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "ibkr", "main")
	if err != nil {
		t.Fatalf("read connection: %v", err)
	}
	if got := h.sync.activityWindowStart(h.ctx, connMeta, today); !got.Equal(yesterday) {
		t.Errorf("window starts %s, want yesterday's midnight %s", got.Format(time.RFC3339), yesterday.Format(time.RFC3339))
	}
}

// The window start travels with the stamp: a failed attempt keeps both, and a
// stamp written without a start clears the old one.
func TestDBSyncStatusWindowStartFollowsTheStamp(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	stamp := startOfTodayUTC().Add(time.Second)
	ws := stamp.Add(-36 * time.Hour)
	up := func(st *repository.SyncStatus) {
		t.Helper()
		st.UserUID, st.Exchange, st.Label = dbUser, "binance", "main"
		if err := h.statuses.Upsert(h.ctx, st); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	up(&repository.SyncStatus{LastSyncTime: &stamp, LastWindowStart: &ws, Status: "completed"})
	up(&repository.SyncStatus{Status: "error", ErrorMessage: "get balance: sync failed"})
	if st := h.status(t, "binance", "main"); st.LastWindowStart == nil || !st.LastWindowStart.Equal(ws) {
		t.Errorf("after a failure: window start %v, want %s kept", st.LastWindowStart, ws.Format(time.RFC3339))
	}
	later := stamp.Add(time.Hour)
	up(&repository.SyncStatus{LastSyncTime: &later, Status: "pending", ErrorMessage: "skipped_stale: x"})
	if st := h.status(t, "binance", "main"); st.LastWindowStart != nil {
		t.Errorf("after a stamp without a row: window start %v, want NULL", st.LastWindowStart)
	}
}

// A failed manual sync on D leaves status error with the stamp and start of
// the pass that wrote row D; the next successful rewrite still starts there.
func TestDBSameDayRewriteAfterAFailedAttemptKeepsTheStoredStart(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	today := startOfTodayUTC()
	x := today.Add(-24 * time.Hour)
	deposit, read := seedMidDayRow(t, h, "binance", x)
	c := &scriptedConnector{
		exchange:  "binance",
		balances:  []connector.Balance{{Equity: 1300, Available: 1300}},
		cashflows: []*connector.Cashflow{{Amount: 300, Currency: "USDT", Timestamp: deposit}},
	}
	h.plant(dbKey, dbSecret, c)
	if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "binance", "main"); !res.Success {
		t.Fatalf("midnight pass failed: %+v", res)
	}

	c.mu.Lock()
	c.balanceErr = errors.New("connection reset")
	c.mu.Unlock()
	if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "binance", "main"); res.Success {
		t.Fatalf("failed attempt reported success: %+v", res)
	}
	if st := h.status(t, "binance", "main"); st.Status != "error" || st.LastWindowStart == nil || !st.LastWindowStart.Equal(read) {
		t.Fatalf("after the failure: status %q, window start %v; want error, %s kept", st.Status, st.LastWindowStart, read.Format(time.RFC3339))
	}

	c.mu.Lock()
	c.balanceErr = nil
	c.mu.Unlock()
	if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "binance", "main"); !res.Success {
		t.Fatalf("rewrite failed: %+v", res)
	}
	if snap := h.snapshotToday(t, "binance", "main"); snap.Deposits != 0 {
		t.Errorf("row D = +%v after the rewrite, want +0: the X 09:00 deposit is already on row X", snap.Deposits)
	}
}

// A gap repair filling D-2 and D-1 after row D was first written leaves the
// stored start (X 14:00) older than the row before today. The rewrite starts
// at that row, or a flow the repaired rows carry lands on row D as well.
func TestDBSameDayRewriteAfterAGapRepairStartsAtTheRepairedRow(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	today := startOfTodayUTC()
	x := today.Add(-72 * time.Hour)
	deposit, _ := seedMidDayRow(t, h, "binance", x)
	gapDeposit := today.Add(-48*time.Hour + 12*time.Hour)
	c := &scriptedConnector{
		exchange: "binance",
		balances: []connector.Balance{{Equity: 1500, Available: 1500}},
		cashflows: []*connector.Cashflow{
			{Amount: 300, Currency: "USDT", Timestamp: deposit},
			{Amount: 200, Currency: "USDT", Timestamp: gapDeposit},
		},
	}
	h.plant(dbKey, dbSecret, c)
	if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "binance", "main"); !res.Success {
		t.Fatalf("midnight pass failed: %+v", res)
	}

	dMinus1 := today.Add(-24 * time.Hour)
	for _, r := range []*repository.Snapshot{
		{Timestamp: today.Add(-48 * time.Hour), TotalEquity: 1500, RealizedBalance: 1500, Deposits: 200},
		{Timestamp: dMinus1, TotalEquity: 1500, RealizedBalance: 1500},
	} {
		r.UserUID, r.Exchange, r.Label = dbUser, "binance", "main"
		if err := h.snaps.Upsert(h.ctx, r); err != nil {
			t.Fatalf("seed repaired row: %v", err)
		}
	}

	connMeta, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "binance", "main")
	if err != nil {
		t.Fatalf("read connection: %v", err)
	}
	if got := h.sync.activityWindowStart(h.ctx, connMeta, today); !got.Equal(dMinus1) {
		t.Errorf("window starts %s, want row D-1's midnight %s", got.Format(time.RFC3339), dMinus1.Format(time.RFC3339))
	}
	if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "binance", "main"); !res.Success {
		t.Fatalf("rewrite failed: %+v", res)
	}
	if snap := h.snapshotToday(t, "binance", "main"); snap.Deposits != 0 {
		t.Errorf("row D = +%v after the rewrite, want +0: the D-2 deposit is on the repaired row", snap.Deposits)
	}
}

package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// seedSameDayRows writes the connection's rows on the given days and the
// status the pass that wrote the last of them leaves: completed, stamped a
// second after today's midnight.
func seedSameDayRows(t *testing.T, h *dbHarness, exchange string, rows ...*repository.Snapshot) time.Time {
	t.Helper()
	for _, r := range rows {
		r.UserUID, r.Exchange, r.Label = dbUser, exchange, "main"
		if err := h.snaps.Upsert(h.ctx, r); err != nil {
			t.Fatalf("seed row %s: %v", r.Timestamp.Format(time.DateOnly), err)
		}
	}
	read := startOfTodayUTC().Add(time.Second)
	if err := h.statuses.Upsert(h.ctx, &repository.SyncStatus{
		UserUID: dbUser, Exchange: exchange, Label: "main",
		LastSyncTime: &read, Status: "completed",
	}); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	return read
}

// Rows on D-3 and D, the two days between missed: the midnight pass on D read
// back to D-3 and booked a deposit made on D-2 on row D. A manual sync later
// that day rewrites row D and must read the same span, or the deposit drops
// off the row and the capital-flow accounting books it as performance.
func TestDBSameDayRewriteAfterAGapKeepsTheGapFlows(t *testing.T) {
	for _, exchange := range []string{"binance", "bybit"} {
		t.Run(exchange, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, exchange, "main", dbKey, dbSecret)
			today := startOfTodayUTC()
			dMinus3 := today.Add(-72 * time.Hour)
			deposit := today.Add(-48*time.Hour + 12*time.Hour)
			seedSameDayRows(t, h, exchange,
				&repository.Snapshot{Timestamp: dMinus3, TotalEquity: 1000, RealizedBalance: 1000},
				&repository.Snapshot{Timestamp: today, TotalEquity: 1300, RealizedBalance: 1300, Deposits: 300},
			)
			h.plant(dbKey, dbSecret, &scriptedConnector{
				exchange:  exchange,
				balances:  []connector.Balance{{Equity: 1300, Available: 1300}},
				cashflows: []*connector.Cashflow{{Amount: 300, Currency: "USDT", Timestamp: deposit}},
			})

			connMeta, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, exchange, "main")
			if err != nil {
				t.Fatalf("read connection: %v", err)
			}
			if got := h.sync.activityWindowStart(h.ctx, connMeta, today); !got.Equal(dMinus3) {
				t.Errorf("window starts %s, want row D-3's midnight %s", got.Format(time.RFC3339), dMinus3.Format(time.RFC3339))
			}

			if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, exchange, "main"); !res.Success {
				t.Fatalf("sync failed: %+v", res)
			}
			snap := h.snapshotToday(t, exchange, "main")
			if snap.TotalEquity != 1300 || snap.Deposits != 300 || snap.Withdrawals != 0 {
				t.Errorf("row D = equity %v, +%v/-%v; want 1300, +300/-0 as the midnight pass wrote it",
					snap.TotalEquity, snap.Deposits, snap.Withdrawals)
			}
		})
	}
}

// Consecutive rows: the rewrite still starts at yesterday's midnight.
func TestDBSameDayRewriteOnConsecutiveDaysIsUnchanged(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	today := startOfTodayUTC()
	yesterday := today.Add(-24 * time.Hour)
	seedSameDayRows(t, h, "binance",
		&repository.Snapshot{Timestamp: yesterday, TotalEquity: 1000, RealizedBalance: 1000},
		&repository.Snapshot{Timestamp: today, TotalEquity: 1000, RealizedBalance: 1000},
	)
	connMeta, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "binance", "main")
	if err != nil {
		t.Fatalf("read connection: %v", err)
	}
	if got := h.sync.activityWindowStart(h.ctx, connMeta, today); !got.Equal(yesterday) {
		t.Errorf("window starts %s, want yesterday's midnight %s", got.Format(time.RFC3339), yesterday.Format(time.RFC3339))
	}
}

// A connection whose only row is today's (connected today), and one with no
// row at all, keep the classic one-day window.
func TestDBSameDayRewriteWithoutAnEarlierRowKeepsTheClassicWindow(t *testing.T) {
	for _, withRow := range []bool{false, true} {
		name := "no row"
		if withRow {
			name = "only today's row"
		}
		t.Run(name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)
			today := startOfTodayUTC()
			if withRow {
				seedSameDayRows(t, h, "binance", &repository.Snapshot{Timestamp: today, TotalEquity: 1000, RealizedBalance: 1000, Deposits: 1000})
			}
			connMeta, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "binance", "main")
			if err != nil {
				t.Fatalf("read connection: %v", err)
			}
			want := today.Add(-defaultActivityWindow)
			if got := h.sync.activityWindowStart(h.ctx, connMeta, today); !got.Equal(want) {
				t.Errorf("window starts %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
			}
		})
	}
}

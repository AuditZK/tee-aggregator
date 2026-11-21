package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// reflowFixture: day0 is the first snapshot (inception deposit), day1 still
// carries a withdrawal the old rule booked, day2 is missing, day3 closes the
// gap, day4 was reconstructed by the rebuilder.
func reflowFixture(t *testing.T) (*dbHarness, [5]time.Time) {
	t.Helper()
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	today := startOfTodayUTC()
	var days [5]time.Time
	for i := range days {
		days[i] = today.AddDate(0, 0, i-5)
	}
	rows := []*repository.Snapshot{
		{Timestamp: days[0], TotalEquity: 1000, RealizedBalance: 1000, Deposits: 1000},
		{Timestamp: days[1], TotalEquity: 950, RealizedBalance: 950, Withdrawals: 50},
		{Timestamp: days[3], TotalEquity: 1140, RealizedBalance: 1140},
		{Timestamp: days[4], TotalEquity: 1150, RealizedBalance: 1150, Deposits: 7, FromExternalRebuilder: true},
	}
	for _, r := range rows {
		r.UserUID, r.Exchange, r.Label = dbUser, "binance", "main"
		if err := h.snaps.Upsert(h.ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	h.plant(dbKey, dbSecret, &scriptedConnector{
		exchange: "binance",
		balances: []connector.Balance{{Equity: 1150, Available: 1150}},
		cashflows: []*connector.Cashflow{
			{Amount: -30, Currency: "USDT", Timestamp: days[1].Add(-2 * time.Hour)},
			{Amount: -99, Currency: "USDT", Timestamp: days[1].Add(-30 * time.Hour)},
			{Amount: 200, Currency: "USDT", Timestamp: days[3].Add(-36 * time.Hour)},
			{Amount: -10, Currency: "USDT", Timestamp: days[3].Add(-1 * time.Hour)},
		},
	})
	return h, days
}

func (h *dbHarness) flowsOn(t *testing.T, day time.Time) (equity, deposits, withdrawals float64) {
	t.Helper()
	if err := h.pool.QueryRow(h.ctx, `SELECT "totalEquity", deposits, withdrawals FROM snapshot_data WHERE timestamp = $1`, day).Scan(&equity, &deposits, &withdrawals); err != nil {
		t.Fatal(err)
	}
	return
}

func TestDBReflowCashflows(t *testing.T) {
	want := map[int][2]float64{0: {1000, 0}, 1: {0, 30}, 3: {200, 10}, 4: {7, 0}}
	kept := map[int]bool{0: true, 4: true}

	for _, apply := range []bool{false, true} {
		name := map[bool]string{false: "dry run", true: "apply"}[apply]
		t.Run(name, func(t *testing.T) {
			h, days := reflowFixture(t)
			out, err := h.sync.ReflowCashflows(h.ctx, dbUser, "binance", "main", days[0], days[4], apply)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) != 4 {
				t.Fatalf("days = %d, want the 4 stored rows", len(out))
			}
			for _, d := range out {
				i := int(d.Day.Sub(days[0]) / (24 * time.Hour))
				if got := [2]float64{d.NewDeposits, d.NewWithdrawals}; got != want[i] {
					t.Errorf("day%d reclassified to %v, want %v", i, got, want[i])
				}
				if (d.Kept != "") != kept[i] {
					t.Errorf("day%d kept = %q", i, d.Kept)
				}
			}

			stored := map[int][3]float64{0: {1000, 1000, 0}, 1: {950, 0, 50}, 3: {1140, 0, 0}, 4: {1150, 7, 0}}
			if apply {
				stored[1] = [3]float64{950, 0, 30}
				stored[3] = [3]float64{1140, 200, 10}
			}
			for i, s := range stored {
				e, dep, wd := h.flowsOn(t, days[i])
				if got := [3]float64{e, dep, wd}; got != s {
					t.Errorf("day%d stored %v, want %v", i, got, s)
				}
			}
		})
	}
}

func TestDBReflowRefusesAnOversizedWindow(t *testing.T) {
	h, days := reflowFixture(t)
	if _, err := h.sync.ReflowCashflows(h.ctx, dbUser, "binance", "main", days[4].AddDate(0, 0, -91), days[4], false); err == nil {
		t.Error("a 91-day window was accepted")
	}
	if _, err := h.sync.ReflowCashflows(h.ctx, dbUser, "binance", "main", days[4], days[0], false); err == nil {
		t.Error("a reversed window was accepted")
	}
}

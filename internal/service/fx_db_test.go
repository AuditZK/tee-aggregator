package service

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// stubFX serves fixed final rates, keyed currency -> YYYY-MM-DD.
type stubFX map[string]map[string]float64

func (f stubFX) FinalRatesToUSD(_ context.Context, currency string, from, to time.Time) (map[string]float64, error) {
	out := map[string]float64{}
	for d := truncDay(from); !d.After(truncDay(to)); d = d.Add(24 * time.Hour) {
		if r, ok := f[currency][dayKey(d)]; ok {
			out[dayKey(d)] = r
		}
	}
	return out, nil
}

// eurRates gives day today-i the rate 1.10 + i/100, for i in 1..6.
func eurRates(today time.Time) stubFX {
	rates := map[string]float64{}
	for i := 1; i <= 6; i++ {
		rates[dayKey(today.AddDate(0, 0, -i))] = 1.10 + float64(i)/100
	}
	return stubFX{"EUR": rates}
}

func (h *dbHarness) storeEURDay(t *testing.T, label string, day time.Time, equity, deposits float64) {
	t.Helper()
	if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
		UserUID: dbUser, Exchange: "ctrader", Label: label, Timestamp: day,
		TotalEquity: equity, RealizedBalance: equity, Deposits: deposits,
		Breakdown: &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: equity, AvailableMargin: equity}},
	}); err != nil {
		t.Fatalf("store day: %v", err)
	}
}

func (h *dbHarness) storedDays(t *testing.T, label string) map[string]*repository.Snapshot {
	t.Helper()
	rows, err := h.snaps.GetByUserAndDateRange(h.ctx, dbUser, time.Unix(0, 0).UTC(), time.Now().UTC().Add(24*time.Hour))
	if err != nil {
		t.Fatalf("read days: %v", err)
	}
	out := map[string]*repository.Snapshot{}
	for _, r := range rows {
		if r.Exchange == "ctrader" && r.Label == label {
			out[dayKey(r.Timestamp)] = r
		}
	}
	return out
}

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

// A EUR account, with days stored before the conversion existed, synced once
// the conversion is on: the stored days are converted before the new one is
// written, each at its own day's rate, and a second sync converts nothing
// twice.
func TestDBEURAccountIsStoredInUSDHistoryIncluded(t *testing.T) {
	for _, path := range syncPaths {
		t.Run(path.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "ctrader", "eur", dbKey, dbSecret)
			today := startOfTodayUTC()
			h.sync.SetFXSource(eurRates(today))
			h.storeEURDay(t, "eur", today.AddDate(0, 0, -2), 1000, 1000)
			h.storeEURDay(t, "eur", today.AddDate(0, 0, -1), 1000, 0)
			h.plant(dbKey, dbSecret, &scriptedConnector{
				exchange: "ctrader",
				balances: []connector.Balance{{Equity: 1000, Available: 1000, Currency: "EUR"}},
			})

			res := path.run(h, "ctrader", "eur")
			if res.Error != "" || res.Skipped {
				t.Fatalf("sync: error=%q skipped=%v (%s)", res.Error, res.Skipped, res.SkipReason)
			}

			days := h.storedDays(t, "eur")
			// Read at the midnight opening each row's date: measured the day before.
			for i, want := range map[int]float64{0: 1.11, 1: 1.12, 2: 1.13} {
				r := days[dayKey(today.AddDate(0, 0, -i))]
				if r == nil {
					t.Fatalf("day -%d missing", i)
				}
				if !approx(r.TotalEquity, 1000*want) {
					t.Errorf("day -%d equity = %v, want %v", i, r.TotalEquity, 1000*want)
				}
				g := r.Breakdown.Global
				if g == nil || g.NativeCurrency != "EUR" || !approx(g.FXRateToUSD, want) || !approx(g.Equity, 1000*want) {
					t.Errorf("day -%d global = %+v", i, g)
				}
			}
			if d := days[dayKey(today.AddDate(0, 0, -2))]; !approx(d.Deposits, 1130) {
				t.Errorf("stored deposit = %v, want 1130", d.Deposits)
			}

			// Again: the stamp keeps every day at one conversion. Through the
			// manual path, since the scheduled pass skips a connection synced
			// today.
			if res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "ctrader", "eur"); res.Error != "" {
				t.Fatalf("second sync: %s", res.Error)
			}
			for i, want := range map[int]float64{0: 1.11, 1: 1.12, 2: 1.13} {
				if r := h.storedDays(t, "eur")[dayKey(today.AddDate(0, 0, -i))]; !approx(r.TotalEquity, 1000*want) {
					t.Errorf("after a second sync, day -%d equity = %v", i, r.TotalEquity)
				}
			}
		})
	}
}

// No final rate for the day just closed: nothing is written for today rather
// than a EUR day between USD ones.
func TestDBEURAccountWithoutTodaysRateIsNotWritten(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ctrader", "eur", dbKey, dbSecret)
	today := startOfTodayUTC()
	rates := eurRates(today)
	delete(rates["EUR"], dayKey(today.AddDate(0, 0, -1)))
	h.sync.SetFXSource(rates)
	h.storeEURDay(t, "eur", today.AddDate(0, 0, -1), 1000, 1000)
	h.plant(dbKey, dbSecret, &scriptedConnector{
		exchange: "ctrader",
		balances: []connector.Balance{{Equity: 1000, Available: 1000, Currency: "EUR"}},
	})

	res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "ctrader", "eur")
	if !res.Skipped {
		t.Fatalf("not skipped: %+v", res)
	}
	if _, ok := h.storedDays(t, "eur")[dayKey(today)]; ok {
		t.Fatal("today was written without its rate")
	}
}

// A USD account never meets the conversion, and an FX source that knows no
// rate cannot block it.
func TestDBUSDAccountIsUntouched(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ctrader", "usd", dbKey, dbSecret)
	h.sync.SetFXSource(stubFX{})
	h.plant(dbKey, dbSecret, &scriptedConnector{
		exchange: "ctrader",
		balances: []connector.Balance{{Equity: 1000, Available: 1000, Currency: "USD"}},
	})
	res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "ctrader", "usd")
	if res.Error != "" || res.Skipped {
		t.Fatalf("sync: %+v", res)
	}
	snap := h.snapshotToday(t, "ctrader", "usd")
	if snap.TotalEquity != 1000 || snap.Breakdown.Global.NativeCurrency != "" {
		t.Fatalf("USD day changed: equity %v, global %+v", snap.TotalEquity, snap.Breakdown.Global)
	}
}

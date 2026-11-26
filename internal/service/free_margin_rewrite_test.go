package service

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

var fmDay = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// classicProRow is a rebuilt day of a Portfolio Margin Pro account as the
// classic path stored it: the cross margin pool in spot at available =
// equity, the USDⓈ-M wallet with its margin floored at 0.
func classicProRow(spotEq, swapEq, swapAvail float64, rebuilt bool) *repository.Snapshot {
	global := spotEq + swapEq
	return &repository.Snapshot{
		ID: "row-1", Timestamp: fmDay, TotalEquity: global, FromExternalRebuilder: rebuilt,
		Breakdown: &repository.MarketBreakdown{
			Spot:   &repository.MarketMetrics{Equity: spotEq, AvailableMargin: math.Max(spotEq, 0)},
			Swap:   &repository.MarketMetrics{Equity: swapEq, AvailableMargin: swapAvail},
			Global: &repository.MarketMetrics{Equity: global, AvailableMargin: clampAvailableMargin(math.Max(spotEq, 0)+swapAvail, global), Trades: 7, Volume: 1234.5},
		},
	}
}

// proRebuilt is the pool walk's row for the same day: swap is the pool, its
// free margin pool − held.
func proRebuilt(pool, held, outside float64) *connector.HistoricalSnapshot {
	return &connector.HistoricalSnapshot{
		Date: fmDay, TotalEquity: pool + outside, MarginHeld: held,
		Breakdown: map[string]*connector.MarketBalance{
			"swap": {MarketType: "swap", Equity: pool, AvailableMargin: math.Max(math.Min(pool-held, pool), 0)},
			"spot": {MarketType: "spot", Equity: outside, AvailableMargin: outside},
		},
	}
}

func fmNear(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

// A pool mostly in cross margin, a small USDⓈ-M wallet, more held by the
// USDⓈ-M positions than that wallet holds. The held margin
// comes off the USDⓈ-M wallet to zero, then off the cross margin pool in
// spot; equity is not part of what is written.
func TestFreeMarginDayDeductsHeldFromThePool(t *testing.T) {
	d := freeMarginDay(classicProRow(180000, 8000, 0, true), proRebuilt(186000, 20000, 2000), time.Time{})
	if d.Kept != "" {
		t.Fatalf("kept: %s", d.Kept)
	}
	if !fmNear(d.OldAvailable, 180000) || !fmNear(d.NewAvailable, 168000) {
		t.Fatalf("global free margin %.2f -> %.2f, want 180000 (the whole pool, the wallet at 0) -> 168000", d.OldAvailable, d.NewAvailable)
	}
	if m := d.Markets["swap"]; !fmNear(m.NewAvailable, 0) || !fmNear(m.Equity, 8000) {
		t.Fatalf("swap %+v, want free 0 on equity 8000", m)
	}
	if m := d.Markets["spot"]; !fmNear(m.NewAvailable, 180000-(20000-8000)) {
		t.Fatalf("spot free %.2f, want %.2f", m.NewAvailable, 180000.0-(20000-8000))
	}
	if !fmNear(d.Equity, 188000) || !fmNear(d.MarginHeld, 20000) {
		t.Fatalf("day equity/held %+v", d)
	}
	for key := range d.next {
		if _, ok := d.expect[key]; !ok {
			t.Fatalf("%s written without the value it must still hold", key)
		}
	}
	if _, ok := d.next["swap"]; ok {
		t.Fatal("swap was already 0: nothing to write there")
	}
}

// The classic path's own deduction on the USDⓈ-M wallet is replaced, not
// added to: a wallet larger than the margin held keeps the difference.
func TestFreeMarginDayReplacesTheClassicDeduction(t *testing.T) {
	d := freeMarginDay(classicProRow(100000, 30000, 25000, true), proRebuilt(130000, 12000, 0), time.Time{})
	if d.Kept != "" {
		t.Fatalf("kept: %s", d.Kept)
	}
	if !fmNear(d.Markets["swap"].NewAvailable, 18000) || !fmNear(d.Markets["spot"].NewAvailable, 100000) || !fmNear(d.NewAvailable, 118000) {
		t.Fatalf("got %+v global %.2f", d.Markets, d.NewAvailable)
	}
}

func TestFreeMarginDayKeepsWhatItCannotFix(t *testing.T) {
	cover := fmDay.AddDate(0, 0, 3)
	bad := proRebuilt(186000, 20000, 0)
	bad.Breakdown["swap"].AvailableMargin = 186000 // a classic row: held not deducted from the pool
	noPool := proRebuilt(186000, 20000, 0)
	delete(noPool.Breakdown, "swap")
	converted := classicProRow(180000, 8000, 0, true)
	converted.Breakdown.Global.NativeCurrency = "EUR"
	brokenSum := classicProRow(180000, 8000, 0, true)
	brokenSum.Breakdown.Global.Equity += 50

	for _, tc := range []struct {
		name    string
		stored  *repository.Snapshot
		rebuilt *connector.HistoricalSnapshot
		cover   time.Time
		kept    string
	}{
		{"live day", classicProRow(180000, 8000, 0, false), proRebuilt(186000, 20000, 0), time.Time{}, "live-measured"},
		{"before the reach", classicProRow(180000, 8000, 0, true), nil, cover, "before the rebuild's reach"},
		{"not rebuilt", classicProRow(180000, 8000, 0, true), nil, time.Time{}, "not in the rebuild"},
		{"not a pool row", classicProRow(180000, 8000, 0, true), bad, time.Time{}, "does not follow"},
		{"no pool entry", classicProRow(180000, 8000, 0, true), noPool, time.Time{}, "no pool"},
		{"converted row", converted, proRebuilt(186000, 20000, 0), time.Time{}, "converted"},
		{"global not the sum", brokenSum, proRebuilt(186000, 20000, 0), time.Time{}, "not the sum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := freeMarginDay(tc.stored, tc.rebuilt, tc.cover)
			if !strings.Contains(d.Kept, tc.kept) {
				t.Fatalf("kept = %q, want %q", d.Kept, tc.kept)
			}
			if d.changed() {
				t.Fatal("a kept day plans a write")
			}
		})
	}
}

// More margin held than the stored buckets hold: free margin ends at zero
// everywhere (the live floor), never below, and the day says so.
func TestFreeMarginDayFloorsAtZero(t *testing.T) {
	d := freeMarginDay(classicProRow(15000, 2000, 0, true), proRebuilt(40000, 30000, 0), time.Time{})
	if d.Kept != "" {
		t.Fatalf("kept: %s", d.Kept)
	}
	if !d.HeldExceedsStoredPool || !fmNear(d.NewAvailable, 0) || !fmNear(d.Markets["spot"].NewAvailable, 0) {
		t.Fatalf("got %+v", d)
	}
}

// A negative USDⓈ-M wallet has no free margin and absorbs none of the held
// margin: it all comes off the pool in spot. No bucket ends above its
// equity or below zero.
func TestFreeMarginDayNegativeWallet(t *testing.T) {
	d := freeMarginDay(classicProRow(190500, -500, 0, true), proRebuilt(189900, 20000, 100), time.Time{})
	if d.Kept != "" {
		t.Fatalf("kept: %s", d.Kept)
	}
	if !fmNear(d.Markets["swap"].NewAvailable, 0) || !fmNear(d.Markets["spot"].NewAvailable, 170500) {
		t.Fatalf("got %+v", d.Markets)
	}
	if !fmNear(d.NewAvailable, 170500) {
		t.Fatalf("global %.2f, want 170500 (under the global equity 190000, no clamp)", d.NewAvailable)
	}
	if err := checkFreeMarginBuckets(d.Markets); err != nil {
		t.Fatal(err)
	}
}

func TestCheckFreeMarginBuckets(t *testing.T) {
	if err := checkFreeMarginBuckets(map[string]FreeMarginMarket{"spot": {Equity: 10, NewAvailable: 11}}); err == nil {
		t.Fatal("free above equity passed")
	}
	if err := checkFreeMarginBuckets(map[string]FreeMarginMarket{"swap": {Equity: -5, NewAvailable: 1}}); err == nil {
		t.Fatal("free margin on a negative bucket passed")
	}
	if err := checkFreeMarginBuckets(map[string]FreeMarginMarket{"swap": {Equity: 5, NewAvailable: -1}}); err == nil {
		t.Fatal("negative free margin passed")
	}
}

func TestFreeMarginBlockingWarnings(t *testing.T) {
	got := freeMarginBlocking([]string{"binance_pm.leverage_assumed_constant", "binance_pm.leverage_unknown: ABCUSDT", "binance_pm.unpriced_contract: XUSDT"})
	if len(got) != 2 {
		t.Fatalf("blocking = %v", got)
	}
}

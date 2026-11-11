package service

import (
	"math"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func rebuiltDay(ts time.Time, equity, deposits, withdrawals float64) *repository.Snapshot {
	return &repository.Snapshot{
		UserUID: "u", Exchange: "bybit", Label: "acc", Timestamp: ts,
		TotalEquity: equity, Deposits: deposits, Withdrawals: withdrawals,
		FromExternalRebuilder: true,
	}
}

// An account funded and traded on its first day: the deposit moves to a
// baseline the day before, and the first day is scored against it.
func TestInceptionBaseline_ScoresTheFirstDay(t *testing.T) {
	first := rebuiltDay(day(2026, time.March, 10), 900, 2000, 1000)
	base := inceptionBaseline(first, nil, day(2025, time.December, 10))
	if base == nil {
		t.Fatal("no baseline: the first day's trading is still the base, its P&L is lost")
	}
	if !base.Timestamp.Equal(day(2026, time.March, 9)) {
		t.Fatalf("baseline on %s, want the day before the first funded day", base.Timestamp)
	}
	if base.TotalEquity != 2000 || base.Deposits != 2000 || !base.FromExternalRebuilder {
		t.Fatalf("baseline %+v, want equity = deposit = 2000 from the rebuilder", base)
	}
	if first.Deposits != 0 || first.Withdrawals != 1000 {
		t.Fatalf("first day deposits %v withdrawals %v: the deposit must move to the baseline, the withdrawal stay", first.Deposits, first.Withdrawals)
	}

	next := rebuiltDay(day(2026, time.March, 11), 950, 0, 0)
	returns := convertSnapshotsToDailyReturns([]*repository.Snapshot{base, first, next})
	if len(returns) != 2 {
		t.Fatalf("got %d returns, want the first day and the next", len(returns))
	}
	// (900 - 2000 + 1000) / 2000
	if got := returns[0].netReturn; math.Abs(got-(-0.05)) > 1e-9 {
		t.Fatalf("first day return %v, want -0.05", got)
	}
}

// A rebuild whose window starts on the funded day cannot tell an inception
// from a deposit into an older account: no baseline.
func TestInceptionBaseline_NotWhenTheWindowStartsThere(t *testing.T) {
	first := rebuiltDay(day(2026, time.March, 10), 900, 2000, 1000)
	if base := inceptionBaseline(first, nil, day(2026, time.March, 10)); base != nil {
		t.Fatalf("baseline %+v on a day the rebuild could not look before", base)
	}
	if first.Deposits != 2000 {
		t.Fatal("the first day's deposit was cleared without a baseline")
	}
}

func TestInceptionBaseline_NotForAStamp(t *testing.T) {
	first := rebuiltDay(day(2026, time.March, 10), 1500, 1500, 0)
	if base := inceptionBaseline(first, nil, day(2025, time.December, 10)); base != nil {
		t.Fatalf("baseline %+v for a deposit that only restates the balance", base)
	}
}

func TestInceptionBaseline_NotForLiveRows(t *testing.T) {
	first := rebuiltDay(day(2026, time.March, 10), 900, 2000, 1000)
	first.FromExternalRebuilder = false
	if base := inceptionBaseline(first, nil, day(2025, time.December, 10)); base != nil {
		t.Fatalf("baseline %+v under a live row", base)
	}
}

func TestInceptionBaseline_NotUnderOlderHistory(t *testing.T) {
	first := rebuiltDay(day(2026, time.March, 10), 900, 2000, 1000)
	older := rebuiltDay(day(2026, time.March, 1), 500, 500, 0)
	if base := inceptionBaseline(first, []*repository.Snapshot{older}, day(2025, time.December, 10)); base != nil {
		t.Fatalf("baseline %+v under history that reaches further back", base)
	}
}

// The midnight recalibration re-runs a rebuild: the baseline it wrote last
// time is the only older row, and the same rows come out again.
func TestInceptionBaseline_SameRowsOnARerun(t *testing.T) {
	stored := rebuiltDay(day(2026, time.March, 9), 2000, 2000, 0)
	first := rebuiltDay(day(2026, time.March, 10), 900, 2000, 1000)
	base := inceptionBaseline(first, []*repository.Snapshot{stored}, day(2025, time.December, 10))
	if base == nil || base.Deposits != 2000 || first.Deposits != 0 {
		t.Fatalf("re-run baseline %+v, first day deposits %v: the deposit would be counted twice", base, first.Deposits)
	}
}

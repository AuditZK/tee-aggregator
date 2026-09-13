package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func measured(ts time.Time, equity float64) *repository.Snapshot {
	return &repository.Snapshot{Timestamp: ts, TotalEquity: equity}
}

func reconstructed(ts time.Time, equity float64) *repository.Snapshot {
	return &repository.Snapshot{Timestamp: ts, TotalEquity: equity, IsHistorical: true}
}

// The defect, measured on 2026-08-31: a binance reconstruction wrote
// $116,040.69 for a day the live sync had already measured at $94,584.28, and
// it was published. The rebuilder's own witness gate let it through, tolerating
// a 50% divergence.
func TestContradictedDay_RejectsAReconstructionThatRewritesAMeasuredDay(t *testing.T) {
	existing := []*repository.Snapshot{
		measured(day(2026, time.August, 29), 94465.33),
		measured(day(2026, time.August, 30), 94584.28),
	}
	rebuilt := []*repository.Snapshot{
		reconstructed(day(2026, time.August, 29), 115917.33),
		reconstructed(day(2026, time.August, 30), 116040.69),
	}

	got, measuredEq, bad := contradictedDay(rebuilt, existing)

	if !bad {
		t.Fatal("a reconstruction contradicting a measured day was accepted")
	}
	if !got.Timestamp.Equal(day(2026, time.August, 29)) {
		t.Fatalf("reported %s, want the first contradicted day 2026-08-29", got.Timestamp.Format("2006-01-02"))
	}
	if measuredEq != 94465.33 {
		t.Fatalf("measured equity = %v, want 94465.33", measuredEq)
	}
}

// Agreement on every shared day is the whole gate: a reconstruction that
// reproduces what we measured is trusted for the days we did not.
func TestContradictedDay_AcceptsAReconstructionThatReproducesTheOverlap(t *testing.T) {
	existing := []*repository.Snapshot{measured(day(2026, time.August, 30), 94584.28)}
	rebuilt := []*repository.Snapshot{
		reconstructed(day(2026, time.August, 20), 91000),
		reconstructed(day(2026, time.August, 30), 94584.28),
	}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("a reconstruction reproducing the measured day was rejected")
	}
}

// No overlap, nothing to check. A first backfill on a fresh connection must not
// be blocked for lack of a day to compare against.
func TestContradictedDay_NoOverlapPasses(t *testing.T) {
	existing := []*repository.Snapshot{measured(day(2026, time.August, 30), 94584.28)}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.June, 3), 3310.97)}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("rejected a reconstruction that shares no day with the measured series")
	}
}

// Only independently measured days are witnesses. Comparing a reconstruction
// against an earlier reconstruction proves nothing — both come from the same
// instrument and would agree on being wrong together.
func TestContradictedDay_IgnoresPreviouslyReconstructedDays(t *testing.T) {
	existing := []*repository.Snapshot{reconstructed(day(2026, time.August, 30), 81213.19)}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.August, 30), 116040.69)}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("treated an earlier reconstruction as an independent witness")
	}
}

// The tolerance separates instruments disagreeing about a price from
// instruments disagreeing about the account. Both bounds come from the gate's
// first live run.
func TestContradictedDay_ToleranceSeparatesNoiseFromDivergence(t *testing.T) {
	// 20 cents on $5,301 — hyperliquid, the live path and the rebuilder pricing
	// the same holdings from different sources. Same account.
	if _, _, bad := contradictedDay(
		[]*repository.Snapshot{reconstructed(day(2026, time.June, 1), 5300.971997)},
		[]*repository.Snapshot{measured(day(2026, time.June, 1), 5301.174097)},
	); bad {
		t.Fatal("valuation noise between two price sources rejected a faithful reconstruction")
	}

	// $1,818 on $125,414 — 1.4%, published under the 50% tolerance the
	// rebuilder's own gate carried.
	if _, _, bad := contradictedDay(
		[]*repository.Snapshot{reconstructed(day(2026, time.June, 2), 123595.21)},
		[]*repository.Snapshot{measured(day(2026, time.June, 2), 125413.64)},
	); !bad {
		t.Fatal("a 1.4% divergence passed — the gate is a business tolerance again")
	}
}

// Days are compared by date, whatever time of day either row carries.
func TestContradictedDay_ComparesByDayNotInstant(t *testing.T) {
	existing := []*repository.Snapshot{
		{Timestamp: time.Date(2026, time.August, 30, 13, 45, 0, 0, time.UTC), TotalEquity: 94584.28},
	}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.August, 30), 116040.69)}

	if _, _, bad := contradictedDay(rebuilt, existing); !bad {
		t.Fatal("a mid-day measured row escaped the comparison")
	}
}

// A live row at zero is far more often a degenerate sync than a funded account
// measured empty — the rest of the service already refuses to anchor on one.
// Witnessing against it discarded 89 days of a sound reconstruction on this
// gate's first live run.
func TestContradictedDay_ZeroEquityDayIsNotAWitness(t *testing.T) {
	existing := []*repository.Snapshot{measured(day(2026, time.June, 3), 0)}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.June, 3), 896.57)}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("a live row at zero was treated as a measurement worth trusting")
	}
}

// But a funded day still witnesses, even when the reconstruction claims zero.
func TestContradictedDay_FundedDayWitnessesAgainstAZeroRebuild(t *testing.T) {
	existing := []*repository.Snapshot{measured(day(2026, time.June, 3), 3310.97)}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.June, 3), 0)}

	if _, _, bad := contradictedDay(rebuilt, existing); !bad {
		t.Fatal("a reconstruction wiping a funded day to zero was accepted")
	}
}

// The external rebuilder's rows carry from_external_rebuilder, not
// is_historical. They are reconstructions too: a fresh pass that changes its
// own method (mark-to-market where the previous pass published realized only)
// must not be held to them. Measured 2026-09-13 on a bybit history: the first
// per-leg pass was rejected against the realized-only pass of the same morning.
func TestContradictedDay_IgnoresRowsTheExternalRebuilderWrote(t *testing.T) {
	previous := &repository.Snapshot{Timestamp: day(2026, time.June, 20), TotalEquity: 11115.84, FromExternalRebuilder: true}
	existing := []*repository.Snapshot{previous}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.June, 20), 10862.86)}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("a reconstruction was held to a day the previous reconstruction wrote")
	}
}

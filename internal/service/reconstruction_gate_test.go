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

// The defect: a reconstruction wrote a day about a fifth above what the live
// sync had already measured, and it was published. The rebuilder's own witness
// gate let it through, tolerating a 50% divergence.
func TestContradictedDay_RejectsAReconstructionThatRewritesAMeasuredDay(t *testing.T) {
	existing := []*repository.Snapshot{
		measured(day(2026, time.March, 9), 100000.00),
		measured(day(2026, time.March, 10), 100100.00),
	}
	rebuilt := []*repository.Snapshot{
		reconstructed(day(2026, time.March, 9), 122700.00),
		reconstructed(day(2026, time.March, 10), 122800.00),
	}

	got, measuredEq, bad := contradictedDay(rebuilt, existing)

	if !bad {
		t.Fatal("a reconstruction contradicting a measured day was accepted")
	}
	if !got.Timestamp.Equal(day(2026, time.March, 9)) {
		t.Fatalf("reported %s, want the first contradicted day 2026-03-09", got.Timestamp.Format("2006-01-02"))
	}
	if measuredEq != 100000.00 {
		t.Fatalf("measured equity = %v, want 100000", measuredEq)
	}
}

// Agreement on every shared day is the whole gate: a reconstruction that
// reproduces what we measured is trusted for the days we did not.
func TestContradictedDay_AcceptsAReconstructionThatReproducesTheOverlap(t *testing.T) {
	existing := []*repository.Snapshot{measured(day(2026, time.March, 10), 100100.00)}
	rebuilt := []*repository.Snapshot{
		reconstructed(day(2026, time.March, 1), 96000),
		reconstructed(day(2026, time.March, 10), 100100.00),
	}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("a reconstruction reproducing the measured day was rejected")
	}
}

// No overlap, nothing to check. A first backfill on a fresh connection must not
// be blocked for lack of a day to compare against.
func TestContradictedDay_NoOverlapPasses(t *testing.T) {
	existing := []*repository.Snapshot{measured(day(2026, time.March, 10), 100100.00)}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.February, 3), 3500.00)}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("rejected a reconstruction that shares no day with the measured series")
	}
}

// Only independently measured days are witnesses. Comparing a reconstruction
// against an earlier reconstruction proves nothing — both come from the same
// instrument and would agree on being wrong together.
func TestContradictedDay_IgnoresPreviouslyReconstructedDays(t *testing.T) {
	existing := []*repository.Snapshot{reconstructed(day(2026, time.March, 10), 86000.00)}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.March, 10), 122800.00)}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("treated an earlier reconstruction as an independent witness")
	}
}

// The tolerance separates instruments disagreeing about a price from
// instruments disagreeing about the account. Both bounds come from the gate's
// first live run.
func TestContradictedDay_ToleranceSeparatesNoiseFromDivergence(t *testing.T) {
	// 0.004%: the live path and the rebuilder pricing the same holdings from
	// different sources. Same account.
	if _, _, bad := contradictedDay(
		[]*repository.Snapshot{reconstructed(day(2026, time.February, 1), 5000.00)},
		[]*repository.Snapshot{measured(day(2026, time.February, 1), 5000.20)},
	); bad {
		t.Fatal("valuation noise between two price sources rejected a faithful reconstruction")
	}

	// 1.4%, published under the 50% tolerance the rebuilder's own gate carried.
	if _, _, bad := contradictedDay(
		[]*repository.Snapshot{reconstructed(day(2026, time.February, 2), 98600.00)},
		[]*repository.Snapshot{measured(day(2026, time.February, 2), 100000.00)},
	); !bad {
		t.Fatal("a 1.4% divergence passed — the gate is a business tolerance again")
	}
}

// Days are compared by date, whatever time of day either row carries.
func TestContradictedDay_ComparesByDayNotInstant(t *testing.T) {
	existing := []*repository.Snapshot{
		{Timestamp: time.Date(2026, time.March, 10, 13, 45, 0, 0, time.UTC), TotalEquity: 100100.00},
	}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.March, 10), 122800.00)}

	if _, _, bad := contradictedDay(rebuilt, existing); !bad {
		t.Fatal("a mid-day measured row escaped the comparison")
	}
}

// A live row at zero is far more often a degenerate sync than a funded account
// measured empty — the rest of the service already refuses to anchor on one.
// Witnessing against it discarded a sound reconstruction whole on this gate's
// first live run.
func TestContradictedDay_ZeroEquityDayIsNotAWitness(t *testing.T) {
	existing := []*repository.Snapshot{measured(day(2026, time.February, 3), 0)}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.February, 3), 900.00)}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("a live row at zero was treated as a measurement worth trusting")
	}
}

// But a funded day still witnesses, even when the reconstruction claims zero.
func TestContradictedDay_FundedDayWitnessesAgainstAZeroRebuild(t *testing.T) {
	existing := []*repository.Snapshot{measured(day(2026, time.February, 3), 3500.00)}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.February, 3), 0)}

	if _, _, bad := contradictedDay(rebuilt, existing); !bad {
		t.Fatal("a reconstruction wiping a funded day to zero was accepted")
	}
}

// The external rebuilder's rows carry from_external_rebuilder, not
// is_historical. They are reconstructions too: a fresh pass that changes its
// own method (mark-to-market where the previous pass published realized only)
// must not be held to them, or the first mark-to-market pass is rejected
// against the realized-only pass published hours earlier.
func TestContradictedDay_IgnoresRowsTheExternalRebuilderWrote(t *testing.T) {
	previous := &repository.Snapshot{Timestamp: day(2026, time.February, 20), TotalEquity: 11100.00, FromExternalRebuilder: true}
	existing := []*repository.Snapshot{previous}
	rebuilt := []*repository.Snapshot{reconstructed(day(2026, time.February, 20), 10850.00)}

	if _, _, bad := contradictedDay(rebuilt, existing); bad {
		t.Fatal("a reconstruction was held to a day the previous reconstruction wrote")
	}
}

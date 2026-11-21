package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func connDay(day int, label string, equity, deposits float64) *repository.Snapshot {
	t := time.Date(2026, 9, 1+day, 0, 0, 0, 0, time.UTC)
	return &repository.Snapshot{Timestamp: t, Exchange: "bybit", Label: label, TotalEquity: equity, Deposits: deposits}
}

// mainGains is a synthetic account gaining 50 a day from 10000 over 30 days.
func mainGains(days int) []*repository.Snapshot {
	var out []*repository.Snapshot
	for n := 0; n < days; n++ {
		out = append(out, connDay(n, "main", 10000+50*float64(n), 0))
	}
	return out
}

func cumulative(t *testing.T, snaps []*repository.Snapshot) float64 {
	t.Helper()
	r := convertSnapshotsToDailyReturns(snaps)
	return r[len(r)-1].cumulativeReturn
}

// A second account reports 5000 for three days and never again. Held at 5000
// it dilutes every later day of the first account; it leaves on its first
// silent day instead, and the exit itself scores nothing.
func TestDailyReturns_DeadConnectionLeavesAsAWithdrawal(t *testing.T) {
	snaps := mainGains(30)
	for n := 0; n < 3; n++ {
		snaps = append(snaps, connDay(n, "alt", 5000, 0))
	}
	sortSnapshots(snaps)

	want := 1.0
	for n := 1; n < 30; n++ {
		base := 10000 + 50*float64(n-1)
		if n <= 3 {
			base += 5000
		}
		want *= 1 + 50/base
	}
	if got := cumulative(t, snaps); !near(got, want-1, 1e-12) {
		t.Fatalf("cumulative return %v, want %v", got, want-1)
	}
}

// A gap within the threshold is a calendar gap: the account is held at its
// last equity, as before.
func TestDailyReturns_ShortGapIsForwardFilled(t *testing.T) {
	snaps := mainGains(10)
	for n := 0; n < 10; n++ {
		if n < 3 || n >= 7 {
			snaps = append(snaps, connDay(n, "alt", 5000, 0))
		}
	}
	sortSnapshots(snaps)

	want := 1.0
	for n := 1; n < 10; n++ {
		want *= 1 + 50/(10000+50*float64(n-1)+5000)
	}
	if got := cumulative(t, snaps); !near(got, want-1, 1e-12) {
		t.Fatalf("cumulative return %v, want %v", got, want-1)
	}
}

// A dead account that reports again rejoins as a new connection: what it
// holds is a deposit, in the day's base like any deposit, and the unmeasured
// stretch scores nothing.
func TestDailyReturns_ComebackRejoinsAsADeposit(t *testing.T) {
	snaps := mainGains(21)
	for n := 0; n < 3; n++ {
		snaps = append(snaps, connDay(n, "alt", 5000, 0))
	}
	snaps = append(snaps, connDay(20, "alt", 5600, 0))
	sortSnapshots(snaps)

	r := convertSnapshotsToDailyReturns(snaps)
	last := r[len(r)-1]
	if want := 50 / (10000 + 50*19.0 + 5600); !near(last.netReturn, want, 1e-12) {
		t.Fatalf("comeback day return %v, want the first account's gain alone %v", last.netReturn, want)
	}
}

func sortSnapshots(snaps []*repository.Snapshot) {
	for i := 1; i < len(snaps); i++ {
		for j := i; j > 0 && snaps[j].Timestamp.Before(snaps[j-1].Timestamp); j-- {
			snaps[j], snaps[j-1] = snaps[j-1], snaps[j]
		}
	}
}

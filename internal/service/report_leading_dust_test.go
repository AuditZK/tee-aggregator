package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func dustDay(d, ex string, equity, deposits float64) *repository.Snapshot {
	t, _ := time.Parse("2006-01-02", d)
	return &repository.Snapshot{Timestamp: t, Exchange: ex, Label: "acct", TotalEquity: equity, Deposits: deposits}
}

// A rebuilt history that projects stablecoin dust back before the first
// funding: the series (and the period) start on the funding day.
func TestDropLeadingDustDays_StartsOnTheFundingDay(t *testing.T) {
	snaps := []*repository.Snapshot{
		dustDay("2026-06-17", "bybit", 0.0000969, 0),
		dustDay("2026-06-18", "bybit", 0.0000969, 0),
		dustDay("2026-06-19", "bybit", 0.0000969, 0),
		dustDay("2026-06-20", "bybit", 11115.84, 11000),
		dustDay("2026-06-21", "bybit", 11122.18, 0),
	}
	kept := dropLeadingDustDays(snaps)
	if len(kept) != 2 || kept[0].Timestamp.Format("2006-01-02") != "2026-06-20" {
		t.Fatalf("want the series to start on 2026-06-20 with 2 snapshots, got %d starting %v", len(kept), kept[0].Timestamp)
	}
	returns := convertSnapshotsToDailyReturns(kept)
	if len(returns) != 1 || returns[0].date != "2026-06-21" {
		t.Fatalf("want one return dated 2026-06-21, got %+v", returns)
	}
	want := (11122.18 - 11115.84) / 11115.84
	if !near(returns[0].netReturn, want, 1e-12) {
		t.Fatalf("first return: got %v want %v", returns[0].netReturn, want)
	}
}

// The trim looks at the whole book for the day: two connections that are
// each under the floor but together above it are material.
func TestDropLeadingDustDays_SumsConnectionsPerDay(t *testing.T) {
	snaps := []*repository.Snapshot{
		dustDay("2026-01-01", "a", 0.6, 0),
		dustDay("2026-01-01", "b", 0.6, 0),
		dustDay("2026-01-02", "a", 0.7, 0),
		dustDay("2026-01-02", "b", 0.7, 0),
	}
	if kept := dropLeadingDustDays(snaps); len(kept) != 4 {
		t.Fatalf("want all 4 snapshots kept, got %d", len(kept))
	}
}

// A cash flow marks the account's real inception even if the close is dust.
func TestDropLeadingDustDays_CashFlowIsInception(t *testing.T) {
	snaps := []*repository.Snapshot{
		dustDay("2026-01-01", "a", 0.0001, 0),
		dustDay("2026-01-02", "a", 0.5, 0.5),
		dustDay("2026-01-03", "a", 200, 199.5),
	}
	kept := dropLeadingDustDays(snaps)
	if len(kept) != 2 || kept[0].Timestamp.Format("2006-01-02") != "2026-01-02" {
		t.Fatalf("want the series to start on the deposit day, got %d from %v", len(kept), kept[0].Timestamp)
	}
}

// Interior dust is history: a blow-up then a refund stays in the series.
func TestDropLeadingDustDays_KeepsInteriorRuns(t *testing.T) {
	snaps := []*repository.Snapshot{
		dustDay("2026-01-01", "a", 1000, 0),
		dustDay("2026-01-02", "a", 0.2, 0),
		dustDay("2026-01-03", "a", 0.2, 0),
		dustDay("2026-01-04", "a", 500, 500),
	}
	if kept := dropLeadingDustDays(snaps); len(kept) != 4 {
		t.Fatalf("want all 4 snapshots kept, got %d", len(kept))
	}
	if kept := dropLeadingDustDays(nil); len(kept) != 0 {
		t.Fatalf("nil in, empty out")
	}
}

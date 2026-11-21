package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

func histDay(d time.Time, equity float64) *connector.HistoricalSnapshot {
	return &connector.HistoricalSnapshot{Date: d, TotalEquity: equity, RealizedBalance: equity}
}

// The external rebuilder pads the window it was asked for. An OKX account nine
// days old, rebuilt over ninety, came back with seventy-nine zero-equity days
// in front of its real history; persisted, they drew two and a half months of
// flat zero before the curve started.
func TestBuildHistoricalSnapshots_DropsThePaddingBeforeTheAccountExists(t *testing.T) {
	today := day(2026, time.August, 30)
	connMeta := &repository.ExchangeConnection{UserUID: "user_42", Exchange: "okx", Label: "OKX principal"}

	hs := []*connector.HistoricalSnapshot{}
	for i := 88; i >= 10; i-- {
		hs = append(hs, histDay(today.AddDate(0, 0, -i), 0))
	}
	hs = append(hs,
		histDay(today.AddDate(0, 0, -9), 3310.97),
		histDay(today.AddDate(0, 0, -8), 3551.59),
	)

	snapshots, _ := buildHistoricalSnapshots(connMeta, hs, today, true)

	if len(snapshots) != 2 {
		t.Fatalf("got %d snapshots, want 2 — the zero-equity padding was persisted", len(snapshots))
	}
	for _, s := range snapshots {
		if s.TotalEquity == 0 {
			t.Fatalf("a zero-equity padding day survived at %s", s.Timestamp.Format("2006-01-02"))
		}
	}
	if want := today.AddDate(0, 0, -9); !snapshots[0].Timestamp.Equal(want) {
		t.Fatalf("series starts %s, want %s", snapshots[0].Timestamp.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

// An account emptied mid-history is real data, not padding. Only the leading
// run goes.
func TestBuildHistoricalSnapshots_KeepsAnInteriorEmptyDay(t *testing.T) {
	today := day(2026, time.August, 30)
	connMeta := &repository.ExchangeConnection{UserUID: "user_42", Exchange: "okx", Label: ""}

	hs := []*connector.HistoricalSnapshot{
		histDay(today.AddDate(0, 0, -5), 0),
		histDay(today.AddDate(0, 0, -4), 1000),
		histDay(today.AddDate(0, 0, -3), 0),
		histDay(today.AddDate(0, 0, -2), 1200),
	}

	snapshots, _ := buildHistoricalSnapshots(connMeta, hs, today, true)

	if len(snapshots) != 3 {
		t.Fatalf("got %d snapshots, want 3 — the account's own flat day was dropped with the padding", len(snapshots))
	}
	if snapshots[1].TotalEquity != 0 {
		t.Fatalf("the interior empty day was rewritten to %v", snapshots[1].TotalEquity)
	}
}

// A day with no equity but a recorded withdrawal is the account being emptied,
// not the rebuilder padding. Dropping it would lose the cash flow.
func TestBuildHistoricalSnapshots_KeepsAnEmptyDayCarryingACashFlow(t *testing.T) {
	today := day(2026, time.August, 30)
	connMeta := &repository.ExchangeConnection{UserUID: "user_42", Exchange: "okx", Label: ""}

	hs := []*connector.HistoricalSnapshot{
		{Date: today.AddDate(0, 0, -3), Withdrawals: 5000},
		histDay(today.AddDate(0, 0, -2), 0),
	}

	snapshots, _ := buildHistoricalSnapshots(connMeta, hs, today, true)

	if len(snapshots) != 2 {
		t.Fatalf("got %d snapshots, want 2 — a withdrawal day was mistaken for padding", len(snapshots))
	}
	if snapshots[0].Withdrawals != 5000 {
		t.Fatalf("withdrawals = %v, want 5000", snapshots[0].Withdrawals)
	}
}

func TestBuildHistoricalSnapshots_AllEmptyProducesNothing(t *testing.T) {
	today := day(2026, time.August, 30)
	connMeta := &repository.ExchangeConnection{UserUID: "user_42", Exchange: "okx", Label: ""}

	hs := []*connector.HistoricalSnapshot{
		histDay(today.AddDate(0, 0, -3), 0),
		histDay(today.AddDate(0, 0, -2), 0),
	}

	snapshots, skipped := buildHistoricalSnapshots(connMeta, hs, today, true)

	if len(snapshots) != 0 {
		t.Fatalf("got %d snapshots, want 0", len(snapshots))
	}
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}
}

// The filter must not depend on the rebuilder handing days back in order.
func TestBuildHistoricalSnapshots_PaddingDroppedRegardlessOfInputOrder(t *testing.T) {
	today := day(2026, time.August, 30)
	connMeta := &repository.ExchangeConnection{UserUID: "user_42", Exchange: "okx", Label: ""}

	hs := []*connector.HistoricalSnapshot{
		histDay(today.AddDate(0, 0, -2), 3551.59),
		histDay(today.AddDate(0, 0, -5), 0),
		histDay(today.AddDate(0, 0, -3), 3310.97),
		histDay(today.AddDate(0, 0, -4), 0),
	}

	snapshots, _ := buildHistoricalSnapshots(connMeta, hs, today, true)

	if len(snapshots) != 2 {
		t.Fatalf("got %d snapshots, want 2", len(snapshots))
	}
	if !snapshots[0].Timestamp.Before(snapshots[1].Timestamp) {
		t.Fatal("output is not in chronological order")
	}
}

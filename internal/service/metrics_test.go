package service

import (
	"math"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func TestFilterSnapshots_LabelAwareAndExchangeFallback(t *testing.T) {
	snapshots := []*repository.Snapshot{
		{Exchange: "binance", Label: "main"},
		{Exchange: "binance", Label: "secondary"},
		{Exchange: "bybit", Label: ""},
	}

	excluded := map[string]struct{}{
		"binance/secondary": {},
		"bybit":             {},
	}

	filtered := filterSnapshots(snapshots, "", excluded)
	if len(filtered) != 1 {
		t.Fatalf("expected 1 snapshot after exclusion, got %d", len(filtered))
	}
	if filtered[0].Exchange != "binance" || filtered[0].Label != "main" {
		t.Fatalf("unexpected remaining snapshot: exchange=%s label=%s", filtered[0].Exchange, filtered[0].Label)
	}

	filteredByExchange := filterSnapshots(snapshots, "binance", excluded)
	if len(filteredByExchange) != 1 {
		t.Fatalf("expected 1 binance snapshot after exclusion, got %d", len(filteredByExchange))
	}
	if filteredByExchange[0].Label != "main" {
		t.Fatalf("expected main label to remain, got %s", filteredByExchange[0].Label)
	}
}

func TestConvertSnapshotsToDailyReturns_UsesExchangeAndLabelKey(t *testing.T) {
	day1 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)

	snapshots := []*repository.Snapshot{
		{Timestamp: day1, Exchange: "binance", Label: "main", TotalEquity: 100},
		{Timestamp: day1, Exchange: "binance", Label: "secondary", TotalEquity: 200},
		{Timestamp: day2, Exchange: "binance", Label: "main", TotalEquity: 130},
		{Timestamp: day2, Exchange: "binance", Label: "secondary", TotalEquity: 180},
	}

	daily := convertSnapshotsToDailyReturns(snapshots)
	if len(daily) != 1 {
		t.Fatalf("expected 1 daily return, got %d", len(daily))
	}

	want := (130.0 + 180.0 - (100.0 + 200.0)) / (100.0 + 200.0)
	if math.Abs(daily[0].netReturn-want) > 1e-9 {
		t.Fatalf("unexpected netReturn: got %.12f want %.12f", daily[0].netReturn, want)
	}
}

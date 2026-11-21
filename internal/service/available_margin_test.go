package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// Free margin cannot exceed the equity it is drawn from. Two venues report the
// pair on different bases — Binance derives availableBalance from the wallet
// while equity is the margin balance, MT5 reports free margin against the
// account balance — so an unrealized loss made the dashboard show more margin
// free than the account owned. Every offending row measured on 2026-08-31 was
// off by exactly the unrealized loss.
func TestClampAvailableMargin_NeverExceedsEquity(t *testing.T) {
	cases := []struct {
		name                      string
		available, equity, expect float64
	}{
		{"unrealized loss inflates the wallet-based figure", 19812.40, 18831.76, 18831.76},
		{"already below equity is untouched", 2529.16, 42327.43, 2529.16},
		{"equal stays equal", 3310.97, 3310.97, 3310.97},
		{"no equity leaves no margin free", 500, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampAvailableMargin(tc.available, tc.equity); got != tc.expect {
				t.Fatalf("clampAvailableMargin(%v, %v) = %v, want %v",
					tc.available, tc.equity, got, tc.expect)
			}
		})
	}
}

// The live path: the connector hands its own available figure straight to
// toRepo, which is what the dashboard's free-margin line reads.
func TestToRepo_GlobalAvailableMarginIsCappedAtEquity(t *testing.T) {
	agg := &aggregatedBreakdown{}
	agg.swap.equity = 18831.76

	breakdown := agg.toRepo(18831.76, 19812.40, 0)

	if breakdown.Global == nil {
		t.Fatal("no global aggregate written")
	}
	if breakdown.Global.AvailableMargin != 18831.76 {
		t.Fatalf("global available margin = %v, want 18831.76 — an account showed more free margin than equity",
			breakdown.Global.AvailableMargin)
	}
}

// The reconstruction path builds its global aggregate separately and must hold
// the same invariant, or the margin curve jumps at the seam between rebuilt
// and live days.
func TestBuildHistoricalSnapshots_GlobalAvailableMarginIsCappedAtEquity(t *testing.T) {
	today := day(2026, time.August, 31)
	connMeta := &repository.ExchangeConnection{UserUID: "user_42", Exchange: "binance", Label: ""}
	hs := []*connector.HistoricalSnapshot{
		{
			Date:        today.AddDate(0, 0, -2),
			TotalEquity: 89653.64,
			Breakdown: map[string]*connector.MarketBalance{
				connector.MarketSwap: {MarketType: connector.MarketSwap, Equity: 89653.64, AvailableMargin: 93286.65},
			},
		},
	}

	snapshots, _ := buildHistoricalSnapshots(connMeta, hs, today, true)

	if len(snapshots) != 1 {
		t.Fatalf("got %d snapshots, want 1", len(snapshots))
	}
	got := snapshots[0].Breakdown.Global
	if got == nil {
		t.Fatal("no global aggregate written")
	}
	if got.AvailableMargin != 89653.64 {
		t.Fatalf("global available margin = %v, want 89653.64", got.AvailableMargin)
	}
}

package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

func isoDay(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestStatementPeriodsCoverRangeWithinIBKRCeiling(t *testing.T) {
	from, to := isoDay("2022-10-11"), isoDay("2025-10-09")
	periods := statementPeriods(from, to)
	if len(periods) != 3 {
		t.Fatalf("got %d periods, want 3: %v", len(periods), periods)
	}
	if !periods[0].from.Equal(from) || !periods[len(periods)-1].to.Equal(to) {
		t.Fatalf("periods do not span [%s, %s]: %v", from, to, periods)
	}
	for n, p := range periods {
		if days := int(p.to.Sub(p.from).Hours()/24) + 1; days > statementPeriodDays {
			t.Fatalf("period %d covers %d days, over IBKR's %d", n, days, statementPeriodDays)
		}
		if n > 0 && !p.from.Equal(periods[n-1].to.AddDate(0, 0, 1)) {
			t.Fatalf("period %d does not start the day after period %d ends: %v", n, n-1, periods)
		}
	}
}

func TestStatementPeriodsSingleDay(t *testing.T) {
	periods := statementPeriods(isoDay("2024-02-29"), isoDay("2024-02-29"))
	if len(periods) != 1 || !periods[0].from.Equal(periods[0].to) {
		t.Fatalf("got %v", periods)
	}
}

func TestMergePeriodSnapshotsKeepsEarlierPeriodOnSharedDate(t *testing.T) {
	first := []*connector.HistoricalSnapshot{
		{Date: isoDay("2023-10-09"), TotalEquity: 100},
		{Date: isoDay("2023-10-10"), TotalEquity: 110, Withdrawals: 5},
	}
	second := []*connector.HistoricalSnapshot{
		{Date: isoDay("2023-10-10"), TotalEquity: 110},
		{Date: isoDay("2023-10-11"), TotalEquity: 120},
	}
	got := mergePeriodSnapshots([][]*connector.HistoricalSnapshot{first, second})
	if len(got) != 3 {
		t.Fatalf("got %d days, want 3", len(got))
	}
	if got[1].Withdrawals != 5 {
		t.Fatal("the shared date lost the flows the earlier period read")
	}
	for n := 1; n < len(got); n++ {
		if !got[n].Date.After(got[n-1].Date) {
			t.Fatalf("series not in date order: %v then %v", got[n-1].Date, got[n].Date)
		}
	}
}

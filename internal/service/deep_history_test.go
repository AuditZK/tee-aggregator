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

func TestHistoryYears(t *testing.T) {
	for _, c := range []struct {
		from, to string
		want     int
	}{
		{"2025-10-24", "2025-10-24", 1},
		{"2024-10-25", "2025-10-24", 1},
		{"2024-10-24", "2025-10-24", 2},
		{"2022-10-11", "2025-10-09", 3},
	} {
		if got := historyYears(isoDay(c.from), isoDay(c.to)); got != c.want {
			t.Errorf("historyYears(%s, %s) = %d, want %d", c.from, c.to, got, c.want)
		}
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

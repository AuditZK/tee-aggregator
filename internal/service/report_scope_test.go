package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func scopeDay(t *testing.T, day string) time.Time {
	t.Helper()
	at, err := time.Parse(dateFormat, day)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

// calendarDays gives January 2025, from one day of the month to another.
func calendarDays(from, to int) []string {
	days := []string{}
	for d := from; d <= to; d++ {
		days = append(days, time.Date(2025, 1, d, 0, 0, 0, 0, time.UTC).Format(dateFormat))
	}
	return days
}

func connectionSnapshots(t *testing.T, exchange string, days []string) []*repository.Snapshot {
	t.Helper()
	out := []*repository.Snapshot{}
	for _, d := range days {
		out = append(out, &repository.Snapshot{Exchange: exchange, Label: "Main", Timestamp: scopeDay(t, d)})
	}
	return out
}

func TestVerifiedFromOf(t *testing.T) {
	all := calendarDays(1, 30)
	withGap := append(calendarDays(1, 5), calendarDays(20, 30)...)
	shortGap := append(calendarDays(1, 5), calendarDays(10, 30)...)
	join := func(a, b []*repository.Snapshot) []*repository.Snapshot {
		return append(append([]*repository.Snapshot{}, a...), b...)
	}

	cases := []struct {
		name      string
		snapshots []*repository.Snapshot
		rebuilt   []string
		want      string
	}{
		{"nothing rebuilt covers everything", connectionSnapshots(t, "ibkr", all), nil, "2025-01-01"},
		{"after the last rebuilt day", connectionSnapshots(t, "okx", all), []string{"2025-01-03", "2025-01-09"}, "2025-01-10"},
		{"every day rebuilt leaves no boundary", connectionSnapshots(t, "okx", all), calendarDays(1, 30), ""},
		{
			"a connection back to a series that went on moves it",
			join(connectionSnapshots(t, "binance", all), connectionSnapshots(t, "okx", withGap)),
			nil, "2025-01-20",
		},
		{
			"the later of a rebuild and a return",
			join(connectionSnapshots(t, "binance", all), connectionSnapshots(t, "okx", withGap)),
			[]string{"2025-01-25"}, "2025-01-26",
		},
		{
			"a short gap moves nothing",
			join(connectionSnapshots(t, "binance", all), connectionSnapshots(t, "okx", shortGap)),
			nil, "2025-01-01",
		},
		{
			"a gap nothing recorded through moves nothing",
			join(connectionSnapshots(t, "binance", withGap), connectionSnapshots(t, "okx", withGap)),
			nil, "2025-01-01",
		},
		{"a one-account report moves nothing", connectionSnapshots(t, "binance", withGap), nil, "2025-01-01"},
		{
			"a connection that simply joined later moves nothing",
			join(connectionSnapshots(t, "binance", all), connectionSnapshots(t, "okx", calendarDays(20, 30))),
			nil, "2025-01-01",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rebuilt := map[time.Time]struct{}{}
			for _, d := range c.rebuilt {
				rebuilt[scopeDay(t, d)] = struct{}{}
			}
			if got := verifiedFromOf(c.snapshots, rebuilt); got != c.want {
				t.Fatalf("verifiedFrom = %q, want %q", got, c.want)
			}
		})
	}
}

func TestSnapshotsInPortfolio(t *testing.T) {
	removed := scopeDay(t, "2025-01-10")
	stays := []repository.PortfolioMember{
		{Exchange: "okx", Label: "Main", AddedAt: scopeDay(t, "2025-01-05"), RemovedAt: &removed},
		{Exchange: "okx", Label: "Main", AddedAt: scopeDay(t, "2025-01-20")},
		{Exchange: "binance", Label: "Main", AddedAt: scopeDay(t, "2025-01-05")},
	}
	snapshots := []*repository.Snapshot{
		{Exchange: "OKX", Label: " main ", Timestamp: scopeDay(t, "2025-01-01")},    // before the first stay: its past comes in
		{Exchange: "okx", Label: "Main", Timestamp: scopeDay(t, "2025-01-10")},      // up to the removal
		{Exchange: "okx", Label: "Main", Timestamp: scopeDay(t, "2025-01-15")},      // between two stays: not the portfolio's
		{Exchange: "okx", Label: "Main", Timestamp: scopeDay(t, "2025-01-25")},      // second stay
		{Exchange: "binance", Label: "Other", Timestamp: scopeDay(t, "2025-01-06")}, // not a member
	}
	got := snapshotsInPortfolio(snapshots, stays)
	days := []string{}
	for _, s := range got {
		days = append(days, s.Timestamp.Format(dateFormat))
	}
	want := []string{"2025-01-01", "2025-01-10", "2025-01-25"}
	if len(days) != len(want) {
		t.Fatalf("days = %v, want %v", days, want)
	}
	for i := range want {
		if days[i] != want[i] {
			t.Fatalf("days = %v, want %v", days, want)
		}
	}
}

func TestPortfolioCompositionHidesLabels(t *testing.T) {
	removed := scopeDay(t, "2025-03-01")
	composition := portfolioComposition(&ReportPortfolio{
		Name:             "Book",
		ConnectionsTotal: 5,
		Stays: []repository.PortfolioMember{
			{Exchange: "okx", Label: "Private", AddedAt: scopeDay(t, "2025-01-01")},
			{Exchange: "binance", Label: "Secret", AddedAt: scopeDay(t, "2025-01-01"), RemovedAt: &removed},
		},
	})
	if composition.ConnectionsIncluded != 1 || composition.ConnectionsTotal != 5 {
		t.Fatalf("counts = %+v", composition)
	}
	if composition.Stays[1].RemovedAt != "2025-03-01" {
		t.Fatalf("stay = %+v", composition.Stays[1])
	}
}

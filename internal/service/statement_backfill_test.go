package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

func TestStatementWindowFrom(t *testing.T) {
	now := isoDay("2026-06-03").Add(6 * time.Hour) // Wednesday, statement to Tuesday
	window := isoDay("2026-06-02").AddDate(0, 0, -(connector.IBKRStatementWindowDays - 1))
	for _, tc := range []struct {
		name   string
		newest time.Time
		want   time.Time
	}{
		{"nothing stored", time.Time{}, window},
		{"synced yesterday", isoDay("2026-06-02"), window},
		{"after an outage", isoDay("2026-05-12"), isoDay("2026-05-09")},
	} {
		if got := statementWindowFrom(now, tc.newest); !got.Equal(tc.want) {
			t.Fatalf("%s: from %s, want %s", tc.name, got.Format(time.DateOnly), tc.want.Format(time.DateOnly))
		}
	}
}

// The backfill ends the business day before the last one, whose statement is
// out at any hour, and spans a year from the last one.
func TestStatementBackfillRange(t *testing.T) {
	from, to := statementBackfillRange(isoDay("2026-06-02").Add(time.Hour)) // Tuesday 01:00
	if want := isoDay("2026-05-29"); !to.Equal(want) {
		t.Fatalf("to %s, want Friday %s", to.Format(time.DateOnly), want.Format(time.DateOnly))
	}
	if want := isoDay("2026-06-01").AddDate(0, 0, -(connector.IBKRStatementMaxDays - 1)); !from.Equal(want) {
		t.Fatalf("from %s, want %s", from.Format(time.DateOnly), want.Format(time.DateOnly))
	}
}

// The rest of a month after a cut may come back empty when it is a holiday or
// two IBKR wrote no day for; three weeks of it may not.
func TestHolidayTail(t *testing.T) {
	end := isoDay("2026-05-29")
	if !holidayTail(isoDay("2026-05-27"), end) {
		t.Fatal("three days before the month's end read as days IBKR kept")
	}
	if holidayTail(isoDay("2026-05-08"), end) {
		t.Fatal("three weeks read as a holiday")
	}
}

func TestNextBackfillCursor(t *testing.T) {
	days := func(dates ...string) []*connector.HistoricalSnapshot {
		var out []*connector.HistoricalSnapshot
		for _, d := range dates {
			out = append(out, &connector.HistoricalSnapshot{Date: isoDay(d)})
		}
		return out
	}
	for _, tc := range []struct {
		name string
		days []*connector.HistoricalSnapshot
		end  string
		want string
	}{
		{"read to the end", days("2026-05-28", "2026-05-29"), "2026-05-29", "2026-05-30"},
		{"ends on a weekend", days("2026-05-28", "2026-05-29"), "2026-05-31", "2026-06-01"},
		{"cut short", days("2026-05-04", "2026-05-06", "2026-05-05"), "2026-05-31", "2026-05-07"},
	} {
		if got := nextBackfillCursor(tc.days, isoDay(tc.end)); !got.Equal(isoDay(tc.want)) {
			t.Fatalf("%s: next %s, want %s", tc.name, got.Format(time.DateOnly), tc.want)
		}
	}
}

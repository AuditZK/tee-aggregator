package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
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

// slowStatements serves business days from first on, and is still generating
// any period longer than slowOver days when the poll gives up. A period
// without a day is answered 1003.
type slowStatements struct {
	first    time.Time
	missing  dayWindow
	slowOver int
	asked    []dayWindow
	waits    []time.Duration
}

func (p *slowStatements) GetHistoricalSnapshotsForPeriod(ctx context.Context, from, to time.Time) ([]*connector.HistoricalSnapshot, error) {
	p.asked = append(p.asked, dayWindow{from: from, to: to})
	p.waits = append(p.waits, connector.FlexPollBudgetOf(ctx))
	if daysInWindow(from, to) > p.slowOver {
		return nil, fmt.Errorf("%w after 20 polls", connector.ErrStatementStillGenerating)
	}
	var out []*connector.HistoricalSnapshot
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		weekend := d.Weekday() == time.Saturday || d.Weekday() == time.Sunday
		missing := p.missing.isSet() && !d.Before(p.missing.from) && !d.After(p.missing.to)
		if weekend || missing || d.Before(p.first) {
			continue
		}
		out = append(out, &connector.HistoricalSnapshot{Date: d})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: synthetic", connector.ErrStatementUnavailable)
	}
	return out, nil
}

func readMonth(t *testing.T, p *slowStatements, m dayWindow) ([]time.Time, error) {
	t.Helper()
	saved := statementBackfillSpacing
	statementBackfillSpacing = 0
	t.Cleanup(func() { statementBackfillSpacing = saved })

	r := &statementReader{s: &SyncService{logger: zap.NewNop()}, provider: p, maxReads: statementBackfillMaxReads}
	periods, err := r.month(context.Background(), m)
	var days []time.Time
	for _, period := range periods {
		for _, d := range period {
			days = append(days, d.Date)
		}
	}
	return days, err
}

func businessDays(from, to time.Time) int {
	n := 0
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		if d.Weekday() != time.Saturday && d.Weekday() != time.Sunday {
			n++
		}
	}
	return n
}

// A month IBKR is still generating when the poll gives up failed the backfill
// on every attempt. It is asked again in halves, and every day is read once.
func TestStatementMonthSlowToGenerateIsReadInShorterPeriods(t *testing.T) {
	m := dayWindow{from: isoDay("2026-03-01"), to: isoDay("2026-03-31")}
	p := &slowStatements{first: isoDay("2026-01-01"), slowOver: 16}

	days, err := readMonth(t, p, m)
	if err != nil {
		t.Fatalf("slow month: %v", err)
	}
	if want := businessDays(m.from, m.to); len(days) != want {
		t.Fatalf("%d days read, want the month's %d", len(days), want)
	}
	seen := map[time.Time]bool{}
	for _, d := range days {
		if seen[d] {
			t.Fatalf("%s read twice", d.Format(time.DateOnly))
		}
		seen[d] = true
	}
	want := []dayWindow{
		{from: isoDay("2026-03-01"), to: isoDay("2026-03-31")},
		{from: isoDay("2026-03-01"), to: isoDay("2026-03-16")},
		{from: isoDay("2026-03-17"), to: isoDay("2026-03-31")},
	}
	if len(p.asked) != len(want) {
		t.Fatalf("%d statements asked, want %d: %v", len(p.asked), len(want), p.asked)
	}
	for n, w := range want {
		if !p.asked[n].from.Equal(w.from) || !p.asked[n].to.Equal(w.to) {
			t.Fatalf("statement %d is %s..%s, want %s..%s", n, p.asked[n].from.Format(time.DateOnly), p.asked[n].to.Format(time.DateOnly), w.from.Format(time.DateOnly), w.to.Format(time.DateOnly))
		}
	}
}

// Each period still generating is a failure on the token's daily budget: the
// halving stops at statementBackfillMinDays and the month fails.
func TestStatementMonthStopsHalvingAtTheShortestPeriod(t *testing.T) {
	m := dayWindow{from: isoDay("2026-03-01"), to: isoDay("2026-03-31")}
	p := &slowStatements{first: isoDay("2026-01-01"), slowOver: 0}

	_, err := readMonth(t, p, m)
	if !errors.Is(err, connector.ErrStatementStillGenerating) {
		t.Fatalf("month never generated: %v, want the still-generating failure", err)
	}
	if len(p.asked) != 3 {
		t.Fatalf("%d statements asked, want 31, 16 then 8 days: %v", len(p.asked), p.asked)
	}
	if last := p.asked[len(p.asked)-1]; daysInWindow(last.from, last.to) != statementBackfillMinDays {
		t.Fatalf("last period is %d days, want %d", daysInWindow(last.from, last.to), statementBackfillMinDays)
	}
}

// Halved, the older half of the account's first month has no statement. That
// is not the month having none: the days after it are read.
func TestStatementMonthHalvedKeepsReadingPastTheAccountStart(t *testing.T) {
	m := dayWindow{from: isoDay("2026-03-01"), to: isoDay("2026-03-31")}
	first := isoDay("2026-03-20")
	p := &slowStatements{first: first, slowOver: 16}

	days, err := readMonth(t, p, m)
	if err != nil {
		t.Fatalf("first month: %v", err)
	}
	if want := businessDays(first, m.to); len(days) != want {
		t.Fatalf("%d days read, want the %d from the account's first", len(days), want)
	}
}

// A shortened period without a day, after days of its month were read, is a
// hole IBKR left: the month fails rather than be written around it.
func TestStatementMonthHalvedFailsOnAHole(t *testing.T) {
	m := dayWindow{from: isoDay("2026-03-01"), to: isoDay("2026-03-31")}
	p := &slowStatements{
		first:    isoDay("2026-01-01"),
		slowOver: 8,
		missing:  dayWindow{from: isoDay("2026-03-09"), to: isoDay("2026-03-16")},
	}

	_, err := readMonth(t, p, m)
	if err == nil || !strings.Contains(err.Error(), "has no day") {
		t.Fatalf("hole in a halved month: %v, want a failure naming the empty period", err)
	}
}

// A sync gives up on a statement after the default poll because a day depends
// on it. A backfill asks for each of its statements with the longer wait.
func TestStatementBackfillWaitsLongerThanASyncForAStatement(t *testing.T) {
	if statementBackfillWait <= connector.FlexPollBudget() {
		t.Fatalf("backfill wait %v does not outlast the default poll %v", statementBackfillWait, connector.FlexPollBudget())
	}
	m := dayWindow{from: isoDay("2026-03-01"), to: isoDay("2026-03-31")}
	p := &slowStatements{first: isoDay("2026-01-01"), slowOver: 16}

	if _, err := readMonth(t, p, m); err != nil {
		t.Fatalf("month: %v", err)
	}
	for n, wait := range p.waits {
		if wait != statementBackfillWait {
			t.Fatalf("statement %d asked with a wait of %v, want %v", n, wait, statementBackfillWait)
		}
	}
}

type deadlineStatements struct {
	deadline time.Time
	hasOne   bool
	ctxErr   error
}

func (p *deadlineStatements) GetHistoricalSnapshotsForPeriod(ctx context.Context, _, _ time.Time) ([]*connector.HistoricalSnapshot, error) {
	p.deadline, p.hasOne = ctx.Deadline()
	p.ctxErr = ctx.Err()
	return nil, errors.New("synthetic: stop here")
}

// The admin route and the connect hook hand a full reconstruction a deadline
// sized for a rebuilder call, or none: either way it runs on the backfill's.
func TestReconstructFromStatementsRunsOnTheBackfillDeadline(t *testing.T) {
	s := &SyncService{logger: zap.NewNop()}
	connMeta := &repository.ExchangeConnection{UserUID: "user-uid-1", Exchange: "ibkr", Label: "main"}
	p := &deadlineStatements{}

	caller, cancel := context.WithTimeout(context.Background(), time.Minute)
	cancel()
	s.reconstructFromStatements(caller, connMeta, p, reconstructOpts{})

	if !p.hasOne {
		t.Fatal("full reconstruction ran without a deadline")
	}
	if left := time.Until(p.deadline); left < statementBackfillTimeout-time.Minute || left > statementBackfillTimeout {
		t.Fatalf("deadline in %v, want the backfill's %v", left, statementBackfillTimeout)
	}
	if p.ctxErr != nil {
		t.Fatalf("the caller's cancelled deadline reached the backfill: %v", p.ctxErr)
	}
}

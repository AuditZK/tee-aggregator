package connector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLastStatementDay_IsTheWeekdayBefore(t *testing.T) {
	for _, tc := range []struct{ now, want string }{
		{"2026-06-02T00:00:00Z", "2026-06-01"}, // Tuesday: Monday
		{"2026-06-02T23:59:00Z", "2026-06-01"},
		{"2026-06-01T06:00:00Z", "2026-05-29"}, // Monday: Friday
		{"2026-05-31T12:00:00Z", "2026-05-29"}, // Sunday: Friday
		{"2026-05-30T12:00:00Z", "2026-05-29"}, // Saturday: Friday
	} {
		now, _ := time.Parse(time.RFC3339, tc.now)
		if got := LastStatementDay(now).Format("2006-01-02"); got != tc.want {
			t.Fatalf("LastStatementDay(%s) = %s, want %s", tc.now, got, tc.want)
		}
	}
}

func TestDailyStatementFrom_StaysBetweenTheWindowAndAYear(t *testing.T) {
	to := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	window := to.AddDate(0, 0, -(IBKRStatementWindowDays - 1))
	for _, tc := range []struct {
		name string
		from time.Time
		want time.Time
	}{
		{"zero", time.Time{}, window},
		{"inside the window", to.AddDate(0, 0, -2), window},
		{"before the window", to.AddDate(0, 0, -40), to.AddDate(0, 0, -40)},
		{"beyond a year", to.AddDate(-2, 0, 0), to.AddDate(0, 0, -(IBKRStatementMaxDays - 1))},
		{"mid-day", to.AddDate(0, 0, -40).Add(15 * time.Hour), to.AddDate(0, 0, -40)},
	} {
		if got := dailyStatementFrom(tc.from, to); !got.Equal(tc.want) {
			t.Fatalf("%s: from = %s, want %s", tc.name, got.Format(time.DateOnly), tc.want.Format(time.DateOnly))
		}
	}
}

// The reconstruction asks for the widest period, then the live reads ask for
// less: they must be answered by the statement already fetched, or the token's
// cooldown refuses the second request and the sync loses its day.
func TestDailyStatement_LiveReadsReuseTheWiderStatement(t *testing.T) {
	const token = "SYNTHETICWINDOWTOKEN0001"
	to := LastStatementDay(time.Now())
	stub := &flexStubTransport{doc: string(flexReport(`
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="` + to.Format("20060102") + `" total="1200" cash="1200"/>
      </EquitySummaryInBase>`))}
	i := &IBKR{token: token, queryID: "123456", client: &http.Client{Transport: stub}}
	t.Cleanup(func() {
		flexReportCacheMu.Lock()
		delete(flexReportCache, token+":123456")
		flexReportCacheMu.Unlock()
	})

	since := to.AddDate(0, 0, -40)
	if _, err := i.GetHistoricalSnapshots(context.Background(), since); err != nil {
		t.Fatalf("GetHistoricalSnapshots: %v", err)
	}
	if _, err := i.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance after the reconstruction: %v", err)
	}
	if _, err := i.GetCashflows(context.Background(), to.AddDate(0, 0, -3)); err != nil {
		t.Fatalf("GetCashflows after the reconstruction: %v", err)
	}

	var sends []string
	for _, u := range stub.urls {
		if strings.Contains(u, "SendRequest") {
			sends = append(sends, u)
		}
	}
	if len(sends) != 1 {
		t.Fatalf("%d statements requested, want the reconstruction's alone: %v", len(sends), sends)
	}
	if !strings.HasPrefix(sends[0], ibkrFlexWSSendURL) ||
		!strings.Contains(sends[0], "fd="+since.Format("20060102")) ||
		!strings.Contains(sends[0], "td="+to.Format("20060102")) {
		t.Fatalf("statement requested as %s, want the Flex Web Service from %s to %s", sends[0], since.Format("20060102"), to.Format("20060102"))
	}
}

func TestCachedStatement_AnswersOnlyAPeriodItCovers(t *testing.T) {
	const key = "synthetic-cover:000003"
	to := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	flexReportCacheMu.Lock()
	flexReportCache[key] = &flexReportEntry{xml: []byte("<x/>"), from: to.AddDate(0, 0, -30), to: to, fetchedAt: time.Now()}
	flexReportCacheMu.Unlock()
	t.Cleanup(func() {
		flexReportCacheMu.Lock()
		delete(flexReportCache, key)
		flexReportCacheMu.Unlock()
	})

	if _, ok := cachedStatement(key, to.AddDate(0, 0, -13), to, time.Minute); !ok {
		t.Fatal("a narrower period ending the same day must be answered from the cache")
	}
	if _, ok := cachedStatement(key, to.AddDate(0, 0, -60), to, time.Minute); ok {
		t.Fatal("a period starting earlier than the cached one must not be answered by it")
	}
	if _, ok := cachedStatement(key, to.AddDate(0, 0, -13), to.AddDate(0, 0, 1), time.Minute); ok {
		t.Fatal("a statement ending on another day must not be answered by it")
	}
}

type flexUnavailableTransport struct{}

func (flexUnavailableTransport) RoundTrip(*http.Request) (*http.Response, error) {
	body := `<FlexStatementResponse><Status>Fail</Status><ErrorCode>1003</ErrorCode><ErrorMessage>Statement is not available.</ErrorMessage></FlexStatementResponse>`
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
}

// IBKR answers 1003 for a day whose statement is not out yet, which is every
// night at midnight UTC: the sync must read it as a statement to come, not as
// a broken connection.
func TestDailyStatement_UnpublishedDayIsTransient(t *testing.T) {
	i := &IBKR{token: "SYNTHETICWINDOWTOKEN0002", queryID: "123456", client: &http.Client{Transport: flexUnavailableTransport{}}}

	_, err := i.GetBalance(context.Background())
	if !errors.Is(err, ErrStatementNotPublished) || !errors.Is(err, ErrTransient) {
		t.Fatalf("GetBalance on an unpublished day: %v, want ErrStatementNotPublished, transient", err)
	}

	// A chosen period keeps 1003 as IBKR states it: an old period with no
	// statement is not one that comes out tonight.
	day := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	i = &IBKR{token: "SYNTHETICWINDOWTOKEN0003", queryID: "123456", client: &http.Client{Transport: flexUnavailableTransport{}}}
	_, err = i.GetHistoricalSnapshotsForPeriod(context.Background(), day, day)
	if !errors.Is(err, ErrStatementUnavailable) || errors.Is(err, ErrTransient) {
		t.Fatalf("chosen period with no statement: %v, want ErrStatementUnavailable, not transient", err)
	}
}

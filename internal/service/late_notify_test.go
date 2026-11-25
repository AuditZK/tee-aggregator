package service

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/repository"
)

// pingSink stands in for analytics' POST /api/v1/sync/user/<uid>: it counts
// the pings per user and, when onHit is set, runs it while the ping is in
// flight, so a test can look at what the database holds at that moment.
type pingSink struct {
	srv   *httptest.Server
	mu    sync.Mutex
	hits  map[string]int
	onHit func(userUID string)
}

func newPingSink(t *testing.T) *pingSink {
	t.Helper()
	p := &pingSink{hits: map[string]int{}}
	const prefix = "/api/v1/sync/user/"
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || len(r.URL.Path) <= len(prefix) || r.URL.Path[:len(prefix)] != prefix {
			http.NotFound(w, r)
			return
		}
		user := r.URL.Path[len(prefix):]
		p.mu.Lock()
		p.hits[user]++
		onHit := p.onHit
		p.mu.Unlock()
		if onHit != nil {
			onHit(user)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *pingSink) url() string { return p.srv.URL + "/api/v1/sync/user" }

func (p *pingSink) count(userUID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hits[userUID]
}

// wirePings points s at the sink and runs every late-write ping inline, so a
// test can count them as soon as the call that sends them returns.
func wirePings(s *SyncService, p *pingSink) {
	s.SetHistoryNotify(p.url(), "")
	s.notifyRun = func(f func()) { f() }
}

func TestLateForNightlyAnalytics(t *testing.T) {
	day := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		at   time.Time
		late bool
	}{
		{day.Add(2 * time.Minute), false},
		{day.Add(13*time.Minute + 59*time.Second), false},
		{day.Add(14 * time.Minute), true},
		{day.Add(6*time.Hour + 5*time.Minute), true}, // the 2026-11-25 retry
		{day.Add(23*time.Hour + 59*time.Minute), true},
		{day.Add(24*time.Hour + time.Minute), false}, // the next midnight pass
		// The same instant in another zone is judged in UTC.
		{day.Add(6 * time.Hour).In(time.FixedZone("UTC+7", 7*3600)), true},
	} {
		if got := lateForNightlyAnalytics(tc.at); got != tc.late {
			t.Errorf("lateForNightlyAnalytics(%s) = %v, want %v", tc.at.UTC().Format(time.RFC3339), got, tc.late)
		}
	}
}

func TestWantsAnalyticsNotify(t *testing.T) {
	for _, tc := range []struct {
		name    string
		results []*SyncResult
		want    bool
	}{
		{"nothing ran", nil, false},
		{"failed, nothing written", []*SyncResult{{Error: "get balance: sync failed"}}, false},
		{"live row saved", []*SyncResult{{Success: true}}, true},
		{"reconstruction wrote, live read failed", []*SyncResult{{Error: "get balance: sync failed", historyWritten: true}}, true},
		{"first row of a connection whose backfill starts", []*SyncResult{{Success: true, backfillDue: true}}, false},
		{"one account wrote, another's backfill starts", []*SyncResult{{Success: true, backfillDue: true}, {Success: true}}, true},
		{"nil result", []*SyncResult{nil}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := wantsAnalyticsNotify(tc.results...); got != tc.want {
				t.Fatalf("wantsAnalyticsNotify = %v, want %v", got, tc.want)
			}
		})
	}
}

// Each retry that wrote pings for its own user as it ends, whatever the other
// retries of the pass do: a restart may drop them before they fire.
func TestFinishDeferredRetryPingsWhenItWrote(t *testing.T) {
	sink := newPingSink(t)
	s := &SyncService{logger: zap.NewNop(), deferredRetries: map[string]bool{"cto": true, "pea": true}}
	wirePings(s, sink)

	s.finishDeferredRetry(&repository.ExchangeConnection{ID: "cto", UserUID: "user-1"}, true)
	if n := sink.count("user-1"); n != 1 {
		t.Fatalf("%d pings after a retry that wrote, want 1 at once", n)
	}
	s.finishDeferredRetry(&repository.ExchangeConnection{ID: "pea", UserUID: "user-1"}, false)
	if n := sink.count("user-1"); n != 1 {
		t.Fatalf("%d pings after a retry that wrote nothing, want still 1", n)
	}
	if len(s.deferredRetries) != 0 {
		t.Fatalf("retry slots left behind: %v", s.deferredRetries)
	}
}

// A sync that finds a backfill claim held leaves its ping to the claim, and
// the ping goes out when the claim is released, whoever held it.
func TestReleaseBackfillSendsThePingASyncLeftToIt(t *testing.T) {
	sink := newPingSink(t)
	s := &SyncService{logger: zap.NewNop()}
	wirePings(s, sink)
	conn := &repository.ExchangeConnection{UserUID: "user-1", Exchange: "ibkr", Label: "main"}

	if !s.claimBackfill(conn) {
		t.Fatal("first claim refused")
	}
	if s.claimBackfillOrOwe(conn, false) {
		t.Fatal("a held claim was granted twice")
	}
	s.releaseBackfill(conn)
	if n := sink.count("user-1"); n != 0 {
		t.Fatalf("%d pings on releasing a claim nobody left a ping to, want none", n)
	}

	if !s.claimBackfill(conn) {
		t.Fatal("claim refused after its release")
	}
	if s.claimBackfillOrOwe(conn, true) {
		t.Fatal("a held claim was granted to a sync")
	}
	if n := sink.count("user-1"); n != 0 {
		t.Fatalf("%d pings while the claim is held, want none yet", n)
	}
	s.releaseBackfill(conn)
	if n := sink.count("user-1"); n != 1 {
		t.Fatalf("%d pings on release, want the one the sync left", n)
	}
}

// No URL configured: nothing to send, and nothing scheduled to send it.
func TestNotifyAnalyticsAsyncWithoutURL(t *testing.T) {
	s := &SyncService{logger: zap.NewNop(), notifyRun: func(func()) { t.Fatal("a ping was scheduled with no URL configured") }}
	s.notifyAnalyticsAsync("user-1", "test")
}

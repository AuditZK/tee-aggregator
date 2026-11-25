package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// errFlexThrottled is a Flex token rate limit as the connector words it.
var errFlexThrottled = errors.New("flex request failed: 1018 Too many requests have been made from this token")

// errVenueThrottled is an HTTP 429 as any venue words it.
var errVenueThrottled = errors.New("get balance: 429 Too many requests")

// throttledStatements is a Flex-like account whose token is rate limited until
// released: the statement and the live balance both answer 1018, the way IBKR
// does at midnight when a token has just been read.
type throttledStatements struct {
	*statementConnector
	throttled atomic.Bool
}

func (c *throttledStatements) GetBalance(ctx context.Context) (*connector.Balance, error) {
	if c.throttled.Load() {
		return nil, errFlexThrottled
	}
	return c.statementConnector.GetBalance(ctx)
}

func (c *throttledStatements) GetHistoricalSnapshots(ctx context.Context, since time.Time) ([]*connector.HistoricalSnapshot, error) {
	if c.throttled.Load() {
		return nil, errFlexThrottled
	}
	return c.statementConnector.GetHistoricalSnapshots(ctx, since)
}

// refusedBackfill is a statement account whose period statements are refused:
// its year's backfill fails on the first month.
type refusedBackfill struct{ *periodicStatementConnector }

func (c *refusedBackfill) GetHistoricalSnapshotsForPeriod(context.Context, time.Time, time.Time) ([]*connector.HistoricalSnapshot, error) {
	return nil, errors.New("synthetic: statement refused")
}

// retryTimers holds the deferred retries a pass arms instead of timing them.
type retryTimers struct {
	mu   sync.Mutex
	fire []func()
}

func captureRetries(h *dbHarness) *retryTimers {
	r := &retryTimers{}
	h.sync.deferAfter = func(_ time.Duration, f func()) {
		r.mu.Lock()
		r.fire = append(r.fire, f)
		r.mu.Unlock()
	}
	return r
}

func (r *retryTimers) armed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.fire)
}

func (r *retryTimers) run(t *testing.T, i int) {
	t.Helper()
	r.mu.Lock()
	if i >= len(r.fire) {
		r.mu.Unlock()
		t.Fatalf("retry %d was never armed", i)
	}
	f := r.fire[i]
	r.mu.Unlock()
	f()
}

// atClock places the scheduled pass's writes at a time of day.
func atClock(t *testing.T, at time.Time) {
	t.Helper()
	saved := lateWriteClock
	lateWriteClock = func() time.Time { return at }
	t.Cleanup(func() { lateWriteClock = saved })
}

func (p *pingSink) setOnHit(f func(string)) {
	p.mu.Lock()
	p.onHit = f
	p.mu.Unlock()
}

func (h *dbHarness) hasDay(exchange, label string, day time.Time) bool {
	_, err := h.snaps.GetByUserExchangeLabelAndDate(h.ctx, dbUser, exchange, label, day)
	return err == nil
}

func okConnector(exchange string) *scriptedConnector {
	return &scriptedConnector{exchange: exchange, balances: []connector.Balance{{Equity: 1000, Available: 1000}}}
}

func (c *scriptedConnector) setBalanceErr(err error) {
	c.mu.Lock()
	c.balanceErr = err
	c.mu.Unlock()
}

// midnightRateLimitedIBKR runs the 2026-11-25 midnight pass: the Flex token is
// rate limited, nothing is written, a deferred retry is armed.
func midnightRateLimitedIBKR(t *testing.T) (*dbHarness, *pingSink, *retryTimers, *throttledStatements, time.Time) {
	t.Helper()
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	h.sync.SetFXSource(stubFX{})
	sink := newPingSink(t)
	wirePings(h.sync, sink)
	retries := captureRetries(h)
	asOf := lastExpectedStatementDate(startOfTodayUTC())
	acct := &throttledStatements{statementConnector: newStatementAccount(asOf)}
	acct.throttled.Store(true)
	h.plant(dbKey, dbSecret, acct)

	atClock(t, startOfTodayUTC().Add(time.Minute))
	results, err := h.sync.SyncUserScheduledDueAtomic(h.ctx, dbUser, time.Now().UTC())
	if err != nil || len(results) != 1 || !results[0].rateLimited() {
		t.Fatalf("midnight pass: err %v, results %+v, want one rate-limited result", err, results)
	}
	if n := retries.armed(); n != 1 {
		t.Fatalf("%d deferred retries armed, want 1", n)
	}
	if n := sink.count(dbUser); n != 0 {
		t.Fatalf("%d pings from a midnight pass that wrote nothing, want none", n)
	}
	return h, sink, retries, acct, asOf
}

// The 2026-11-25 incident. The midnight pass is rate limited and writes
// nothing, analytics copies the stale history at 00:15, then the deferred
// retry writes the statement days and the live row hours later. Analytics
// must hear of it once, after those rows are in.
func TestDBDeferredRetryNotifiesAnalyticsOnceAfterItsWrite(t *testing.T) {
	h, sink, retries, acct, asOf := midnightRateLimitedIBKR(t)

	var liveAtPing, statementDayAtPing atomic.Bool
	sink.setOnHit(func(string) {
		liveAtPing.Store(h.hasDay("ibkr", "main", startOfTodayUTC()))
		statementDayAtPing.Store(h.hasDay("ibkr", "main", asOf))
	})
	acct.throttled.Store(false)
	retries.run(t, 0)

	if n := sink.count(dbUser); n != 1 {
		t.Fatalf("%d pings after the retry wrote, want exactly 1", n)
	}
	if !liveAtPing.Load() || !statementDayAtPing.Load() {
		t.Fatalf("at ping time: live row written %v, statement day written %v, want both", liveAtPing.Load(), statementDayAtPing.Load())
	}
	if st := h.status(t, "ibkr", "main"); st.Status != "completed" {
		t.Fatalf("status %q after the retry, want completed", st.Status)
	}
}

// A retry that is refused again writes nothing, and analytics is not bothered.
func TestDBDeferredRetryThatStillFailsDoesNotNotify(t *testing.T) {
	h, sink, retries, _, _ := midnightRateLimitedIBKR(t)

	retries.run(t, 0)

	if n := sink.count(dbUser); n != 0 {
		t.Fatalf("%d pings after a retry that wrote nothing, want none", n)
	}
	if h.hasDay("ibkr", "main", startOfTodayUTC()) {
		t.Fatal("a refused retry wrote a live row")
	}
}

// Two accounts of one user, both retried from the same pass: one ping, after
// the second retry has written.
func TestDBDeferredRetriesOfOneUserNotifyOnce(t *testing.T) {
	h := newDBHarness(t)
	sink := newPingSink(t)
	wirePings(h.sync, sink)
	retries := captureRetries(h)
	atClock(t, startOfTodayUTC().Add(time.Minute))
	var accounts []*scriptedConnector
	for _, label := range []string{"cto", "pea"} {
		key, secret := "key-"+label, "secret-"+label
		h.seedConnection(t, "binance", label, key, secret)
		c := okConnector("binance")
		c.balanceErr = errVenueThrottled
		h.plant(key, secret, c)
		accounts = append(accounts, c)
	}

	results, err := h.sync.SyncUserScheduledDueAtomic(h.ctx, dbUser, time.Now().UTC())
	if err != nil || len(results) != 2 {
		t.Fatalf("midnight pass: err %v, %d results, want 2", err, len(results))
	}
	if n := retries.armed(); n != 2 {
		t.Fatalf("%d deferred retries armed, want 2", n)
	}
	for _, c := range accounts {
		c.setBalanceErr(nil)
	}

	var bothAtPing atomic.Bool
	sink.setOnHit(func(string) {
		bothAtPing.Store(h.hasDay("binance", "cto", startOfTodayUTC()) && h.hasDay("binance", "pea", startOfTodayUTC()))
	})
	retries.run(t, 0)
	if n := sink.count(dbUser); n != 0 {
		t.Fatalf("%d pings before the user's last retry ended, want none yet", n)
	}
	retries.run(t, 1)
	if n := sink.count(dbUser); n != 1 {
		t.Fatalf("%d pings after both retries wrote, want exactly 1", n)
	}
	if !bothAtPing.Load() {
		t.Fatal("the ping went out before both accounts' rows were written")
	}
}

// The ping is best effort: analytics unreachable or failing changes nothing in
// what the retry wrote or recorded, and says so at warn.
func TestDBDeferredRetryKeepsItsWriteWhenAnalyticsIsDown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		url     func(t *testing.T) string
		warning string
	}{
		{"unreachable", func(t *testing.T) string {
			srv := httptest.NewServer(http.NotFoundHandler())
			srv.Close()
			return srv.URL
		}, "history-rebuilt notify failed"},
		{"answers 500", func(t *testing.T) string {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			}))
			t.Cleanup(srv.Close)
			return srv.URL
		}, "history-rebuilt notify rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)
			core, logs := observer.New(zapcore.WarnLevel)
			h.sync.logger = zap.New(core)
			h.sync.SetHistoryNotify(tc.url(t)+"/api/v1/sync/user", "")
			h.sync.notifyRun = func(f func()) { f() }
			retries := captureRetries(h)
			atClock(t, startOfTodayUTC().Add(time.Minute))
			c := okConnector("binance")
			c.balanceErr = errVenueThrottled
			h.plant(dbKey, dbSecret, c)

			if _, err := h.sync.SyncUserScheduledDueAtomic(h.ctx, dbUser, time.Now().UTC()); err != nil {
				t.Fatalf("midnight pass: %v", err)
			}
			c.setBalanceErr(nil)
			retries.run(t, 0)

			if snap := h.snapshotToday(t, "binance", "main"); snap.TotalEquity != 1000 {
				t.Fatalf("live row equity %v, want 1000", snap.TotalEquity)
			}
			if st := h.status(t, "binance", "main"); st.Status != "completed" {
				t.Fatalf("status %q, want completed", st.Status)
			}
			if n := logs.FilterMessage(tc.warning).Len(); n != 1 {
				t.Fatalf("%d %q warnings, want 1", n, tc.warning)
			}
		})
	}
}

// A manual sync writes a connection's first row at any hour: one ping for the
// user, whatever the number of accounts it wrote.
func TestDBManualSyncOfSeveralAccountsNotifiesOnce(t *testing.T) {
	h := newDBHarness(t)
	sink := newPingSink(t)
	wirePings(h.sync, sink)
	for _, label := range []string{"cto", "pea"} {
		key, secret := "key-"+label, "secret-"+label
		h.seedConnection(t, "binance", label, key, secret)
		h.plant(key, secret, okConnector("binance"))
	}

	results, err := h.sync.SyncUser(h.ctx, dbUser)
	if err != nil || len(results) != 2 || !results[0].Success || !results[1].Success {
		t.Fatalf("manual sync: err %v, results %+v", err, results)
	}
	if n := sink.count(dbUser); n != 1 {
		t.Fatalf("%d pings after a manual sync of two accounts, want 1", n)
	}
}

// The first sync after connect pings for its row unless a rebuild follows it:
// that rebuild pings once it ends.
func TestDBFirstSyncAfterConnectNotifiesUnlessARebuildFollows(t *testing.T) {
	for _, tc := range []struct {
		rebuildFollows bool
		want           int
	}{{false, 1}, {true, 0}} {
		t.Run(map[bool]string{false: "no rebuild", true: "rebuild follows"}[tc.rebuildFollows], func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)
			h.plant(dbKey, dbSecret, okConnector("binance"))
			sink := newPingSink(t)
			wirePings(h.sync, sink)

			if res := h.sync.SyncConnectionOnConnect(h.ctx, dbUser, "binance", "main", tc.rebuildFollows); !res.Success {
				t.Fatalf("first sync: %+v", res)
			}
			if n := sink.count(dbUser); n != tc.want {
				t.Fatalf("%d pings, want %d", n, tc.want)
			}
		})
	}
}

// At connect the rebuild owns the ping and sends it however it ends. An
// exchange the deployed rebuilder does not register, or no rebuilder at all,
// used to end without one, and the first live row waited for the nightly copy.
// A dry run writes nothing and pings nothing.
func TestDBConnectWithARebuildPingsOnceEvenWhenNothingIsRebuilt(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	h.plant(dbKey, dbSecret, okConnector("binance"))
	sink := newPingSink(t)
	wirePings(h.sync, sink)

	if res := h.sync.SyncConnectionOnConnect(h.ctx, dbUser, "binance", "main", true); !res.Success {
		t.Fatalf("first sync: %+v", res)
	}
	// No rebuilder client wired: the rebuild has nowhere to go.
	h.sync.ReconstructHistoryOnConnect(h.ctx, dbUser, "binance", "main")
	if n := sink.count(dbUser); n != 1 {
		t.Fatalf("%d pings for the connect, want exactly 1", n)
	}

	h.sync.DryRunReconstructRange(h.ctx, dbUser, "binance", "main", startOfTodayUTC().AddDate(0, 0, -7), startOfTodayUTC().AddDate(0, 0, -1))
	if n := sink.count(dbUser); n != 1 {
		t.Fatalf("%d pings after a dry run, want still 1", n)
	}
}

// The midnight pass is analytics' nightly input. Its rows ping only when they
// land after the nightly copy: a pass that ran long, or admin sync-now.
func TestDBScheduledPassNotifiesOnlyAfterTheNightlyCopy(t *testing.T) {
	for _, tc := range []struct {
		name string
		at   time.Duration
		want int
	}{
		{"before the nightly copy", 5 * time.Minute, 0},
		{"after it", 6 * time.Hour, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)
			h.plant(dbKey, dbSecret, okConnector("binance"))
			sink := newPingSink(t)
			wirePings(h.sync, sink)
			atClock(t, startOfTodayUTC().Add(tc.at))

			results, err := h.sync.SyncUserScheduledDueAtomic(h.ctx, dbUser, time.Now().UTC())
			if err != nil || len(results) != 1 || !results[0].Success {
				t.Fatalf("scheduled pass: err %v, results %+v", err, results)
			}
			if n := sink.count(dbUser); n != tc.want {
				t.Fatalf("%d pings, want %d", n, tc.want)
			}
		})
	}
}

// A new IBKR connection's first sync writes its live row and starts the year's
// statement backfill. The sync leaves the ping to the backfill, which sends it
// however it ends: one live day mirrored first would show the account as
// imported while its history is still being read.
func TestDBStatementBackfillOwnsTheFirstSyncsPing(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail bool
	}{{"backfill succeeds", false}, {"backfill fails", true}} {
		t.Run(tc.name, func(t *testing.T) {
			withoutBackfillSpacing(t)
			h := newDBHarness(t)
			h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
			h.sync.SetFXSource(stubFX{})
			sink := newPingSink(t)
			wirePings(h.sync, sink)
			asOf := lastExpectedStatementDate(startOfTodayUTC())
			var acct connector.Connector = newPeriodicAccount(asOf.AddDate(0, 0, -60), asOf)
			if tc.fail {
				acct = &refusedBackfill{acct.(*periodicStatementConnector)}
			}
			h.plant(dbKey, dbSecret, acct)

			res := h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, "ibkr", "main")
			if !res.Success || !res.backfillDue {
				t.Fatalf("first sync: %+v, want a live row with the backfill due", res)
			}

			connMeta := h.connection(t, "ibkr", "main")
			deadline := time.Now().Add(10 * time.Second)
			for sink.count(dbUser) == 0 || h.backfillRunning(connMeta) {
				if time.Now().After(deadline) {
					t.Fatalf("backfill still running %v, %d pings, after 10s", h.backfillRunning(connMeta), sink.count(dbUser))
				}
				time.Sleep(20 * time.Millisecond)
			}
			if n := sink.count(dbUser); n != 1 {
				t.Fatalf("%d pings for the sync and its backfill, want exactly 1", n)
			}
			if done, err := h.connSvc.HistoryBackfilled(h.ctx, connMeta.ID); err != nil || done == tc.fail {
				t.Fatalf("backfill recorded done=%v (%v), want %v", done, err, !tc.fail)
			}
		})
	}
}

func (h *dbHarness) backfillRunning(connMeta *repository.ExchangeConnection) bool {
	if !h.sync.claimBackfill(connMeta) {
		return true
	}
	h.sync.releaseBackfill(connMeta)
	return false
}

// Deleting a connection's rebuilt days changes the user's stats as much as
// writing them.
func TestDBDeletingRebuiltHistoryNotifies(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "hyperliquid", "main", dbKey, dbSecret)
	sink := newPingSink(t)
	wirePings(h.sync, sink)
	if err := h.snaps.UpsertBatch(h.ctx, []*repository.Snapshot{{
		UserUID: dbUser, Exchange: "hyperliquid", Label: "main", Timestamp: startOfTodayUTC().AddDate(0, 0, -10),
		TotalEquity: 1000, RealizedBalance: 1000, IsHistorical: true, FromExternalRebuilder: true,
	}}); err != nil {
		t.Fatalf("store a rebuilt day: %v", err)
	}

	deleted, err := h.sync.DeleteRebuiltHistory(h.ctx, dbUser, "hyperliquid", "main")
	if err != nil || deleted != 1 {
		t.Fatalf("delete: %d rows, %v, want 1", deleted, err)
	}
	if n := sink.count(dbUser); n != 1 {
		t.Fatalf("%d pings after the deletion, want 1", n)
	}

	if deleted, err := h.sync.DeleteRebuiltHistory(h.ctx, dbUser, "hyperliquid", "main"); err != nil || deleted != 0 {
		t.Fatalf("second delete: %d rows, %v, want none", deleted, err)
	}
	if n := sink.count(dbUser); n != 1 {
		t.Fatalf("%d pings after a deletion of nothing, want still 1", n)
	}
}

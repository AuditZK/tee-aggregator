package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// periodicStatementConnector is a statement venue that also reads a chosen
// period, and cuts each statement after maxDays days the way IBKR does under
// load: the oldest kept, nothing said.
type periodicStatementConnector struct {
	*statementConnector
	maxDays      int
	notPublished bool

	mu      sync.Mutex
	periods []dayWindow
	since   []time.Time
}

func (c *periodicStatementConnector) days(from, to time.Time) []*connector.HistoricalSnapshot {
	all, _ := c.statementConnector.GetHistoricalSnapshots(context.Background(), from)
	sort.Slice(all, func(i, j int) bool { return all[i].Date.Before(all[j].Date) })
	var out []*connector.HistoricalSnapshot
	for _, h := range all {
		if !h.Date.Before(from) && !h.Date.After(to) {
			out = append(out, h)
		}
	}
	return out
}

func (c *periodicStatementConnector) GetHistoricalSnapshotsForPeriod(_ context.Context, from, to time.Time) ([]*connector.HistoricalSnapshot, error) {
	c.mu.Lock()
	c.periods = append(c.periods, dayWindow{from: from, to: to})
	c.mu.Unlock()
	out := c.days(from, to)
	if c.maxDays > 0 && len(out) > c.maxDays {
		out = out[:c.maxDays]
	}
	return out, nil
}

func (c *periodicStatementConnector) GetHistoricalSnapshots(_ context.Context, since time.Time) ([]*connector.HistoricalSnapshot, error) {
	c.mu.Lock()
	c.since = append(c.since, since)
	c.mu.Unlock()
	if c.notPublished {
		return nil, fmt.Errorf("%w: synthetic", connector.ErrStatementNotPublished)
	}
	return c.days(since, c.asOf), nil
}

func (c *periodicStatementConnector) GetBalance(ctx context.Context) (*connector.Balance, error) {
	if c.notPublished {
		return nil, fmt.Errorf("%w: synthetic", connector.ErrStatementNotPublished)
	}
	return c.statementConnector.GetBalance(ctx)
}

func (c *periodicStatementConnector) reconstructionStarts() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.since...)
}

// newPeriodicAccount is a synthetic account with a close every business day
// from first to asOf, funded on its first day.
func newPeriodicAccount(first, asOf time.Time) *periodicStatementConnector {
	acct := &statementConnector{asOf: asOf, closes: map[time.Time]float64{}}
	n := 0
	for d := first; !d.After(asOf); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		acct.closes[d] = 1000 + float64(n)
		n++
	}
	acct.flows = []*connector.Cashflow{{Amount: 1000, Currency: "USD", Timestamp: first}}
	return &periodicStatementConnector{statementConnector: acct}
}

func (h *dbHarness) connection(t *testing.T, exchange, label string) *repository.ExchangeConnection {
	t.Helper()
	conn, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, exchange, label)
	if err != nil {
		t.Fatalf("read connection: %v", err)
	}
	return conn
}

func withoutBackfillSpacing(t *testing.T) {
	saved := statementBackfillSpacing
	statementBackfillSpacing = 0
	t.Cleanup(func() { statementBackfillSpacing = saved })
}

// Every statement comes back cut after a week: the backfill follows each cut
// with a statement for the rest of its month, and writes every day.
func TestDBStatementBackfillFollowsCutStatements(t *testing.T) {
	withoutBackfillSpacing(t)
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	h.sync.SetFXSource(stubFX{})
	connMeta := h.connection(t, "ibkr", "main")

	_, to := statementBackfillRange(time.Now())
	first := to.AddDate(0, 0, -80)
	acct := newPeriodicAccount(first, to)
	acct.maxDays = 5

	h.sync.reconstructFromStatements(h.ctx, connMeta, acct, reconstructOpts{})

	rows, err := h.snaps.GetByUserAndDateRange(h.ctx, dbUser, first, to.Add(time.Hour))
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	stored := map[time.Time]float64{}
	for _, r := range rows {
		stored[r.Timestamp.UTC()] = r.TotalEquity
	}
	for day, eq := range acct.closes {
		if got, ok := stored[day]; !ok || got != eq {
			t.Fatalf("%s: stored %v (present %v), want %v", day.Format(time.DateOnly), got, ok, eq)
		}
	}
	if done, err := h.connSvc.HistoryBackfilled(h.ctx, connMeta.ID); err != nil || !done {
		t.Fatalf("backfill not recorded as done (%v, %v)", done, err)
	}
	for i, p := range acct.periods[1:] {
		if prev := acct.periods[i]; !p.from.After(prev.from) {
			t.Fatalf("statement %d starts %s, not after the previous %s", i+1, p.from.Format(time.DateOnly), prev.from.Format(time.DateOnly))
		}
	}
}

// waitBackfilled waits for the backfill a sync started in the background.
func (h *dbHarness) waitBackfilled(t *testing.T, exchange, label string) {
	t.Helper()
	connMeta := h.connection(t, exchange, label)
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if done, err := h.connSvc.HistoryBackfilled(h.ctx, connMeta.ID); err == nil && done {
			return
		}
	}
	t.Fatal("the backfill the sync started did not complete")
}

// Until the backfill has run, a sync writes the live row and no window: a
// window would open the history on its first day and book the inception
// deposit there. The sync starts the backfill, which clears the live row's
// stamp. Once it has run, the sync reconstructs its window, not the year.
func TestDBIBKRSyncReconstructsItsWindowOnlyOnceBackfilled(t *testing.T) {
	withoutBackfillSpacing(t)
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	h.sync.SetFXSource(stubFX{})
	asOf := lastExpectedStatementDate(startOfTodayUTC())
	acct := newPeriodicAccount(asOf.AddDate(0, 0, -60), asOf)
	h.plant(dbKey, dbSecret, acct)

	if res := syncPaths[0].run(h, "ibkr", "main"); res.Error != "" || res.Skipped {
		t.Fatalf("first sync: %+v", res)
	}
	if starts := acct.reconstructionStarts(); len(starts) != 0 {
		t.Fatalf("reconstructed before the backfill, from %v", starts)
	}
	if _, err := h.snaps.GetByUserExchangeLabelAndDate(h.ctx, dbUser, "ibkr", "main", asOf); err == nil {
		t.Fatal("the statement's last day was written before the backfill")
	}

	h.waitBackfilled(t, "ibkr", "main")
	if live := h.snapshotToday(t, "ibkr", "main"); live.Deposits != 0 {
		t.Fatalf("the connect-time inception stamp survived the backfill: deposits %v", live.Deposits)
	}

	if res := syncPaths[1].run(h, "ibkr", "main"); res.Error != "" || res.Skipped {
		t.Fatalf("sync after the backfill: %+v", res)
	}
	starts := acct.reconstructionStarts()
	if len(starts) != 1 {
		t.Fatalf("%d reconstructions after the backfill, want 1", len(starts))
	}
	if want := statementWindowFrom(time.Now(), startOfTodayUTC()); !starts[0].Equal(want) {
		t.Fatalf("window from %s, want %s", starts[0].Format(time.DateOnly), want.Format(time.DateOnly))
	}
	if _, err := h.snaps.GetByUserExchangeLabelAndDate(h.ctx, dbUser, "ibkr", "main", asOf); err != nil {
		t.Fatalf("the window did not write the statement's last day: %v", err)
	}
}

// At midnight UTC the business day that just closed is not out yet. The sync
// writes nothing and waits for it, as for a stale statement; it is not an error.
func TestDBIBKRUnpublishedStatementIsDeferred(t *testing.T) {
	for _, p := range syncPaths {
		t.Run(p.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
			var retries []time.Duration
			h.sync.deferAfter = func(d time.Duration, _ func()) { retries = append(retries, d) }
			connMeta := h.connection(t, "ibkr", "main")
			if err := h.conns.MarkRebuildFinalized(h.ctx, connMeta.ID, time.Now()); err != nil {
				t.Fatalf("mark backfilled: %v", err)
			}
			asOf := lastExpectedStatementDate(startOfTodayUTC())
			acct := newPeriodicAccount(asOf.AddDate(0, 0, -10), asOf)
			acct.notPublished = true
			h.plant(dbKey, dbSecret, acct)

			res := p.run(h, "ibkr", "main")
			if res.Error != "" || !res.Skipped || !strings.Contains(res.SkipReason, "not published") {
				t.Fatalf("result %+v, want a skip for the unpublished statement", res)
			}
			if len(retries) != 1 {
				t.Fatalf("%d retries armed, want 1", len(retries))
			}
			if st := h.status(t, "ibkr", "main"); st.Status != "pending" || !strings.HasPrefix(st.ErrorMessage, "skipped_stale:") {
				t.Fatalf("status %q %q, want pending skipped_stale", st.Status, st.ErrorMessage)
			}
			if _, err := h.snaps.GetByUserExchangeLabelAndDate(h.ctx, dbUser, "ibkr", "main", startOfTodayUTC()); err == nil {
				t.Fatal("a live row was written from a statement not published yet")
			}
		})
	}
}

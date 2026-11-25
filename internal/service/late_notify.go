package service

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/repository"
)

// Analytics learns of new snapshots in two ways: its nightly job
// (sync-to-neon, cron 15 0 * * *) copies every user's rows and recomputes
// their stats, and POST <historyNotifyURL>/<userUID> (notifyHistoryRebuilt)
// does the same for one user on demand. A row written after the nightly copy
// reaches neither unless its writer pings: the equity curve, read live, moves
// on while the stats in user_metrics stay a day behind. On 2026-11-25 an IBKR
// sync deferred at midnight by Flex rate limiting succeeded at 06:05, rewrote
// one day and added two, and the customer's shared report showed the curve at
// +8.89% against headline stats at -1.56% until the per-user sync was run by
// hand.

// analyticsNightlyRead is how long after 00:00 UTC the nightly copy starts. A
// row committed before it is in that copy; one committed after may not be. A
// minute short of the cron's 00:15, for the clocks of two hosts.
const analyticsNightlyRead = 14 * time.Minute

// lateWriteClock is when the scheduled pass's writes landed. A variable only
// so tests can place a pass before or after the nightly copy.
var lateWriteClock = time.Now

// lateForNightlyAnalytics reports whether a row committed at t can have missed
// the nightly copy of t's day.
func lateForNightlyAnalytics(t time.Time) bool {
	t = t.UTC()
	return t.Sub(t.Truncate(24*time.Hour)) >= analyticsNightlyRead
}

// wantsAnalyticsNotify reports whether a pass's results call for the ping: a
// row written, live or by the every-sync reconstruction, for a connection
// whose statement backfill is not starting behind it. That backfill pings when
// it ends (startStatementBackfill), or the release of its claim does when
// another read holds it (releaseBackfill). A ping before it would show the
// connection with a single live day while its year is read: analytics marks a
// connection that asked for no import historically complete on its first
// mirrored row.
func wantsAnalyticsNotify(results ...*SyncResult) bool {
	for _, r := range results {
		if r != nil && (r.Success || r.historyWritten) && !r.backfillDue {
			return true
		}
	}
	return false
}

// notifyAnalyticsAsync pings analytics for userUID off the caller's path. The
// endpoint answers once its per-user recompute is done, which takes seconds,
// and neither a manual sync nor the midnight pass should wait on it. Best
// effort like every ping: a failure is logged at warn by notifyHistoryRebuilt,
// the write it follows stands, and the nightly copy catches up.
func (s *SyncService) notifyAnalyticsAsync(userUID, reason string) {
	if s.historyNotifyURL == "" {
		return
	}
	s.logger.Info("snapshots changed outside the analytics nightly copy; notifying analytics",
		zap.String("user_uid", userUID),
		zap.String("reason", reason),
	)
	run := s.notifyRun
	if run == nil {
		run = func(f func()) { go f() }
	}
	run(func() { s.notifyHistoryRebuilt(context.Background(), userUID) })
}

// finishDeferredRetry frees the connection's retry slot and pings analytics
// when the retry wrote. Each retry pings for itself: the timers live in
// memory, so a ping held for a user's last retry was lost with any retry that
// a restart dropped, and two retries of one user fire minutes apart anyway
// (deferredRetrySpacing).
func (s *SyncService) finishDeferredRetry(conn *repository.ExchangeConnection, wrote bool) {
	s.deferMu.Lock()
	delete(s.deferredRetries, conn.ID)
	s.deferMu.Unlock()
	if wrote {
		s.notifyAnalyticsAsync(conn.UserUID, "deferred retry")
	}
}

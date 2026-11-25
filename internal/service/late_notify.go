package service

import (
	"context"
	"time"

	"go.uber.org/zap"
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
// it ends (startStatementBackfill), and a ping before it would publish the
// connection as imported with a single live day: analytics marks a connection
// historically complete on its first mirrored row.
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

// retryGroup gathers the deferred retries one pass armed for one user, so
// analytics hears once, after the last of them, instead of once per
// connection: two IBKR accounts on one Flex token are retried five minutes
// apart (deferredRetrySpacing). Guarded by SyncService.deferMu.
type retryGroup struct {
	userUID string
	pending int
	wrote   bool
}

type retryGroupKey struct{}

// withRetryGroup gives the deferred retries a pass arms for userUID one group.
func withRetryGroup(ctx context.Context, userUID string) context.Context {
	return context.WithValue(ctx, retryGroupKey{}, &retryGroup{userUID: userUID})
}

// retryGroupFor is the pass's group for a retry of userUID's connection, or a
// group of its own when the retry was armed outside a pass.
func retryGroupFor(ctx context.Context, userUID string) *retryGroup {
	if g, ok := ctx.Value(retryGroupKey{}).(*retryGroup); ok && g.userUID == userUID {
		return g
	}
	return &retryGroup{userUID: userUID}
}

// finishDeferredRetry frees the connection's retry slot and, when it is the
// last retry of its group to end and one of them wrote, pings analytics.
func (s *SyncService) finishDeferredRetry(connID string, g *retryGroup, wrote bool) {
	s.deferMu.Lock()
	delete(s.deferredRetries, connID)
	g.pending--
	g.wrote = g.wrote || wrote
	ping := g.pending == 0 && g.wrote
	if g.pending == 0 {
		g.wrote = false
	}
	s.deferMu.Unlock()
	if ping {
		s.notifyAnalyticsAsync(g.userUID, "deferred retry")
	}
}

package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// A statement broker's daily sync reads a short window (IBKRStatementWindowDays)
// and its year of history once, here, a month per statement. One statement for
// the year was what the daily sync used to ask for: for an active account
// broken out by day it took under a minute one day and more than ten the next,
// and every timeout cost the day.
const statementBackfillPeriodDays = 31

// statementBackfillMaxReads bounds the statements of one backfill. A year is
// twelve; a statement IBKR cuts short under load is followed by one for the
// rest of its month, and at seven days a statement that is about fifty.
const statementBackfillMaxReads = 80

// statementBackfillTimeout bounds one backfill: up to statementBackfillMaxReads
// statements and the spacing between them.
const statementBackfillTimeout = 2 * time.Hour

// statementBackfillSpacing paces a backfill's statements like a deep
// reconstruction's. A variable only so tests can run a backfill without
// waiting it out.
var statementBackfillSpacing = deepHistoryPeriodSpacing

// statementBackfillMaxWait is the longest a backfill waits for a token another
// read holds. Longer means the token is backing off failures, and the next sync
// is the better time to ask again.
const statementBackfillMaxWait = 15 * time.Minute

// statementWindowStart is where an IBKR sync's statement starts
// (statementWindowFrom), from the connection's newest stored day.
func (s *SyncService) statementWindowStart(ctx context.Context, connMeta *repository.ExchangeConnection, now time.Time) time.Time {
	var newest time.Time
	if s.snapshotRepo != nil {
		if latest, err := s.snapshotRepo.GetLatestByUserExchangeLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label); err == nil && latest != nil {
			newest = latest.Timestamp
		}
	}
	return statementWindowFrom(now, newest)
}

// statementWindowFrom is the daily window, reaching back past the newest
// stored day after an outage, or after a statement IBKR cut short, so the days
// it missed are written too. Three days before it, because a live row holds
// the business day before its date, a weekend before that. The connector caps
// it at a year. A zero newest means nothing is stored.
func statementWindowFrom(now, newest time.Time) time.Time {
	start := connector.LastStatementDay(now).AddDate(0, 0, -(connector.IBKRStatementWindowDays - 1))
	if newest.IsZero() {
		return start
	}
	if resume := truncDay(newest).AddDate(0, 0, -3); resume.Before(start) {
		return resume
	}
	return start
}

// statementBackfillRange is the year a backfill reads, ending the business day
// before the last one: that day's statement is out at any hour, the last one's
// only after IBKR's overnight run. The daily window writes the days after it.
func statementBackfillRange(now time.Time) (from, to time.Time) {
	last := connector.LastStatementDay(now)
	return last.AddDate(0, 0, -(connector.IBKRStatementMaxDays - 1)), connector.LastStatementDay(last)
}

// historyBackfilled reports whether the connection's year of statements has
// been read. Until it has, the daily sync writes no reconstruction: its window
// would open the history on a day that is not the account's first, and the
// inception deposit would land there. A failed read keeps the daily window.
func (s *SyncService) historyBackfilled(ctx context.Context, connMeta *repository.ExchangeConnection) bool {
	done, err := s.connSvc.HistoryBackfilled(ctx, connMeta.ID)
	if err != nil {
		s.logger.Warn("statement backfill state not read; keeping the daily window",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Error(err),
		)
		return true
	}
	return done
}

// claimBackfill marks a connection's backfill as running, or reports that one
// already is.
func (s *SyncService) claimBackfill(connMeta *repository.ExchangeConnection) bool {
	key := connMeta.UserUID + "|" + strings.ToLower(connMeta.Exchange) + "|" + connMeta.Label
	s.backfillMu.Lock()
	defer s.backfillMu.Unlock()
	if s.backfills == nil {
		s.backfills = make(map[string]bool)
	}
	if s.backfills[key] {
		return false
	}
	s.backfills[key] = true
	return true
}

func (s *SyncService) releaseBackfill(connMeta *repository.ExchangeConnection) {
	key := connMeta.UserUID + "|" + strings.ToLower(connMeta.Exchange) + "|" + connMeta.Label
	s.backfillMu.Lock()
	delete(s.backfills, key)
	s.backfillMu.Unlock()
}

// startStatementBackfill reads the connection's year in the background. It
// shares the cached connector with the daily syncs: its period reads write
// nothing on it.
func (s *SyncService) startStatementBackfill(connMeta *repository.ExchangeConnection) {
	if !s.claimBackfill(connMeta) {
		return
	}
	go func() {
		defer s.releaseBackfill(connMeta)
		ctx, cancel := context.WithTimeout(context.Background(), statementBackfillTimeout)
		defer cancel()
		creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label)
		if err != nil {
			s.logger.Error("statement backfill: credential decrypt failed",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.Error(err),
			)
			return
		}
		conn, err := s.getOrCreateConnector(connMeta.Exchange, connMeta.UserUID, connMeta.Label, creds)
		creds = nil
		if err != nil {
			s.logger.Error("statement backfill: connector create failed",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.Error(err),
			)
			return
		}
		provider, ok := conn.(connector.PeriodHistoryProvider)
		if !ok {
			return
		}
		from, to := statementBackfillRange(time.Now())
		if err := s.backfillStatements(ctx, connMeta, provider, from, to, reconstructOpts{}); err != nil {
			s.logger.Error("statement backfill failed",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.Error(err),
				zap.String("hint", "retried by the connection's next sync"),
			)
			return
		}
		s.notifyHistoryRebuilt(ctx, connMeta.UserUID)
	}()
}

// backfillStatements reads [from, to] a month per statement, oldest first, and
// writes the series once all of it has been read. A statement IBKR cut short
// is followed by one for the rest of its month: the cut keeps the oldest days
// and says nothing, and a month missing its end is a hole the TWR walks
// straight through. A month with no statement, or no day, before the account's
// first is skipped. Any failure writes nothing. A full run, neither dry nor
// windowed, records the backfill as done. The caller holds claimBackfill.
func (s *SyncService) backfillStatements(ctx context.Context, connMeta *repository.ExchangeConnection, provider connector.PeriodHistoryProvider, from, to time.Time, opts reconstructOpts) error {
	var read [][]*connector.HistoricalSnapshot
	reads := 0
	for cursor := from; !cursor.After(to); reads++ {
		if reads == statementBackfillMaxReads {
			return fmt.Errorf("history not read to %s in %d statements", to.Format("2006-01-02"), reads)
		}
		if reads > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(statementBackfillSpacing):
			}
		}
		end := cursor.AddDate(0, 0, statementBackfillPeriodDays-1)
		if end.After(to) {
			end = to
		}
		days, err := s.readStatementPeriod(ctx, provider, dayWindow{from: cursor, to: end})
		if err != nil && !errors.Is(err, connector.ErrStatementUnavailable) {
			return fmt.Errorf("read statement %s..%s: %w", cursor.Format("2006-01-02"), end.Format("2006-01-02"), err)
		}
		if len(days) == 0 {
			if len(read) > 0 {
				// A holiday closing a period, or a hole worth a look.
				s.logger.Info("statement backfill: a period inside the history has no day",
					zap.String("user_uid", connMeta.UserUID),
					zap.String("exchange", connMeta.Exchange),
					zap.String("label", connMeta.Label),
					zap.Time("period_from", cursor),
					zap.Time("period_to", end),
				)
			}
			cursor = end.AddDate(0, 0, 1)
			continue
		}
		read = append(read, days)
		cursor = nextBackfillCursor(days, end)
	}

	series, err := s.convertHistoryToUSD(ctx, connMeta, mergePeriodSnapshots(read))
	if err != nil {
		return fmt.Errorf("convert backfill to USD: %w", err)
	}
	if err := s.persistHistoricalSnapshots(ctx, connMeta, series, s.isFirstSync(ctx, connMeta), sourceInEnclave, opts); err != nil {
		return err
	}
	if opts.dryRun || opts.window.isSet() {
		return nil
	}
	if err := s.connSvc.MarkRebuildFinalized(ctx, connMeta.ID, time.Now().UTC()); err != nil {
		return fmt.Errorf("record the backfill: %w", err)
	}
	s.logger.Info("statement backfill completed",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
		zap.Int("days", len(series)),
		zap.Int("statements", reads),
	)
	return nil
}

// nextBackfillCursor is where the next statement starts: after the period when
// its days reach the period's last business day, after the last day read when
// IBKR cut the statement short. A holiday at the end reads as a cut, and costs
// one more statement that comes back empty.
func nextBackfillCursor(days []*connector.HistoricalSnapshot, end time.Time) time.Time {
	last := days[0].Date
	for _, d := range days[1:] {
		if d.Date.After(last) {
			last = d.Date
		}
	}
	lastBusinessDay := connector.LastStatementDay(end.AddDate(0, 0, 1))
	if last.Before(lastBusinessDay) {
		return truncDay(last).AddDate(0, 0, 1)
	}
	return end.AddDate(0, 0, 1)
}

// readStatementPeriod reads one period, waiting for the token when another
// read holds it: two accounts on one token backfill side by side, and the
// daily reads go on meanwhile.
func (s *SyncService) readStatementPeriod(ctx context.Context, provider connector.PeriodHistoryProvider, p dayWindow) ([]*connector.HistoricalSnapshot, error) {
	for attempt := 0; ; attempt++ {
		days, err := provider.GetHistoricalSnapshotsForPeriod(ctx, p.from, p.to)
		retryAt, busy := connector.FlexRetryAt(err)
		if !busy || attempt >= 5 {
			return days, err
		}
		wait := time.Until(retryAt) + time.Second
		if wait > statementBackfillMaxWait {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

// reconstructFromStatements is the connect-time and admin reconstruction of a
// statement broker: the full backfill, or only the days of opts.window.
func (s *SyncService) reconstructFromStatements(ctx context.Context, connMeta *repository.ExchangeConnection, provider connector.PeriodHistoryProvider, opts reconstructOpts) {
	if !s.claimBackfill(connMeta) {
		s.logger.Info("statement backfill already running, reconstruction skipped",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
		)
		return
	}
	defer s.releaseBackfill(connMeta)

	from, to := statementBackfillRange(time.Now())
	if opts.window.isSet() {
		from, to = opts.window.from, opts.window.to
	}
	if err := s.backfillStatements(ctx, connMeta, provider, from, to, opts); err != nil {
		s.logger.Error("statement reconstruction failed",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Error(err),
		)
		return
	}
	s.notifyHistoryRebuilt(ctx, connMeta.UserUID)
}

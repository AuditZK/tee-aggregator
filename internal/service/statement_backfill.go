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

// statementBackfillMaxReads bounds the statements of one backfill per year it
// reads. A year is twelve; a statement IBKR cuts short under load is followed
// by one for the rest of its month, and at seven days a statement that is
// about fifty.
const statementBackfillMaxReads = 80

// statementBackfillMinDays is the shortest period a month is split down to
// when IBKR is still generating its statement at the end of the poll: about
// the week IBKR itself cuts a statement to under load.
const statementBackfillMinDays = 8

// statementBackfillWait is how long a backfill waits for one statement. The
// syncs give up after connector.FlexPollBudget because a day depends on them;
// nothing waits on a backfill, and giving up that early failed it for good on
// a week IBKR needed longer to generate.
const statementBackfillWait = 30 * time.Minute

// statementBackfillTimeout bounds one backfill's year: up to
// statementBackfillMaxReads statements and the spacing between them.
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

// backfillClaim is a running backfill of one connection, or another read that
// holds its claim (a deep or admin reconstruction, a dry run of either).
type backfillClaim struct {
	// pingOwed is set by a sync that found the claim held when it went to
	// start the backfill: it wrote a live row and left its analytics ping to
	// the backfill (wantsAnalyticsNotify), which this holder may not be.
	pingOwed bool
}

func backfillKey(connMeta *repository.ExchangeConnection) string {
	return connMeta.UserUID + "|" + strings.ToLower(connMeta.Exchange) + "|" + connMeta.Label
}

// claimBackfill marks a connection's backfill as running, or reports that one
// already is.
func (s *SyncService) claimBackfill(connMeta *repository.ExchangeConnection) bool {
	return s.claimBackfillOrOwe(connMeta, false)
}

// claimBackfillOrOwe claims the connection's backfill; when the claim is held,
// owe marks it as owing the caller's analytics ping, in the same critical
// section, so the holder cannot release in between.
func (s *SyncService) claimBackfillOrOwe(connMeta *repository.ExchangeConnection, owe bool) bool {
	key := backfillKey(connMeta)
	s.backfillMu.Lock()
	defer s.backfillMu.Unlock()
	if s.backfills == nil {
		s.backfills = make(map[string]*backfillClaim)
	}
	if held := s.backfills[key]; held != nil {
		held.pingOwed = held.pingOwed || owe
		return false
	}
	s.backfills[key] = &backfillClaim{}
	return true
}

// releaseBackfill frees the claim and sends the analytics ping a sync left to
// it, if one did. Every sync that saw the claim held wrote before this.
func (s *SyncService) releaseBackfill(connMeta *repository.ExchangeConnection) {
	if s.freeBackfill(connMeta) {
		s.notifyAnalyticsAsync(connMeta.UserUID, "a sync's ping left to a backfill claim")
	}
}

// freeBackfill frees the claim and reports whether a sync left its analytics
// ping to it.
func (s *SyncService) freeBackfill(connMeta *repository.ExchangeConnection) (pingOwed bool) {
	key := backfillKey(connMeta)
	s.backfillMu.Lock()
	defer s.backfillMu.Unlock()
	if held := s.backfills[key]; held != nil {
		pingOwed = held.pingOwed
	}
	delete(s.backfills, key)
	return pingOwed
}

// startBackfillIfDue starts the statement backfill a sync found due, once the
// sync has saved its live row: the backfill clears that row's inception stamp,
// and cannot clear one not written yet. After the live read, too, whose token
// the backfill's first statement would otherwise take.
func (s *SyncService) startBackfillIfDue(connMeta *repository.ExchangeConnection, result *SyncResult) {
	if result != nil && result.backfillDue {
		s.startStatementBackfill(connMeta)
	}
}

// startStatementBackfill reads the connection's year in the background. It
// shares the cached connector with the daily syncs: its period reads write
// nothing on it.
//
// It pings analytics however it ends: the sync that started it, and any sync
// that found it running, left their own ping to it (wantsAnalyticsNotify), and
// a failed year still leaves their live rows to mirror. The ping follows the
// release of the claim, so every sync that saw the claim held wrote before it.
// When another read holds the claim, the sync's ping is owed to that read
// instead (releaseBackfill): a dry run or a failed deep reconstruction would
// not send one of its own.
func (s *SyncService) startStatementBackfill(connMeta *repository.ExchangeConnection) {
	if !s.claimBackfillOrOwe(connMeta, true) {
		return
	}
	go func() {
		defer func() {
			// This one pings whatever a sync left owed: freeBackfill, not
			// releaseBackfill, so the ping goes out once.
			s.freeBackfill(connMeta)
			s.notifyHistoryRebuilt(context.Background(), connMeta.UserUID)
		}()
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
		}
	}()
}

// backfillStatements reads [from, to] a month per statement, newest first, and
// writes the series once all of it has been read. It stops at the first month
// without a statement, the one before the account's first: IBKR answers it with
// 1003, a failure that doubles the token's back-off, and one per month the
// account did not exist would fail the backfill of any account younger than the
// year. A statement IBKR cut short is followed by one for the rest of its
// month: the cut keeps the oldest days and says nothing, and a month missing its
// end is a hole the TWR walks straight through. Any failure writes nothing. A
// full run, neither dry nor windowed, records the backfill as done. The caller
// holds claimBackfill.
func (s *SyncService) backfillStatements(ctx context.Context, connMeta *repository.ExchangeConnection, provider connector.PeriodHistoryProvider, from, to time.Time, opts reconstructOpts) error {
	r := &statementReader{s: s, provider: provider, maxReads: statementBackfillMaxReads * historyYears(from, to)}
	var read [][]*connector.HistoricalSnapshot
	for end := to; !end.Before(from); {
		m := dayWindow{from: end.AddDate(0, 0, -(statementBackfillPeriodDays - 1)), to: end}
		if m.from.Before(from) {
			m.from = from
		}
		month, err := r.month(ctx, m)
		if err != nil {
			return err
		}
		if len(month) == 0 {
			if err := s.historyStartsAfter(ctx, connMeta, m, len(read) == 0); err != nil {
				return err
			}
			break
		}
		read = append(month, read...)
		end = m.from.AddDate(0, 0, -1)
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
		zap.Int("statements", r.reads),
	)
	return nil
}

// historyStartsAfter reads a month without a statement as the account's start,
// unless days on or before it are stored: then it is a hole IBKR left, and the
// backfill fails rather than write around it. The newest month without one is
// an account with no statement yet, which the next sync asks about again.
func (s *SyncService) historyStartsAfter(ctx context.Context, connMeta *repository.ExchangeConnection, m dayWindow, newest bool) error {
	if newest {
		return fmt.Errorf("no statement for %s..%s, the newest month", m.from.Format(time.DateOnly), m.to.Format(time.DateOnly))
	}
	earliest, err := s.snapshotRepo.GetEarliestTimestamp(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label)
	switch {
	case errors.Is(err, repository.ErrNotFound):
	case err != nil:
		return fmt.Errorf("read earliest stored day: %w", err)
	case !earliest.After(m.to):
		return fmt.Errorf("no statement for %s..%s, yet days from %s are stored", m.from.Format(time.DateOnly), m.to.Format(time.DateOnly), earliest.Format(time.DateOnly))
	}
	s.logger.Info("statement backfill reached the account's first statement",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
		zap.Time("no_statement_to", m.to),
	)
	return nil
}

// statementReader reads a backfill's statements, spaced and bounded.
type statementReader struct {
	s        *SyncService
	provider connector.PeriodHistoryProvider
	maxReads int
	reads    int
}

// month reads one month, following a statement IBKR cut short with one for the
// rest of the month. A statement IBKR is still generating when the poll gives
// up is asked again for half its period, down to statementBackfillMinDays: a
// month heavy enough to outlast the poll did so on every attempt, and failed
// the whole backfill each time. Nothing back means the month has no statement.
func (r *statementReader) month(ctx context.Context, m dayWindow) ([][]*connector.HistoricalSnapshot, error) {
	var out [][]*connector.HistoricalSnapshot
	span := statementBackfillPeriodDays
	for cursor := m.from; !cursor.After(m.to); {
		end := cursor.AddDate(0, 0, span-1)
		if end.After(m.to) {
			end = m.to
		}
		days, err := r.read(ctx, dayWindow{from: cursor, to: end})
		if asked := daysInWindow(cursor, end); errors.Is(err, connector.ErrStatementStillGenerating) && asked > statementBackfillMinDays {
			span = max((asked+1)/2, statementBackfillMinDays)
			r.s.logger.Info("statement slow to generate, month read in shorter periods",
				zap.Time("period_from", cursor),
				zap.Time("period_to", end),
				zap.Int("next_period_days", span),
			)
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(days) == 0 {
			if len(out) > 0 && !holidayTail(cursor, m.to) {
				return nil, fmt.Errorf("statement %s..%s has no day, after the first days of its month", cursor.Format(time.DateOnly), end.Format(time.DateOnly))
			}
			// A shortened period before the account's first day says nothing
			// of the rest of its month.
			if len(out) == 0 && end.Before(m.to) {
				cursor = end.AddDate(0, 0, 1)
				continue
			}
			return out, nil
		}
		out = append(out, days)
		cursor = nextBackfillCursor(days, end)
	}
	return out, nil
}

func daysInWindow(from, to time.Time) int {
	return int(to.Sub(from).Hours()/24) + 1
}

func (r *statementReader) read(ctx context.Context, p dayWindow) ([]*connector.HistoricalSnapshot, error) {
	if r.reads == r.maxReads {
		return nil, fmt.Errorf("history not read in %d statements", r.reads)
	}
	if r.reads > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(statementBackfillSpacing):
		}
	}
	r.reads++
	days, err := r.s.readStatementPeriod(ctx, r.provider, p)
	if err != nil && !errors.Is(err, connector.ErrStatementUnavailable) {
		return nil, fmt.Errorf("read statement %s..%s: %w", p.from.Format(time.DateOnly), p.to.Format(time.DateOnly), err)
	}
	return days, nil
}

// statementHolidayTailDays is how short a month's rest may be and still come
// back empty: a statement stopping on the eve of a holiday IBKR wrote no day
// for reads as a cut, and the read for the rest finds nothing. Longer, it is
// days IBKR has and did not serve.
const statementHolidayTailDays = 3

func holidayTail(cursor, end time.Time) bool {
	return cursor.AddDate(0, 0, statementHolidayTailDays).After(end)
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
		days, err := provider.GetHistoricalSnapshotsForPeriod(connector.WithFlexPollBudget(ctx, statementBackfillWait), p.from, p.to)
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
// statement broker: the full backfill, or only the days of opts.window. Its
// caller pings analytics when it returns (reconstructHistoryFor), skipped or
// not.
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
	} else {
		// The caller's deadline is sized for a rebuilder call: a year read at
		// statementBackfillWait a statement gets the backfill's own.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), statementBackfillTimeout)
		defer cancel()
	}
	if err := s.backfillStatements(ctx, connMeta, provider, from, to, opts); err != nil {
		s.logger.Error("statement reconstruction failed",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Error(err),
		)
	}
}

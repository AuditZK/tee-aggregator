package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// deepHistoryPeriodSpacing paces the statements of one deep reconstruction.
// It clears the per-token cooldown claimFlexToken enforces after a success, so
// no period is refused locally halfway through.
const deepHistoryPeriodSpacing = 75 * time.Second

// ErrDeepHistoryUnsupported is returned for a connector that cannot read a
// chosen period.
var ErrDeepHistoryUnsupported = errors.New("connector cannot read history for a chosen period")

// DeepHistoryTimeout bounds a deep reconstruction back to from: a backfill's
// time for each year it reads.
func DeepHistoryTimeout(from, now time.Time) time.Duration {
	return time.Duration(historyYears(from, now)) * statementBackfillTimeout
}

// historyYears is the number of years [from, to] spans, rounded up.
func historyYears(from, to time.Time) int {
	days := int(to.Sub(from).Hours()/24) + 1
	if days <= connector.IBKRStatementMaxDays {
		return 1
	}
	return (days + connector.IBKRStatementMaxDays - 1) / connector.IBKRStatementMaxDays
}

// mergePeriodSnapshots joins the periods' days into one series, oldest first.
// A date two periods both return keeps the earlier period's row: that period
// read the date's flows, the later one starts after them.
func mergePeriodSnapshots(periods [][]*connector.HistoricalSnapshot) []*connector.HistoricalSnapshot {
	seen := make(map[time.Time]struct{})
	var out []*connector.HistoricalSnapshot
	for _, days := range periods {
		for _, d := range days {
			if _, dup := seen[d.Date]; dup {
				continue
			}
			seen[d.Date] = struct{}{}
			out = append(out, d)
		}
	}
	return out
}

// ReconstructDeepHistory extends a connection's history back to from, up to
// and including the earliest stored day. That overlapping day is what clears
// the inception deposit the stored history was stamped with: left alone, it
// would read as the whole balance arriving mid-series.
func (s *SyncService) ReconstructDeepHistory(ctx context.Context, userUID, exchange, label string, from time.Time) error {
	if err := s.reconstructDeepHistory(ctx, userUID, exchange, label, from, reconstructOpts{}); err != nil {
		return err
	}
	s.notifyHistoryRebuilt(ctx, userUID)
	return nil
}

func (s *SyncService) reconstructDeepHistory(ctx context.Context, userUID, exchange, label string, from time.Time, opts reconstructOpts) error {
	connMeta, err := s.connSvc.GetActiveConnectionByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return fmt.Errorf("look up connection: %w", err)
	}
	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return fmt.Errorf("decrypt credentials: %w", err)
	}
	conn, err := s.getOrCreateConnector(connMeta.Exchange, connMeta.UserUID, label, creds)
	if err != nil {
		return fmt.Errorf("build connector: %w", err)
	}
	provider, ok := conn.(connector.PeriodHistoryProvider)
	if !ok {
		return fmt.Errorf("%s: %w", exchange, ErrDeepHistoryUnsupported)
	}
	return s.deepHistoryFrom(ctx, connMeta, provider, from, opts)
}

// deepHistoryFrom reads [from, earliest stored day] the way the statement
// backfill reads its year: a month per statement, newest first, following each
// statement IBKR cuts short, stopping at the first month without one, which is
// the account's first or the oldest IBKR still serves. A year-long statement
// broken out by day comes back cut under load with no error, and the days it
// drops would be a hole the TWR walks straight through. Any failure writes
// nothing.
func (s *SyncService) deepHistoryFrom(ctx context.Context, connMeta *repository.ExchangeConnection, provider connector.PeriodHistoryProvider, from time.Time, opts reconstructOpts) error {
	from = time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
	to, err := s.deepHistoryEnd(ctx, connMeta)
	if err != nil {
		return err
	}
	if !from.Before(to) {
		return fmt.Errorf("stored history already starts on %s", to.Format("2006-01-02"))
	}
	if !s.claimBackfill(connMeta) {
		return errors.New("a statement backfill is already running for this connection")
	}
	defer s.releaseBackfill(connMeta)
	return s.backfillStatements(ctx, connMeta, provider, from, to, opts)
}

// deepHistoryEnd is the last day a deep reconstruction reads: the earliest
// stored day, or yesterday for a connection with no history yet.
func (s *SyncService) deepHistoryEnd(ctx context.Context, connMeta *repository.ExchangeConnection) (time.Time, error) {
	earliest, err := s.snapshotRepo.GetEarliestTimestamp(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label)
	switch {
	case errors.Is(err, repository.ErrNotFound):
		now := time.Now().UTC()
		return time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC), nil
	case err != nil:
		return time.Time{}, fmt.Errorf("read earliest stored day: %w", err)
	}
	return time.Date(earliest.Year(), earliest.Month(), earliest.Day(), 0, 0, 0, 0, time.UTC), nil
}

// DryRunDeepHistory runs ReconstructDeepHistory without writing and hands back
// the days it would have persisted.
func (s *SyncService) DryRunDeepHistory(ctx context.Context, userUID, exchange, label string, from time.Time) ([]*repository.Snapshot, error) {
	var out []*repository.Snapshot
	err := s.reconstructDeepHistory(ctx, userUID, exchange, label, from, reconstructOpts{
		dryRun:  true,
		collect: func(sn []*repository.Snapshot) { out = sn },
	})
	return out, err
}

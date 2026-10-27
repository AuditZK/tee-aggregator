package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// statementPeriodDays is IBKR's ceiling on one statement's period.
const statementPeriodDays = connector.IBKRStatementMaxDays

// deepHistoryPeriodSpacing paces the statements of one deep reconstruction.
// It clears the per-token cooldown claimFlexToken enforces after a success, so
// no period is refused locally halfway through.
const deepHistoryPeriodSpacing = 75 * time.Second

// ErrDeepHistoryUnsupported is returned for a connector that cannot read a
// chosen period.
var ErrDeepHistoryUnsupported = errors.New("connector cannot read history for a chosen period")

// statementPeriods splits [from, to] into consecutive periods of at most
// statementPeriodDays days, oldest first.
func statementPeriods(from, to time.Time) []dayWindow {
	return periodsOf(from, to, statementPeriodDays)
}

// periodsOf splits [from, to] into consecutive periods of at most days days,
// oldest first.
func periodsOf(from, to time.Time, days int) []dayWindow {
	var out []dayWindow
	for start := from; !start.After(to); {
		end := start.AddDate(0, 0, days-1)
		if end.After(to) {
			end = to
		}
		out = append(out, dayWindow{from: start, to: end})
		start = end.AddDate(0, 0, 1)
	}
	return out
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

// ReconstructDeepHistory extends a connection's history back to from, reading
// one statement per period up to and including the earliest stored day. That
// overlapping day is what clears the inception deposit the stored history was
// stamped with: left alone, it would read as the whole balance arriving
// mid-series. Every period must succeed or nothing is written, because a
// missing year inside a series is a hole the TWR walks straight through.
func (s *SyncService) ReconstructDeepHistory(ctx context.Context, userUID, exchange, label string, from time.Time) error {
	return s.reconstructDeepHistory(ctx, userUID, exchange, label, from, reconstructOpts{})
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

	from = time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
	to, err := s.deepHistoryEnd(ctx, connMeta)
	if err != nil {
		return err
	}
	if !from.Before(to) {
		return fmt.Errorf("stored history already starts on %s", to.Format("2006-01-02"))
	}

	periods := statementPeriods(from, to)
	read := make([][]*connector.HistoricalSnapshot, 0, len(periods))
	for n, p := range periods {
		if n > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(deepHistoryPeriodSpacing):
			}
		}
		days, err := provider.GetHistoricalSnapshotsForPeriod(ctx, p.from, p.to)
		if err != nil {
			return fmt.Errorf("read statement %s..%s: %w", p.from.Format("2006-01-02"), p.to.Format("2006-01-02"), err)
		}
		s.logger.Info("deep history period read",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Time("period_from", p.from),
			zap.Time("period_to", p.to),
			zap.Int("days", len(days)),
		)
		read = append(read, days)
	}

	series, err := s.convertHistoryToUSD(ctx, connMeta, mergePeriodSnapshots(read))
	if err != nil {
		return fmt.Errorf("convert deep history to USD: %w", err)
	}
	return s.persistHistoricalSnapshots(ctx, connMeta, series, false, sourceInEnclave, opts)
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

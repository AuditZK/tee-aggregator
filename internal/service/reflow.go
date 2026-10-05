package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// maxReflowWindow bounds how far back a reflow reaches: the venues' ledgers
// keep about three months, and older days could only be emptied, not fixed.
const maxReflowWindow = 90 * 24 * time.Hour

// ReflowDay is one live snapshot's flows, as stored and as the connector
// classifies them today.
type ReflowDay struct {
	Day            time.Time `json:"day"`
	OldDeposits    float64   `json:"old_deposits"`
	OldWithdrawals float64   `json:"old_withdrawals"`
	NewDeposits    float64   `json:"new_deposits"`
	NewWithdrawals float64   `json:"new_withdrawals"`
	// Kept names why the row was left as stored; empty otherwise.
	Kept    string `json:"kept,omitempty"`
	Written bool   `json:"written,omitempty"`
}

func (d ReflowDay) changed() bool {
	return math.Abs(d.NewDeposits-d.OldDeposits) > 1e-9 || math.Abs(d.NewWithdrawals-d.OldWithdrawals) > 1e-9
}

// ReflowCashflows re-derives the deposits and withdrawals of a connection's
// live snapshots between from and to (snapshot days, inclusive) from the
// connector's current classification, after a change in how flows are
// classified left earlier days booked under the old rule. Equity is never
// touched. Each day's window is the one its sync used: the 24 hours before
// it, or back to the previous snapshot after a gap. With apply false nothing
// is written.
//
// The classification follows the wallets the key reads today. A day on which
// the key read other wallets (a permission added or removed since) is
// reclassified as if it read today's: read the dry run before applying.
func (s *SyncService) ReflowCashflows(ctx context.Context, userUID, exchange, label string, from, to time.Time, apply bool) ([]ReflowDay, error) {
	from, to = from.UTC().Truncate(24*time.Hour), to.UTC().Truncate(24*time.Hour)
	if err := checkReflowWindow(from, to); err != nil {
		return nil, err
	}
	connMeta, fetcher, accountCurrency, err := s.reflowSource(ctx, userUID, exchange, label)
	if err != nil {
		return nil, err
	}
	rows, earliest, err := s.reflowRows(ctx, connMeta, from, to)
	if err != nil {
		return nil, err
	}
	flows, err := fetcher.GetCashflows(ctx, reflowSince(rows, from))
	if err != nil {
		return nil, fmt.Errorf("read cashflows: %w", err)
	}
	// The rows hold USD; flows of an account held in EUR must be too.
	if flows, err = s.flowsInUSD(ctx, connMeta, accountCurrency, flows); err != nil {
		return nil, err
	}

	out, written, err := s.reflowDays(ctx, rows, from, to, earliest, flows, apply)
	if err != nil {
		return out, err
	}

	s.logger.Info("cashflow reflow",
		zap.String("user_uid", userUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
		zap.Bool("apply", apply),
		zap.Int("days", len(out)),
		zap.Int("written", written),
	)
	if written > 0 {
		s.notifyHistoryRebuilt(ctx, userUID)
	}
	return out, nil
}

// reflowDays classifies every row between from and to and, when apply is
// set, writes the ones whose flows changed. Returns the days and how many
// rows were written.
func (s *SyncService) reflowDays(ctx context.Context, rows []*repository.Snapshot, from, to, earliest time.Time, flows []*connector.Cashflow, apply bool) ([]ReflowDay, int, error) {
	var out []ReflowDay
	written := 0
	for i, r := range rows {
		if day := r.Timestamp.UTC(); day.Before(from) || day.After(to) {
			continue
		}
		d := reflowDay(rows, i, earliest, flows)
		if apply && d.Kept == "" && d.changed() {
			ok, err := s.snapshotRepo.UpdateFlows(ctx, r.ID, r.Deposits, r.Withdrawals, d.NewDeposits, d.NewWithdrawals)
			if err != nil {
				return out, written, err
			}
			d.Written = ok
			if ok {
				written++
			}
		}
		out = append(out, d)
	}
	return out, written, nil
}

func checkReflowWindow(from, to time.Time) error {
	if to.Before(from) {
		return errors.New("reflow: to precedes from")
	}
	if to.Sub(from) > maxReflowWindow {
		return fmt.Errorf("reflow: window longer than %d days", int(maxReflowWindow/(24*time.Hour)))
	}
	return nil
}

// reflowSource returns the connection, its cashflow reader and the currency
// the account is held in, after a balance read: the classification depends
// on which wallets the key reads, which only that read establishes.
func (s *SyncService) reflowSource(ctx context.Context, userUID, exchange, label string) (*repository.ExchangeConnection, connector.CashflowFetcher, string, error) {
	connMeta, err := s.connSvc.GetActiveConnectionByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return nil, nil, "", fmt.Errorf("look up connection: %w", err)
	}
	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, userUID, connMeta.Exchange, connMeta.Label)
	if err != nil {
		return nil, nil, "", fmt.Errorf("decrypt credentials: %w", err)
	}
	conn, err := s.getOrCreateConnector(connMeta.Exchange, userUID, connMeta.Label, creds)
	if err != nil {
		return nil, nil, "", fmt.Errorf("build connector: %w", err)
	}
	fetcher, ok := conn.(connector.CashflowFetcher)
	if !ok {
		return nil, nil, "", fmt.Errorf("reflow: %s reports no cashflows", connMeta.Exchange)
	}
	balance, err := conn.GetBalance(ctx)
	if err != nil {
		return nil, nil, "", fmt.Errorf("read balance: %w", err)
	}
	return connMeta, fetcher, balance.Currency, nil
}

// reflowRows returns the connection's snapshots from far enough before from
// to know each day's previous snapshot, oldest first, and the connection's
// earliest snapshot day.
func (s *SyncService) reflowRows(ctx context.Context, connMeta *repository.ExchangeConnection, from, to time.Time) ([]*repository.Snapshot, time.Time, error) {
	all, err := s.snapshotRepo.GetByUserAndDateRange(ctx, connMeta.UserUID, from.Add(-maxActivityLookback), to)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("read snapshots: %w", err)
	}
	rows := make([]*repository.Snapshot, 0, len(all))
	for _, r := range all {
		if r.Exchange == connMeta.Exchange && r.Label == connMeta.Label {
			rows = append(rows, r)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Timestamp.Before(rows[j].Timestamp) })
	earliest, err := s.snapshotRepo.GetEarliestTimestamp(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("read earliest snapshot: %w", err)
	}
	return rows, earliest.UTC(), nil
}

// reflowWindowStart is the start of the window row i's sync read: the day
// before it, or back to the previous snapshot after a gap, within the
// activity lookback.
func reflowWindowStart(rows []*repository.Snapshot, i int) time.Time {
	day := rows[i].Timestamp.UTC()
	start := day.Add(-defaultActivityWindow)
	if i > 0 {
		if prev := rows[i-1].Timestamp.UTC(); prev.Before(start) {
			start = prev
		}
	}
	if floor := day.Add(-maxActivityLookback); start.Before(floor) {
		start = floor
	}
	return start
}

// reflowSince is where the cashflow read must start to cover the window of
// the first reflowed day.
func reflowSince(rows []*repository.Snapshot, from time.Time) time.Time {
	since := from.Add(-defaultActivityWindow)
	for i, r := range rows {
		if r.Timestamp.Before(from) {
			continue
		}
		if ws := reflowWindowStart(rows, i); ws.Before(since) {
			since = ws
		}
		break
	}
	return since
}

// reflowDay classifies row i: kept as stored when it was reconstructed or is
// the connection's first day, otherwise rebuilt from the flows in its window.
func reflowDay(rows []*repository.Snapshot, i int, earliest time.Time, flows []*connector.Cashflow) ReflowDay {
	r := rows[i]
	day := r.Timestamp.UTC()
	d := ReflowDay{Day: day, OldDeposits: r.Deposits, OldWithdrawals: r.Withdrawals}
	switch {
	case r.FromExternalRebuilder || r.IsHistorical:
		d.Kept = "reconstructed day"
	case day.Equal(earliest):
		d.Kept = "first snapshot, may carry the inception deposit"
	}
	if d.Kept != "" {
		d.NewDeposits, d.NewWithdrawals = r.Deposits, r.Withdrawals
		return d
	}
	d.NewDeposits, d.NewWithdrawals = sumFlows(flows, reflowWindowStart(rows, i), day)
	return d
}

// sumFlows splits the flows in [start, end) into deposits and withdrawals.
func sumFlows(flows []*connector.Cashflow, start, end time.Time) (deposits, withdrawals float64) {
	for _, f := range flows {
		t := f.Timestamp.UTC()
		if t.Before(start) || !t.Before(end) {
			continue
		}
		if f.Amount > 0 {
			deposits += f.Amount
		} else {
			withdrawals -= f.Amount
		}
	}
	return deposits, withdrawals
}

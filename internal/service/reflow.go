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

// ReflowCashflows re-derives the deposits and withdrawals of a connection's
// live snapshots between from and to (snapshot days, inclusive) from the
// connector's current classification, after a change in how flows are
// classified left earlier days booked under the old rule. Equity is never
// touched. Each day's window is the one its sync used: the 24 hours before
// it, or back to the previous snapshot after a gap. With apply false nothing
// is written.
func (s *SyncService) ReflowCashflows(ctx context.Context, userUID, exchange, label string, from, to time.Time, apply bool) ([]ReflowDay, error) {
	from, to = from.UTC().Truncate(24*time.Hour), to.UTC().Truncate(24*time.Hour)
	if to.Before(from) {
		return nil, errors.New("reflow: to precedes from")
	}
	if to.Sub(from) > maxReflowWindow {
		return nil, fmt.Errorf("reflow: window longer than %d days", int(maxReflowWindow/(24*time.Hour)))
	}

	connMeta, err := s.connSvc.GetActiveConnectionByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return nil, fmt.Errorf("look up connection: %w", err)
	}
	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, userUID, connMeta.Exchange, connMeta.Label)
	if err != nil {
		return nil, fmt.Errorf("decrypt credentials: %w", err)
	}
	conn, err := s.getOrCreateConnector(connMeta.Exchange, userUID, connMeta.Label, creds)
	if err != nil {
		return nil, fmt.Errorf("build connector: %w", err)
	}
	fetcher, ok := conn.(connector.CashflowFetcher)
	if !ok {
		return nil, fmt.Errorf("reflow: %s reports no cashflows", connMeta.Exchange)
	}
	// The classification depends on which wallets the key reads, which only a
	// balance read establishes.
	if _, err := conn.GetBalance(ctx); err != nil {
		return nil, fmt.Errorf("read balance: %w", err)
	}

	rows, err := s.snapshotRepo.GetByUserAndDateRange(ctx, userUID, from.Add(-maxActivityLookback), to)
	if err != nil {
		return nil, fmt.Errorf("read snapshots: %w", err)
	}
	mine := make([]*repository.Snapshot, 0, len(rows))
	for _, r := range rows {
		if r.Exchange == connMeta.Exchange && r.Label == connMeta.Label {
			mine = append(mine, r)
		}
	}
	sort.Slice(mine, func(i, j int) bool { return mine[i].Timestamp.Before(mine[j].Timestamp) })
	earliest, err := s.snapshotRepo.GetEarliestTimestamp(ctx, userUID, connMeta.Exchange, connMeta.Label)
	if err != nil {
		return nil, fmt.Errorf("read earliest snapshot: %w", err)
	}

	windowStart := func(i int) time.Time {
		day := mine[i].Timestamp.UTC()
		start := day.Add(-defaultActivityWindow)
		if i > 0 {
			if prev := mine[i-1].Timestamp.UTC(); prev.Before(start) {
				start = prev
			}
		}
		if floor := day.Add(-maxActivityLookback); start.Before(floor) {
			start = floor
		}
		return start
	}

	since := from.Add(-defaultActivityWindow)
	for i, r := range mine {
		if !r.Timestamp.Before(from) {
			if ws := windowStart(i); ws.Before(since) {
				since = ws
			}
			break
		}
	}
	flows, err := fetcher.GetCashflows(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("read cashflows: %w", err)
	}

	var out []ReflowDay
	written := 0
	for i, r := range mine {
		day := r.Timestamp.UTC()
		if day.Before(from) || day.After(to) {
			continue
		}
		d := ReflowDay{Day: day, OldDeposits: r.Deposits, OldWithdrawals: r.Withdrawals}
		switch {
		case r.FromExternalRebuilder || r.IsHistorical:
			d.Kept = "reconstructed day"
		case day.Equal(earliest.UTC()):
			d.Kept = "first snapshot, may carry the inception deposit"
		}
		if d.Kept != "" {
			d.NewDeposits, d.NewWithdrawals = r.Deposits, r.Withdrawals
			out = append(out, d)
			continue
		}

		start := windowStart(i)
		for _, f := range flows {
			t := f.Timestamp.UTC()
			if t.Before(start) || !t.Before(day) {
				continue
			}
			if f.Amount > 0 {
				d.NewDeposits += f.Amount
			} else {
				d.NewWithdrawals -= f.Amount
			}
		}

		changed := math.Abs(d.NewDeposits-d.OldDeposits) > 1e-9 || math.Abs(d.NewWithdrawals-d.OldWithdrawals) > 1e-9
		if apply && changed {
			ok, err := s.snapshotRepo.UpdateFlows(ctx, r.ID, r.Deposits, r.Withdrawals, d.NewDeposits, d.NewWithdrawals)
			if err != nil {
				return out, err
			}
			d.Written = ok
			if ok {
				written++
			}
		}
		out = append(out, d)
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

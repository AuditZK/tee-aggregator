package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

// errNoStoredSnapshots: the connection has nothing stored to convert.
var errNoStoredSnapshots = errors.New("no snapshots")

// FXBackfillDay is one stored day of a USD backfill, as stored and as it
// would be.
type FXBackfillDay struct {
	Day            string  `json:"day"`
	Kept           string  `json:"kept,omitempty"`
	Rate           float64 `json:"rate,omitempty"`
	Equity         float64 `json:"equity"`
	NewEquity      float64 `json:"new_equity,omitempty"`
	Deposits       float64 `json:"deposits"`
	NewDeposits    float64 `json:"new_deposits,omitempty"`
	Withdrawals    float64 `json:"withdrawals"`
	NewWithdrawals float64 `json:"new_withdrawals,omitempty"`
}

// storedMeasuredOn is the day whose close a stored row's equity describes:
// its own date for IBKR, whose rows are keyed by statement date by the Flex
// reconstruction that rewrites them, and the day before for everyone else,
// read at the midnight that opens the row's date. The same rule the live
// sync and the reconstructions follow, so a backfilled day and a freshly
// written one agree.
func storedMeasuredOn(exchange string, ts time.Time) time.Time {
	d := truncDay(ts)
	if strings.EqualFold(exchange, "ibkr") {
		return d
	}
	return d.Add(-24 * time.Hour)
}

func breakdownMarkets(b *repository.MarketBreakdown) []*repository.MarketMetrics {
	if b == nil {
		return nil
	}
	return []*repository.MarketMetrics{b.Stocks, b.Spot, b.Swap, b.Futures, b.Options,
		b.Margin, b.Earn, b.CFD, b.Forex, b.Commodities, b.Global}
}

// alreadyConverted reports whether a stored row carries the conversion stamp.
func alreadyConverted(r *repository.Snapshot) bool {
	return r.Breakdown != nil && r.Breakdown.Global != nil && r.Breakdown.Global.NativeCurrency != ""
}

// convertStoredRow converts a row written before the conversion existed. Its
// flows are taken in the account currency at the row's rate: the sums no
// longer say which flow was in which currency. The next IBKR reconstruction
// rewrites its window from the flows themselves.
func convertStoredRow(r *repository.Snapshot, currency string, rate float64) {
	r.TotalEquity *= rate
	r.RealizedBalance *= rate
	r.UnrealizedPnL *= rate
	r.Deposits *= rate
	r.Withdrawals *= rate
	for _, m := range breakdownMarkets(r.Breakdown) {
		if m != nil {
			m.Equity *= rate
			m.AvailableMargin *= rate
		}
	}
	if r.Breakdown == nil {
		r.Breakdown = &repository.MarketBreakdown{}
	}
	stampFX(r.Breakdown, currency, rate)
}

// planFXBackfill converts, in place, every stored row of one connection that
// is not stamped yet and has a final rate, and reports each day. Pure.
func planFXBackfill(rows []*repository.Snapshot, exchange, currency string, rates fxRates) (days []FXBackfillDay, changed []*repository.Snapshot) {
	for _, r := range rows {
		d := FXBackfillDay{Day: dayKey(r.Timestamp), Equity: r.TotalEquity, Deposits: r.Deposits, Withdrawals: r.Withdrawals}
		switch rate, ok := rates.rate(currency, storedMeasuredOn(exchange, r.Timestamp)); {
		case alreadyConverted(r):
			d.Kept = "already converted from " + r.Breakdown.Global.NativeCurrency
		case !ok:
			d.Kept = "no final rate yet"
		default:
			convertStoredRow(r, currency, rate)
			d.Rate = rate
			d.NewEquity, d.NewDeposits, d.NewWithdrawals = r.TotalEquity, r.Deposits, r.Withdrawals
			changed = append(changed, r)
		}
		days = append(days, d)
	}
	return days, changed
}

// ConvertStoredToUSD converts the rows a connection stored before the USD
// conversion existed, at each day's rate. The account currency is given by
// the operator, who reads it off the connection's warning marker: guessing
// it here would convert a USD account. A row already stamped is left alone,
// so the call can be repeated. With apply false nothing is written; with it,
// every row is written in one batch.
func (s *SyncService) ConvertStoredToUSD(ctx context.Context, userUID, exchange, label, currency string, apply bool) ([]FXBackfillDay, error) {
	ccy, ok := fiatToConvert(currency)
	if !ok {
		return nil, fmt.Errorf("%q is not a currency converted to USD", currency)
	}
	if s.fx == nil {
		return nil, errors.New("no FX source configured")
	}
	// Converting the stored days of a broker whose live days are not would
	// mix units the other way round.
	if !convertsExchange(exchange) {
		return nil, fmt.Errorf("%s accounts are not converted to USD yet", exchange)
	}

	all, err := s.snapshotRepo.GetByUserAndDateRange(ctx, userUID, time.Unix(0, 0).UTC(), time.Now().UTC().Add(24*time.Hour))
	if err != nil {
		return nil, fmt.Errorf("read snapshots: %w", err)
	}
	var rows []*repository.Snapshot
	for _, r := range all {
		if strings.EqualFold(r.Exchange, exchange) && r.Label == label {
			rows = append(rows, r)
		}
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w for %s/%s", errNoStoredSnapshots, exchange, label)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Timestamp.Before(rows[j].Timestamp) })

	from := storedMeasuredOn(exchange, rows[0].Timestamp)
	to := storedMeasuredOn(exchange, rows[len(rows)-1].Timestamp)
	rates, err := loadFXRates(ctx, s.fx, map[string]bool{ccy: true}, from, to)
	if err != nil {
		return nil, err
	}

	days, changed := planFXBackfill(rows, exchange, ccy, rates)
	if apply && len(changed) > 0 {
		if err := s.snapshotRepo.UpsertBatch(ctx, changed); err != nil {
			return days, fmt.Errorf("write converted rows: %w", err)
		}
		s.notifyHistoryRebuilt(ctx, userUID)
	}
	s.logger.Info("stored history converted to USD",
		zap.String("user_uid", userUID),
		zap.String("exchange", exchange),
		zap.String("label", label),
		zap.String("currency", ccy),
		zap.Bool("apply", apply),
		zap.Int("days", len(days)),
		zap.Int("converted", len(changed)),
	)
	return days, nil
}

// backfillsStoredRows reports whether a venue's stored days are converted by
// the backfill. Not IBKR's: its reconstruction rewrites the whole Flex window
// every sync, and the backfill reads an unstamped row as one in the account's
// currency, which multiplied by the rate a dollar row written while the
// statement was in USD.
func backfillsStoredRows(exchange string) bool {
	return !strings.EqualFold(exchange, "ibkr")
}

// ensureStoredConverted converts a connection's stored days before the sync
// writes its first converted one.
//
// Run by hand, the backfill was a race against the next sync: a converted day
// written next to stored EUR days is a step the size of the rate, read as a
// return, and the first IBKR reconstruction would be refused by the gate
// against the unconverted days. And a hand-kept list of non-USD connections
// misses the ones whose marker a later error overwrote. So the sync does it,
// the first time it meets the account in a convertible currency, and does
// not write until it has. Idempotent through the stamp; remembered per
// process once every stored day is converted.
func (s *SyncService) ensureStoredConverted(ctx context.Context, connMeta *repository.ExchangeConnection, currency string) error {
	if !backfillsStoredRows(connMeta.Exchange) {
		return nil
	}
	key := connMeta.UserUID + "|" + strings.ToLower(connMeta.Exchange) + "|" + connMeta.Label
	if _, done := s.fxStoredDone.Load(key); done {
		return nil
	}
	days, err := s.ConvertStoredToUSD(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label, currency, true)
	if errors.Is(err, errNoStoredSnapshots) {
		s.fxStoredDone.Store(key, true)
		return nil
	}
	if err != nil {
		return fmt.Errorf("convert stored days: %w", err)
	}
	pending := 0
	for _, d := range days {
		if d.Kept == "no final rate yet" {
			pending++
		}
	}
	if pending == 0 {
		s.fxStoredDone.Store(key, true)
	}
	return nil
}

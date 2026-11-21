package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

// fxTailGrace is how recent a day may be and still lack a final rate: the
// close of the last trading days becomes final only once a later close
// exists. Past that, a missing rate is a hole in the series and the report is
// refused rather than cut short.
const fxTailGrace = 7 * 24 * time.Hour

// reportCurrency normalizes a requested report currency; empty means USD.
func reportCurrency(c string) string {
	c = strings.ToUpper(strings.TrimSpace(c))
	if c == "" {
		return "USD"
	}
	return c
}

// loadReportRates reads the final ccy rates covering every day a report in ccy
// values: each snapshot's measured day and each benchmark close.
func (s *ReportService) loadReportRates(ctx context.Context, ccy string, snapshots []*repository.Snapshot, start, end time.Time) (fxRates, error) {
	if !fxConvertible[ccy] {
		return nil, fmt.Errorf("report currency not supported: %s", ccy)
	}
	if s.benchmarkSvc == nil {
		return nil, fmt.Errorf("report in %s: fx service not available", ccy)
	}
	from := storedMeasuredOn(snapshots[0].Exchange, snapshots[0].Timestamp)
	to := truncDay(snapshots[len(snapshots)-1].Timestamp)
	if !start.IsZero() && truncDay(start).Before(from) {
		from = truncDay(start)
	}
	if !end.IsZero() && truncDay(end).After(to) {
		to = truncDay(end)
	}
	return loadFXRates(ctx, s.benchmarkSvc, map[string]bool{ccy: true}, from, to)
}

// snapshotsInCurrency values the report's USD snapshots in ccy, each at the
// final rate of the day its equity was measured, so the returns computed from
// them carry the currency's own moves. snapshots must be sorted. Trailing days
// without a final rate yet are dropped, the report ending at the last day it
// can state in ccy, so a re-run over the same period signs the same figures.
func snapshotsInCurrency(snapshots []*repository.Snapshot, rates fxRates, ccy string, now time.Time) ([]*repository.Snapshot, error) {
	out := make([]*repository.Snapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		r, ok := snapshotRate(snap, rates, ccy)
		if !ok {
			cut := truncDay(snap.Timestamp)
			if now.Sub(cut) > fxTailGrace {
				return nil, fmt.Errorf("no final exchange rate for %s on %s", ccy, dayKey(storedMeasuredOn(snap.Exchange, snap.Timestamp)))
			}
			return beforeDay(out, cut), nil
		}
		c := *snap
		c.TotalEquity /= r
		c.RealizedBalance /= r
		c.UnrealizedPnL /= r
		c.Deposits /= r
		c.Withdrawals /= r
		c.TotalVolume /= r
		c.TotalFees /= r
		out = append(out, &c)
	}
	return out, nil
}

// snapshotRate is the USD value of one unit of ccy on the day snap was
// measured. A row converted from an account held in ccy carries the very rate
// it was converted at, which gives its native figures back exactly.
func snapshotRate(snap *repository.Snapshot, rates fxRates, ccy string) (float64, bool) {
	if b := snap.Breakdown; b != nil && b.Global != nil && b.Global.FXRateToUSD > 0 &&
		strings.EqualFold(b.Global.NativeCurrency, ccy) {
		return b.Global.FXRateToUSD, true
	}
	return rates.rate(ccy, storedMeasuredOn(snap.Exchange, snap.Timestamp))
}

// beforeDay keeps the snapshots dated before day: a day is dropped whole, not
// with one connection valued and the other left out.
func beforeDay(snapshots []*repository.Snapshot, day time.Time) []*repository.Snapshot {
	out := snapshots[:0]
	for _, s := range snapshots {
		if truncDay(s.Timestamp).Before(day) {
			out = append(out, s)
		}
	}
	return out
}

// benchPointsIn values benchmark closes in ccy at each close's own final rate,
// leaving out a close without one.
func benchPointsIn(points []benchPoint, rates fxRates, ccy string) []benchPoint {
	out := make([]benchPoint, 0, len(points))
	for _, p := range points {
		day, err := time.Parse(dateFormat, p.date)
		if err != nil {
			continue
		}
		r, ok := rates.rate(ccy, day)
		if !ok {
			continue
		}
		out = append(out, benchPoint{date: p.date, close: p.close / r})
	}
	return out
}

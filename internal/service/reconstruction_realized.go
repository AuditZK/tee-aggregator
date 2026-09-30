package service

import (
	"math"
	"sort"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

// realizedOnlyReconstruction reports whether an exchange's in-enclave
// reconstruction carries the settled balance and nothing else. cTrader's walk
// reads the balance each closing deal and ledger entry left behind and has no
// historical marks, so its days hold no unrealized PnL where the live sync's
// do, and comparing the two equities rejected every day a position was open.
func realizedOnlyReconstruction(exchange string) bool {
	return strings.EqualFold(exchange, "ctrader")
}

// isLiveMeasurement reports whether a stored row was written by the live sync.
// The production schema has no column saying so for in-enclave rows
// (is_historical is Go-only), but the two writers leave different breakdowns:
// the live path always files the equity under a market (completeBreakdown), a
// reconstruction fills only the global aggregate.
func isLiveMeasurement(s *repository.Snapshot) bool {
	if s.IsHistorical || s.FromExternalRebuilder || s.Breakdown == nil {
		return false
	}
	b := s.Breakdown
	return b.Stocks != nil || b.Spot != nil || b.Swap != nil || b.Futures != nil ||
		b.Options != nil || b.Margin != nil || b.Earn != nil || b.CFD != nil ||
		b.Forex != nil || b.Commodities != nil
}

// realizedMerge is what a realized-only reconstruction does to the rows a
// connection already holds.
type realizedMerge struct {
	write []*repository.Snapshot
	// agree and disagree count the measured days whose realized balance the
	// reconstruction reproduces or not; contradicted is the first of the
	// latter and rebuiltAt the balance the reconstruction gave it.
	agree, disagree int
	contradicted    *repository.Snapshot
	rebuiltAt       float64
}

// rejected reports whether more measured days contradict the reconstruction
// than reproduce it. A lone disagreement is expected: the connect-time sync and
// a manual re-sync measure mid-day, not at the midnight a rebuilt row holds. A
// reconstruction of the wrong account disagrees everywhere.
func (m realizedMerge) rejected() bool { return m.disagree > m.agree }

// mergeRealizedOnly folds a realized-only reconstruction into a connection's
// stored rows.
//
// A connection with nothing stored before today takes the whole series: that
// is the backfill at connect.
//
// Once a history is stored, the reconstruction never rewrites, extends or cuts
// it, because what the broker serves now is not what it served then. A demo
// reset erases the ledger before it: rebuilt from what is left, the account
// starts on the reset, booked as a deposit of the full new balance, and the
// stored days before it no longer add up with it. A broker can also serve more
// than it once did, and a series grown backwards moves the account's
// inception. So a day a previous reconstruction wrote stays as it is, and a
// day nobody wrote is filled only when the day before it is held: a live row
// written after a sync gap carries the flows of the whole gap, and filling the
// gap from inside a 14-day window would drop the rest.
//
// A day the live sync measured keeps its measurement when the rebuild
// reproduces its realized balance: the live row also holds the unrealized PnL,
// the free margin and the per-market split the rebuild cannot know. It takes
// only the rebuilt flows, which follow calendar days and see through a demo
// reset. A measured day the rebuild does not reproduce was measured mid-day,
// and the midnight rebuild replaces it.
func mergeRealizedOnly(rebuilt, existing []*repository.Snapshot, today time.Time) realizedMerge {
	const day = 24 * time.Hour
	fresh := true
	stored := make(map[time.Time]*repository.Snapshot, len(existing))
	for _, e := range existing {
		d := e.Timestamp.UTC().Truncate(day)
		stored[d] = e
		if d.Before(today) {
			fresh = false
		}
	}
	ordered := append([]*repository.Snapshot(nil), rebuilt...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Timestamp.Before(ordered[j].Timestamp) })

	var m realizedMerge
	written := make(map[time.Time]bool, len(ordered))
	for _, r := range ordered {
		d := r.Timestamp.UTC().Truncate(day)
		prev := d.Add(-day)
		_, prevStored := stored[prev]
		held := fresh || prevStored || written[prev]

		e := stored[d]
		switch {
		case e == nil:
			if held {
				m.write = append(m.write, r)
				written[d] = true
			}
		case isLiveMeasurement(e) && e.RealizedBalance > 0:
			if !reproduces(r.RealizedBalance, e.RealizedBalance) {
				m.disagree++
				if m.contradicted == nil {
					m.contradicted, m.rebuiltAt = e, r.RealizedBalance
				}
				if held {
					m.write = append(m.write, r)
				}
				continue
			}
			m.agree++
			if !held || (e.Deposits == r.Deposits && e.Withdrawals == r.Withdrawals) {
				continue
			}
			kept := *e
			kept.Deposits, kept.Withdrawals = r.Deposits, r.Withdrawals
			m.write = append(m.write, &kept)
		}
	}
	return m
}

func reproduces(rebuilt, measured float64) bool {
	return math.Abs(rebuilt-measured)/math.Abs(measured) <= reproductionTolerance
}

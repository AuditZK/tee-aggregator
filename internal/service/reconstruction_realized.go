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

// realizedMode is how much of a connection's stored history a realized-only
// reconstruction owns.
type realizedMode int

const (
	// realizedFull (connect, admin reconstruct) rewrites every reconstructed
	// day it spans and vacates the one its new dating left behind.
	realizedFull realizedMode = iota
	// realizedWindow (gap repair) rewrites the days of its window, no other.
	realizedWindow
	// realizedRecurring (every sync) never rewrites a reconstructed day.
	realizedRecurring
)

// realizedMerge is what a realized-only reconstruction does to the rows a
// connection already holds.
type realizedMerge struct {
	write   []*repository.Snapshot
	vacated []time.Time
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
// A day the live sync measured keeps its measurement when the rebuild
// reproduces its realized balance: the live row also holds the unrealized PnL,
// the free margin and the per-market split the rebuild cannot know. It takes
// only the rebuilt flows, which follow calendar days and see through a demo
// reset. A measured day the rebuild does not reproduce was measured mid-day,
// and the midnight rebuild replaces it.
//
// The recurring run leaves every reconstructed day alone, and writes a day only
// when the day before it is held. A live row written after a sync gap carries
// the flows of the whole gap; handing it the flows of its last day alone, or
// filling the gap from inside a 14-day window, would drop the rest.
//
// A full run also vacates the stored reconstructed days its series no longer
// covers, from the day before its first row. Rows were once keyed by the day
// that had just closed; the row that dating left before the new first row
// holds the same flows as that first row.
func mergeRealizedOnly(rebuilt, existing []*repository.Snapshot, mode realizedMode) realizedMerge {
	const day = 24 * time.Hour
	stored := make(map[time.Time]*repository.Snapshot, len(existing))
	for _, e := range existing {
		stored[e.Timestamp.UTC().Truncate(day)] = e
	}
	ordered := append([]*repository.Snapshot(nil), rebuilt...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Timestamp.Before(ordered[j].Timestamp) })

	var m realizedMerge
	emitted := make(map[time.Time]bool, len(ordered))
	written := make(map[time.Time]bool, len(ordered))
	for _, r := range ordered {
		d := r.Timestamp.UTC().Truncate(day)
		emitted[d] = true
		prev := d.Add(-day)
		_, prevStored := stored[prev]
		bounded := mode == realizedRecurring && !prevStored && !written[prev]

		e := stored[d]
		switch {
		case e == nil:
			if bounded {
				continue
			}
			m.write = append(m.write, r)
			written[d] = true
		case isLiveMeasurement(e) && e.RealizedBalance > 0:
			if !reproduces(r.RealizedBalance, e.RealizedBalance) {
				m.disagree++
				if m.contradicted == nil {
					m.contradicted, m.rebuiltAt = e, r.RealizedBalance
				}
				if !bounded {
					m.write = append(m.write, r)
				}
				continue
			}
			m.agree++
			if bounded || (e.Deposits == r.Deposits && e.Withdrawals == r.Withdrawals) {
				continue
			}
			kept := *e
			kept.Deposits, kept.Withdrawals = r.Deposits, r.Withdrawals
			m.write = append(m.write, &kept)
		case mode != realizedRecurring:
			m.write = append(m.write, r)
		}
	}

	if mode == realizedFull && len(ordered) > 0 {
		from := ordered[0].Timestamp.UTC().Truncate(day).Add(-day)
		to := ordered[len(ordered)-1].Timestamp.UTC().Truncate(day)
		for d, e := range stored {
			if d.Before(from) || d.After(to) || emitted[d] || isLiveMeasurement(e) {
				continue
			}
			m.vacated = append(m.vacated, d)
		}
		sort.Slice(m.vacated, func(i, j int) bool { return m.vacated[i].Before(m.vacated[j]) })
	}
	return m
}

func reproduces(rebuilt, measured float64) bool {
	return math.Abs(rebuilt-measured)/math.Abs(measured) <= reproductionTolerance
}

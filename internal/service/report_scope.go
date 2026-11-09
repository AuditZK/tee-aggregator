package service

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/signing"
)

// ReportPortfolio scopes a signed report to one portfolio.
type ReportPortfolio struct {
	Name             string
	Stays            []repository.PortfolioMember
	ConnectionsTotal int
}

// staysInPortfolio groups a journal by connection, each connection's stays in order.
func staysByConnection(stays []repository.PortfolioMember) map[string][]repository.PortfolioMember {
	out := make(map[string][]repository.PortfolioMember, len(stays))
	for _, stay := range stays {
		key := snapshotConnectionKey(stay.Exchange, stay.Label)
		out[key] = append(out[key], stay)
	}
	for _, list := range out {
		sort.SliceStable(list, func(i, j int) bool { return list[i].AddedAt.Before(list[j].AddedAt) })
	}
	return out
}

// inStays says whether a moment belongs to a portfolio. A connection's first
// stay brings its whole past in, and everything until it leaves stays
// consolidated; what it records while out of the portfolio is not the portfolio's.
func inStays(list []repository.PortfolioMember, at time.Time) bool {
	for i, stay := range list {
		afterStart := i == 0 || !at.Before(stay.AddedAt)
		beforeEnd := stay.RemovedAt == nil || !at.After(*stay.RemovedAt)
		if afterStart && beforeEnd {
			return true
		}
	}
	return false
}

// snapshotsInPortfolio keeps what a portfolio consolidates. Same rule as the
// analytics' view (services/portfolio-scope.ts), so the report and the dashboard
// describe the same series.
func snapshotsInPortfolio(snapshots []*repository.Snapshot, stays []repository.PortfolioMember) []*repository.Snapshot {
	byConnection := staysByConnection(stays)
	kept := make([]*repository.Snapshot, 0, len(snapshots))
	for _, s := range snapshots {
		if inStays(byConnection[snapshotConnectionKey(s.Exchange, s.Label)], s.Timestamp) {
			kept = append(kept, s)
		}
	}
	return kept
}

// portfolioRebuilderDays narrows the days rebuilt outside the enclave to the
// connections of the portfolio. The per-user set would label a member's day with
// the provenance of an account that is not in this report.
func (s *ReportService) portfolioRebuilderDays(ctx context.Context, req *GenerateReportRequest) (map[time.Time]struct{}, error) {
	rows, err := s.snapshotRepo.GetExternalRebuilderConnectionDays(ctx, req.UserUID, req.StartDate, req.EndDate)
	if err != nil {
		return nil, err
	}
	byConnection := staysByConnection(req.Portfolio.Stays)
	days := make(map[time.Time]struct{})
	for _, r := range rows {
		if inStays(byConnection[snapshotConnectionKey(r.Exchange, r.Label)], r.Day) {
			days[r.Day] = struct{}{}
		}
	}
	return days, nil
}

// verifiedFromOf is the first day past the last one rebuilt outside the enclave
// and past the last time a connection came back to a series that went on without
// it. Empty when no such day exists.
//
// Silent past staleConnectionDays, a connection leaves the TWR at its last value
// and rejoins as a new one, so what it lost meanwhile never reaches the return:
// that is what pausing a losing account would buy. A gap nothing else recorded
// through is not that, as in a platform outage or a one-account report. Same rule
// as the analytics (services/verified-history.ts).
func verifiedFromOf(snapshots []*repository.Snapshot, rebuilderDays map[time.Time]struct{}) string {
	daysOfConnection := make(map[string]map[string]struct{})
	recorded := make(map[string]struct{})
	for _, snap := range snapshots {
		day := snap.Timestamp.Format(dateFormat)
		key := snapshotConnectionKey(snap.Exchange, snap.Label)
		if daysOfConnection[key] == nil {
			daysOfConnection[key] = make(map[string]struct{})
		}
		daysOfConnection[key][day] = struct{}{}
		recorded[day] = struct{}{}
	}

	boundary := ""
	for day := range rebuilderDays {
		next := day.AddDate(0, 0, 1).Format(dateFormat)
		if next > boundary {
			boundary = next
		}
	}
	for _, days := range daysOfConnection {
		sorted := make([]string, 0, len(days))
		for day := range days {
			sorted = append(sorted, day)
		}
		sort.Strings(sorted)
		for i := 1; i < len(sorted); i++ {
			if sorted[i] > boundary && wentOnWithout(sorted[i-1], sorted[i], recorded) {
				boundary = sorted[i]
			}
		}
	}

	all := make([]string, 0, len(recorded))
	for day := range recorded {
		all = append(all, day)
	}
	sort.Strings(all)
	for _, day := range all {
		if day >= boundary {
			return day
		}
	}
	return ""
}

// wentOnWithout says whether a gap in one connection lasted long enough for it to
// leave the TWR, while something else was recording.
func wentOnWithout(from, to string, recorded map[string]struct{}) bool {
	start, errFrom := time.Parse(dateFormat, from)
	end, errTo := time.Parse(dateFormat, to)
	if errFrom != nil || errTo != nil || int(end.Sub(start).Hours()/24) <= staleConnectionDays {
		return false
	}
	for at := start.AddDate(0, 0, 1); at.Before(end); at = at.AddDate(0, 0, 1) {
		if _, ok := recorded[at.Format(dateFormat)]; ok {
			return true
		}
	}
	return false
}

// portfolioComposition is what a reader is told of a portfolio, under the
// signature: dates and venues, not the account labels the owner chose.
func portfolioComposition(p *ReportPortfolio) *signing.PortfolioComposition {
	current := make(map[string]struct{})
	stays := make([]signing.PortfolioStay, 0, len(p.Stays))
	for _, stay := range p.Stays {
		signed := signing.PortfolioStay{Exchange: stay.Exchange, AddedAt: stay.AddedAt.UTC().Format(dateFormat)}
		if stay.RemovedAt != nil {
			signed.RemovedAt = stay.RemovedAt.UTC().Format(dateFormat)
		} else {
			current[snapshotConnectionKey(stay.Exchange, stay.Label)] = struct{}{}
		}
		stays = append(stays, signed)
	}
	return &signing.PortfolioComposition{
		Name:                p.Name,
		ConnectionsIncluded: len(current),
		ConnectionsTotal:    p.ConnectionsTotal,
		Stays:               stays,
	}
}

// ForReport reads a portfolio for a signed report.
func (s *PortfolioService) ForReport(ctx context.Context, userUID, id string) (*ReportPortfolio, error) {
	p, err := s.store.Get(ctx, userUID, id)
	if err != nil {
		return nil, err
	}
	total, err := s.connections.CountConnections(ctx, userUID)
	if err != nil {
		return nil, err
	}
	if len(p.Members) == 0 {
		return nil, errors.New("portfolio has no connections")
	}
	return &ReportPortfolio{Name: p.Name, Stays: p.Members, ConnectionsTotal: total}, nil
}

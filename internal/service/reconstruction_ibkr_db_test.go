package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// A synthetic IBKR account whose live rows each hold the statement of the day
// before, with a day missing. The reconstruction states every day at its own
// date: it must be accepted, fill the missing day, and leave every rewritten
// day holding its own statement.
func TestDBIBKRReconstructionFillsTheDaysTheLiveSyncMissed(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	connMeta, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "ibkr", "main")
	if err != nil {
		t.Fatalf("read connection: %v", err)
	}

	today := startOfTodayUTC()
	d := func(n int) time.Time { return today.AddDate(0, 0, -10+n) }
	closeOf := func(n int) float64 { return 1000 + 10*float64(n) }
	live := func(n int) *repository.Snapshot {
		eq := closeOf(n - 1)
		return &repository.Snapshot{
			UserUID: dbUser, Exchange: "ibkr", Label: "main", Timestamp: d(n),
			TotalEquity: eq, RealizedBalance: eq,
			Breakdown: &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: eq}},
		}
	}
	if err := h.snaps.UpsertBatch(h.ctx, []*repository.Snapshot{live(2), live(3), live(4), live(6)}); err != nil {
		t.Fatalf("seed history: %v", err)
	}

	var rebuilt []*connector.HistoricalSnapshot
	for n := 1; n <= 6; n++ {
		rebuilt = append(rebuilt, &connector.HistoricalSnapshot{Date: d(n), TotalEquity: closeOf(n), RealizedBalance: closeOf(n)})
	}
	if err := h.sync.persistHistoricalSnapshots(h.ctx, connMeta, rebuilt, false, sourceInEnclave, reconstructOpts{}); err != nil {
		t.Fatalf("reconstruction rejected: %v", err)
	}

	rows, err := h.snaps.GetByUserAndDateRange(h.ctx, dbUser, d(0), today)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	byDay := map[time.Time]float64{}
	for _, r := range rows {
		byDay[r.Timestamp.UTC()] = r.TotalEquity
	}
	for n := 1; n <= 6; n++ {
		if got, ok := byDay[d(n)]; !ok || got != closeOf(n) {
			t.Fatalf("%s = %v (present %v), want its own close %v", d(n).Format(time.DateOnly), got, ok, closeOf(n))
		}
	}
}

package service

import (
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/repository"
)

// liveRow is a day the live sync measured: equity with the open positions'
// unrealized PnL, filed under a market with its free margin.
func liveRow(d time.Time, realized, upnl, deposits float64) *repository.Snapshot {
	return &repository.Snapshot{
		Timestamp:       d,
		TotalEquity:     realized + upnl,
		RealizedBalance: realized,
		UnrealizedPnL:   upnl,
		Deposits:        deposits,
		Breakdown: &repository.MarketBreakdown{
			CFD:    &repository.MarketMetrics{Equity: realized + upnl, AvailableMargin: realized / 2},
			Global: &repository.MarketMetrics{Equity: realized + upnl},
		},
	}
}

// rebuiltRow is a day the realized-only reconstruction produces.
func rebuiltRow(d time.Time, realized, deposits float64) *repository.Snapshot {
	return &repository.Snapshot{
		Timestamp:       d,
		TotalEquity:     realized,
		RealizedBalance: realized,
		Deposits:        deposits,
		IsHistorical:    true,
		Breakdown:       &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: realized}},
	}
}

// storedReconstruction is a row an earlier reconstruction wrote, as the
// production schema reads it back: no is_historical column to say so.
func storedReconstruction(d time.Time, realized, deposits float64) *repository.Snapshot {
	r := rebuiltRow(d, realized, deposits)
	r.IsHistorical = false
	return r
}

func writtenOn(m realizedMerge, d time.Time) *repository.Snapshot {
	for _, w := range m.write {
		if w.Timestamp.Equal(d) {
			return w
		}
	}
	return nil
}

func TestRealizedOnlyReconstruction(t *testing.T) {
	if !realizedOnlyReconstruction("cTrader") || !realizedOnlyReconstruction("ctrader") {
		t.Fatal("cTrader's reconstruction is realized-only")
	}
	if realizedOnlyReconstruction("ibkr") {
		t.Fatal("IBKR's Flex history carries its own marks")
	}
}

func TestIsLiveMeasurement(t *testing.T) {
	d := day(2026, time.May, 4)
	if !isLiveMeasurement(liveRow(d, 1000, 0, 0)) {
		t.Fatal("a live row files its equity under a market")
	}
	if isLiveMeasurement(storedReconstruction(d, 1000, 0)) {
		t.Fatal("a reconstruction fills only the global aggregate")
	}
	hist := liveRow(d, 1000, 0, 0)
	hist.IsHistorical = true
	if isLiveMeasurement(hist) {
		t.Fatal("a row flagged historical is not a measurement")
	}
}

// The rebuild reproduces the realized balance the live sync read at midnight.
// Overwriting the live row threw away what only it knows: the open positions'
// unrealized PnL, the free margin, the per-market split. It keeps all of that
// and takes the rebuilt flows, which see through a demo reset the live read
// booked at its raw size.
func TestMergeRealizedOnly_KeepsTheMeasurementAndTakesTheFlows(t *testing.T) {
	d0, d1 := day(2026, time.May, 4), day(2026, time.May, 5)
	stored := []*repository.Snapshot{liveRow(d0, 1000, 0, 0), liveRow(d1, 10000, 250, 10000)}
	rebuilt := []*repository.Snapshot{rebuiltRow(d1, 10000, 9000)}

	m := mergeRealizedOnly(rebuilt, stored, realizedRecurring)

	got := writtenOn(m, d1)
	if got == nil {
		t.Fatal("the corrected flows were not written")
	}
	if got.TotalEquity != 10250 || got.UnrealizedPnL != 250 || got.Breakdown.CFD == nil || got.Breakdown.CFD.AvailableMargin != 5000 {
		t.Fatalf("the measurement was lost: %+v", got)
	}
	if got.Deposits != 9000 {
		t.Fatalf("deposits %v, want the rebuilt 9000", got.Deposits)
	}
	if got.IsHistorical {
		t.Fatal("a measured day stays a measurement")
	}
	if m.agree != 1 || m.rejected() {
		t.Fatalf("agree=%d disagree=%d", m.agree, m.disagree)
	}
}

func TestMergeRealizedOnly_LeavesAMatchingMeasurementUntouched(t *testing.T) {
	d0, d1 := day(2026, time.May, 4), day(2026, time.May, 5)
	stored := []*repository.Snapshot{liveRow(d0, 1000, 0, 0), liveRow(d1, 1000, 40, 0)}

	m := mergeRealizedOnly([]*repository.Snapshot{rebuiltRow(d1, 1000, 0)}, stored, realizedRecurring)

	if len(m.write) != 0 {
		t.Fatalf("rewrote a day that needed nothing: %+v", m.write)
	}
}

// The connect-time sync and a manual re-sync measure mid-day. The midnight
// rebuild replaces that row, and one such day among agreeing ones does not
// condemn the reconstruction.
func TestMergeRealizedOnly_ReplacesAMidDayMeasurement(t *testing.T) {
	d0, d1, d2 := day(2026, time.May, 4), day(2026, time.May, 5), day(2026, time.May, 6)
	stored := []*repository.Snapshot{
		liveRow(d0, 1000, 0, 0),
		liveRow(d1, 1000, 0, 0),
		liveRow(d2, 1180, 0, 200), // measured at 14:00, after a deposit and a trade
	}
	rebuilt := []*repository.Snapshot{rebuiltRow(d1, 1000, 0), rebuiltRow(d2, 1000, 0)}

	m := mergeRealizedOnly(rebuilt, stored, realizedRecurring)

	if m.rejected() {
		t.Fatalf("one mid-day measurement rejected the reconstruction (agree=%d disagree=%d)", m.agree, m.disagree)
	}
	got := writtenOn(m, d2)
	if got == nil || got.RealizedBalance != 1000 || got.Deposits != 0 {
		t.Fatalf("the mid-day row was not replaced by the midnight rebuild: %+v", got)
	}
}

// A reconstruction of another account disagrees with every measured day.
func TestMergeRealizedOnly_RejectsWhenMostMeasuredDaysDisagree(t *testing.T) {
	d0, d1, d2 := day(2026, time.May, 4), day(2026, time.May, 5), day(2026, time.May, 6)
	stored := []*repository.Snapshot{liveRow(d0, 1000, 0, 0), liveRow(d1, 1000, 0, 0), liveRow(d2, 1010, 0, 0)}
	rebuilt := []*repository.Snapshot{rebuiltRow(d1, 52000, 0), rebuiltRow(d2, 52100, 0)}

	m := mergeRealizedOnly(rebuilt, stored, realizedFull)

	if !m.rejected() {
		t.Fatal("a reconstruction contradicting every measured day was accepted")
	}
	if m.contradicted == nil || !m.contradicted.Timestamp.Equal(d1) || m.rebuiltAt != 52000 {
		t.Fatalf("reported %+v / %v, want the first contradicted day", m.contradicted, m.rebuiltAt)
	}
}

// The day after a connect, the connect-time row is the only measured day and
// it was taken mid-day. The recurring run waits for the next midnight sync
// rather than raising an alarm; a full run, which an operator started, says
// so.
func TestMergeRealizedReconstruction_ALoneContradictionWaitsForASecondDay(t *testing.T) {
	d0, d1 := day(2026, time.May, 4), day(2026, time.May, 5)
	stored := []*repository.Snapshot{storedReconstruction(d0, 1000, 1000), liveRow(d1, 1180, 0, 0)}
	rebuilt := []*repository.Snapshot{rebuiltRow(d1, 1000, 0)}
	s := &SyncService{logger: zap.NewNop()}
	conn := &repository.ExchangeConnection{UserUID: "user-1", Exchange: "ctrader", Label: "main"}

	write, vacated, err := s.mergeRealizedReconstruction(conn, rebuilt, stored, reconstructOpts{recurring: true})
	if err != nil || len(write) != 0 || len(vacated) != 0 {
		t.Fatalf("recurring: write=%d vacated=%d err=%v, want a quiet deferral", len(write), len(vacated), err)
	}

	if _, _, err := s.mergeRealizedReconstruction(conn, rebuilt, stored, reconstructOpts{}); err == nil {
		t.Fatal("a full run contradicted by its only measured day went through silently")
	}
}

// A day an earlier reconstruction wrote belongs to full runs: the recurring
// run must not rewrite old-dated rows piecemeal, or the day before its window
// and the first day inside it carry the same flows.
func TestMergeRealizedOnly_OnlyAFullRunRewritesReconstructedDays(t *testing.T) {
	d0, d1 := day(2026, time.May, 4), day(2026, time.May, 5)
	stored := []*repository.Snapshot{storedReconstruction(d0, 1000, 1000), storedReconstruction(d1, 1010, 0)}
	rebuilt := []*repository.Snapshot{rebuiltRow(d1, 1000, 1000)}

	if m := mergeRealizedOnly(rebuilt, stored, realizedRecurring); len(m.write) != 0 {
		t.Fatalf("the recurring run rewrote a reconstructed day: %+v", m.write)
	}
	if m := mergeRealizedOnly(rebuilt, stored, realizedFull); writtenOn(m, d1) == nil {
		t.Fatal("a full run left a reconstructed day it re-emits unwritten")
	}
}

// The recurring run writes a day only when the day before it is held. A gap
// longer than its window keeps its flows on the live row after it.
func TestMergeRealizedOnly_RecurringFillsOnlyFromAHeldDay(t *testing.T) {
	d := func(n int) time.Time { return day(2026, time.May, 1).Add(time.Duration(n) * 24 * time.Hour) }
	stored := []*repository.Snapshot{
		liveRow(d(0), 1000, 0, 0),
		liveRow(d(6), 1500, 0, 450), // after a gap: carries the flows of d(1)..d(5)
	}
	rebuilt := []*repository.Snapshot{
		rebuiltRow(d(1), 1000, 0),
		rebuiltRow(d(2), 1100, 100),
		rebuiltRow(d(4), 1300, 200),
		rebuiltRow(d(5), 1450, 150),
		rebuiltRow(d(6), 1500, 0),
	}

	m := mergeRealizedOnly(rebuilt, stored, realizedRecurring)

	for _, n := range []int{1, 2} {
		if writtenOn(m, d(n)) == nil {
			t.Errorf("d(%d) follows a held day and was not filled", n)
		}
	}
	for _, n := range []int{4, 5} {
		if writtenOn(m, d(n)) != nil {
			t.Errorf("d(%d) filled although d(3) is held by nobody: the gap's early flows would be lost", n)
		}
	}
	if got := writtenOn(m, d(6)); got != nil {
		t.Fatalf("the live row after the gap lost its flows to one day's: %+v", got)
	}
}

// Rows used to be keyed by the day that had just closed. The one that dating
// left just before the new first row holds the same flows as that first row:
// an inception deposit counted twice reads as a total loss.
func TestMergeRealizedOnly_FullRunVacatesTheOldDatingsLeftover(t *testing.T) {
	d0, d1, d2 := day(2026, time.May, 4), day(2026, time.May, 5), day(2026, time.May, 6)
	stored := []*repository.Snapshot{
		storedReconstruction(d0, 1000, 1000),
		storedReconstruction(d1, 1000, 0),
	}
	rebuilt := []*repository.Snapshot{rebuiltRow(d1, 1000, 1000), rebuiltRow(d2, 1000, 0)}

	m := mergeRealizedOnly(rebuilt, stored, realizedFull)
	if len(m.vacated) != 1 || !m.vacated[0].Equal(d0) {
		t.Fatalf("vacated %v, want [%s]", m.vacated, d0.Format(time.DateOnly))
	}

	// Never a live row, and never outside a full run.
	stored[0] = liveRow(d0, 1000, 0, 1000)
	if m := mergeRealizedOnly(rebuilt, stored, realizedFull); len(m.vacated) != 0 {
		t.Fatalf("vacated a live measurement: %v", m.vacated)
	}
	stored[0] = storedReconstruction(d0, 1000, 1000)
	if m := mergeRealizedOnly(rebuilt, stored, realizedWindow); len(m.vacated) != 0 {
		t.Fatalf("a gap repair vacated a day outside its window: %v", m.vacated)
	}
	if m := mergeRealizedOnly(rebuilt, stored, realizedRecurring); len(m.vacated) != 0 {
		t.Fatalf("the recurring run vacated a day: %v", m.vacated)
	}
}

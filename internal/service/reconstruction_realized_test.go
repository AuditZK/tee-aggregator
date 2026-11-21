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

// today sits after every day the tests below store or rebuild.
var today = day(2026, time.June, 1)

func may(n int) time.Time { return day(2026, time.May, 1).AddDate(0, 0, n) }

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
	stored := []*repository.Snapshot{liveRow(may(3), 1000, 0, 0), liveRow(may(4), 10000, 250, 10000)}
	rebuilt := []*repository.Snapshot{rebuiltRow(may(4), 10000, 9000)}

	m := mergeRealizedOnly(rebuilt, stored, today)

	got := writtenOn(m, may(4))
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
	stored := []*repository.Snapshot{liveRow(may(3), 1000, 0, 0), liveRow(may(4), 1000, 40, 0)}

	m := mergeRealizedOnly([]*repository.Snapshot{rebuiltRow(may(4), 1000, 0)}, stored, today)

	if len(m.write) != 0 {
		t.Fatalf("rewrote a day that needed nothing: %+v", m.write)
	}
}

// The connect-time sync and a manual re-sync measure mid-day. The midnight
// rebuild replaces that row, and one such day among agreeing ones does not
// condemn the reconstruction.
func TestMergeRealizedOnly_ReplacesAMidDayMeasurement(t *testing.T) {
	stored := []*repository.Snapshot{
		liveRow(may(3), 1000, 0, 0),
		liveRow(may(4), 1000, 0, 0),
		liveRow(may(5), 1180, 0, 200), // synthetic: measured at 14:00, after a deposit and a trade
	}
	rebuilt := []*repository.Snapshot{rebuiltRow(may(4), 1000, 0), rebuiltRow(may(5), 1000, 0)}

	m := mergeRealizedOnly(rebuilt, stored, today)

	if m.rejected() {
		t.Fatalf("one mid-day measurement rejected the reconstruction (agree=%d disagree=%d)", m.agree, m.disagree)
	}
	got := writtenOn(m, may(5))
	if got == nil || got.RealizedBalance != 1000 || got.Deposits != 0 {
		t.Fatalf("the mid-day row was not replaced by the midnight rebuild: %+v", got)
	}
}

// A reconstruction of another account disagrees with every measured day.
func TestMergeRealizedOnly_RejectsWhenMostMeasuredDaysDisagree(t *testing.T) {
	stored := []*repository.Snapshot{liveRow(may(3), 1000, 0, 0), liveRow(may(4), 1000, 0, 0), liveRow(may(5), 1010, 0, 0)}
	rebuilt := []*repository.Snapshot{rebuiltRow(may(4), 52000, 0), rebuiltRow(may(5), 52100, 0)}

	m := mergeRealizedOnly(rebuilt, stored, today)

	if !m.rejected() {
		t.Fatal("a reconstruction contradicting every measured day was accepted")
	}
	if m.contradicted == nil || !m.contradicted.Timestamp.Equal(may(4)) || m.rebuiltAt != 52000 {
		t.Fatalf("reported %+v / %v, want the first contradicted day", m.contradicted, m.rebuiltAt)
	}
}

// The day after a connect, the connect-time row is the only measured day and
// it was taken mid-day. The reconstruction waits for the next midnight sync
// rather than raising an alarm.
func TestMergeRealizedReconstruction_ALoneContradictionWaitsForASecondDay(t *testing.T) {
	stored := []*repository.Snapshot{storedReconstruction(may(3), 1000, 1000), liveRow(may(4), 1180, 0, 0)}
	rebuilt := []*repository.Snapshot{rebuiltRow(may(4), 1000, 0)}
	s := &SyncService{logger: zap.NewNop()}
	conn := &repository.ExchangeConnection{UserUID: "user-1", Exchange: "ctrader", Label: "main"}

	write, err := s.mergeRealizedReconstruction(conn, rebuilt, stored, reconstructOpts{}, today)
	if err != nil || len(write) != 0 {
		t.Fatalf("write=%d err=%v, want a quiet deferral", len(write), err)
	}
}

// A new connection holds nothing before today, so the backfill takes the
// whole series.
func TestMergeRealizedOnly_ANewConnectionTakesTheWholeSeries(t *testing.T) {
	stored := []*repository.Snapshot{liveRow(today, 1200, 0, 1200)}
	rebuilt := []*repository.Snapshot{rebuiltRow(may(0), 1000, 1000), rebuiltRow(may(1), 1100, 0), rebuiltRow(may(2), 1200, 0)}

	m := mergeRealizedOnly(rebuilt, stored, today)

	if len(m.write) != 3 {
		t.Fatalf("wrote %d days of a new connection's history, want 3", len(m.write))
	}
}

// A demo reset erases the ledger before it. Rebuilt from what is left, the
// account starts on the reset with the whole new balance booked as a deposit,
// while the stored days before it hold the account as it was. In production a
// full reconstruction rewrote the day after the reset with that raw deposit
// and deleted the reset day, which carried the net figure: the account showed
// a total loss on that day.
func TestMergeRealizedOnly_AResetLedgerDoesNotRewriteTheStoredHistory(t *testing.T) {
	// Synthetic: 1000 funded, traded to 1050, reset to 10000 on may(3), net 8950.
	stored := []*repository.Snapshot{
		storedReconstruction(may(0), 1000, 1000),
		storedReconstruction(may(1), 1020, 0),
		storedReconstruction(may(2), 1050, 0),
		storedReconstruction(may(3), 10000, 8950),
		storedReconstruction(may(4), 10000, 0),
		liveRow(may(5), 10000, 0, 0),
	}
	// What the broker serves now: nothing before the reset.
	rebuilt := []*repository.Snapshot{rebuiltRow(may(4), 10000, 10000), rebuiltRow(may(5), 10000, 0)}

	m := mergeRealizedOnly(rebuilt, stored, today)

	for _, w := range m.write {
		if w.Deposits != 0 {
			t.Fatalf("wrote %s with a deposit of %v over the stored history", w.Timestamp.Format(time.DateOnly), w.Deposits)
		}
	}
	if writtenOn(m, may(4)) != nil {
		t.Fatal("rewrote a day a previous reconstruction wrote")
	}
}

// A broker can also serve more than it once did. Grown backwards, the series
// moved the account's inception and added capital nobody had booked.
func TestMergeRealizedOnly_ALongerLedgerDoesNotExtendTheStoredHistory(t *testing.T) {
	stored := []*repository.Snapshot{
		storedReconstruction(may(10), 50, 50),
		storedReconstruction(may(11), 50, 0),
		liveRow(may(12), 50, 0, 0),
	}
	rebuilt := []*repository.Snapshot{rebuiltRow(may(1), 1000, 1000)}
	for n := 2; n <= 12; n++ {
		rebuilt = append(rebuilt, rebuiltRow(may(n), 50, 0))
	}

	m := mergeRealizedOnly(rebuilt, stored, today)

	for _, w := range m.write {
		if w.Timestamp.Before(may(10)) {
			t.Fatalf("extended the stored history back to %s", w.Timestamp.Format(time.DateOnly))
		}
	}
}

// A day nobody wrote is filled only when the day before it is held. A gap
// longer than the recurring window keeps its flows on the live row after it.
func TestMergeRealizedOnly_FillsOnlyFromAHeldDay(t *testing.T) {
	stored := []*repository.Snapshot{
		liveRow(may(0), 1000, 0, 0),
		liveRow(may(6), 1500, 0, 450), // after a gap: carries the flows of may(1)..may(5)
	}
	rebuilt := []*repository.Snapshot{
		rebuiltRow(may(1), 1000, 0),
		rebuiltRow(may(2), 1100, 100),
		rebuiltRow(may(4), 1300, 200),
		rebuiltRow(may(5), 1450, 150),
		rebuiltRow(may(6), 1500, 0),
	}

	m := mergeRealizedOnly(rebuilt, stored, today)

	for _, n := range []int{1, 2} {
		if writtenOn(m, may(n)) == nil {
			t.Errorf("may(%d) follows a held day and was not filled", n)
		}
	}
	for _, n := range []int{4, 5} {
		if writtenOn(m, may(n)) != nil {
			t.Errorf("may(%d) filled although may(3) is held by nobody: the gap's early flows would be lost", n)
		}
	}
	if got := writtenOn(m, may(6)); got != nil {
		t.Fatalf("the live row after the gap lost its flows to one day's: %+v", got)
	}
}

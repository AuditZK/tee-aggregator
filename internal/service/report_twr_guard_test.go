package service

import (
	"math"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func twrDay(d string, equity, deposits, withdrawals float64) *repository.Snapshot {
	t, _ := time.Parse("2006-01-02", d)
	return &repository.Snapshot{Timestamp: t, Exchange: "bybit", Label: "acct", TotalEquity: equity, Deposits: deposits, Withdrawals: withdrawals}
}

func twrByDate(t *testing.T, snaps []*repository.Snapshot) map[string]dailyReturn {
	t.Helper()
	out := map[string]dailyReturn{}
	for _, r := range convertSnapshotsToDailyReturns(snaps) {
		out[r.date] = r
	}
	return out
}

func near(a, b, tol float64) bool { return math.Abs(a-b) <= tol }

// A stablecoin-dust balance projected back before the first funding, then
// the real deposit: the funding day is a baseline reset, not a +10^8 % day,
// and the following ordinary day is scored on the funded base.
func TestTWR_DustBaseThenFundingIsNotScored(t *testing.T) {
	snaps := []*repository.Snapshot{
		twrDay("2026-06-18", 0.00009689678233215773, 0, 0),
		twrDay("2026-06-19", 0.00009689679133216486, 0, 0),
		twrDay("2026-06-20", 11115.83587806676, 11000, 0), // +115.83 of PnL on the funding day
		twrDay("2026-06-21", 11122.17920103673, 0, 0),
	}
	r := twrByDate(t, snaps)
	if r["2026-06-20"].netReturn != 0 {
		t.Fatalf("funding day on a dust base must score 0, got %v", r["2026-06-20"].netReturn)
	}
	want := (11122.17920103673 - 11115.83587806676) / 11115.83587806676
	if !near(r["2026-06-21"].netReturn, want, 1e-12) {
		t.Fatalf("day after funding: got %v want %v", r["2026-06-21"].netReturn, want)
	}
	if !near(r["2026-06-21"].cumulativeReturn, want, 1e-12) {
		t.Fatalf("cumulative must not carry the funding day: got %v want %v", r["2026-06-21"].cumulativeReturn, want)
	}
}

// Days with no cash flow and a material base are unchanged by the guards.
func TestTWR_OrdinaryDayUnchanged(t *testing.T) {
	snaps := []*repository.Snapshot{
		twrDay("2026-01-01", 100, 0, 0),
		twrDay("2026-01-02", 110, 0, 0),
		twrDay("2026-01-03", 99, 0, 0),
	}
	r := twrByDate(t, snaps)
	if !near(r["2026-01-02"].netReturn, 0.10, 1e-12) || !near(r["2026-01-03"].netReturn, -0.10, 1e-12) {
		t.Fatalf("ordinary days: got %v and %v", r["2026-01-02"].netReturn, r["2026-01-03"].netReturn)
	}
	if !near(r["2026-01-03"].cumulativeReturn, -0.01, 1e-12) {
		t.Fatalf("cumulative: got %v want -0.01", r["2026-01-03"].cumulativeReturn)
	}
}

// Modified Dietz: a same-day deposit is at risk that day and belongs in the
// base. Deposit 67 onto 201 and close at 100 is -168/268 = -62.7%, not
// -168/201 = -83.6%; the open-only base can even print losses below -100%.
func TestTWR_DepositIsInTheBase(t *testing.T) {
	snaps := []*repository.Snapshot{
		twrDay("2026-01-01", 201, 0, 0),
		twrDay("2026-01-02", 100, 67, 0),
	}
	r := twrByDate(t, snaps)
	if !near(r["2026-01-02"].netReturn, -168.0/268.0, 1e-12) {
		t.Fatalf("loss with same-day deposit: got %v want %v", r["2026-01-02"].netReturn, -168.0/268.0)
	}
	// And a normal deposit day: 1000 open, +500 deposit, close 1530 → +30 on 1500.
	snaps = []*repository.Snapshot{
		twrDay("2026-01-01", 1000, 0, 0),
		twrDay("2026-01-02", 1530, 500, 0),
	}
	r = twrByDate(t, snaps)
	if !near(r["2026-01-02"].netReturn, 30.0/1500.0, 1e-12) {
		t.Fatalf("deposit day: got %v want %v", r["2026-01-02"].netReturn, 30.0/1500.0)
	}
}

// Gross flows dwarfing the base cannot be timed at daily granularity: not scored.
func TestTWR_FlowDominatedDayIsNotScored(t *testing.T) {
	snaps := []*repository.Snapshot{
		twrDay("2026-01-01", 2.5, 0, 0),
		twrDay("2026-01-02", 60, 62.9, 0),
	}
	r := twrByDate(t, snaps)
	if r["2026-01-02"].netReturn != 0 {
		t.Fatalf("flow-dominated day must score 0, got %v", r["2026-01-02"].netReturn)
	}
}

// A net inflow that accounts for the whole close is an account (re)start.
func TestTWR_InflowCoveringTheCloseIsAReset(t *testing.T) {
	snaps := []*repository.Snapshot{
		twrDay("2026-01-01", 100, 0, 0),
		twrDay("2026-01-02", 100, 0, 100), // withdraw everything
		twrDay("2026-01-03", 5000, 5000, 0), // refund: whole close is the deposit
		twrDay("2026-01-04", 5100, 0, 0),
	}
	r := twrByDate(t, snaps)
	if r["2026-01-03"].netReturn != 0 {
		t.Fatalf("refund day must be a reset, got %v", r["2026-01-03"].netReturn)
	}
	if !near(r["2026-01-04"].netReturn, 0.02, 1e-12) {
		t.Fatalf("day after refund: got %v want 0.02", r["2026-01-04"].netReturn)
	}
}

// Withdrawals are removed from the numerator but do not enter the base.
func TestTWR_WithdrawalDay(t *testing.T) {
	snaps := []*repository.Snapshot{
		twrDay("2026-01-01", 46474.46, 0, 0),
		twrDay("2026-01-02", 26474.46, 0, 20000),
	}
	r := twrByDate(t, snaps)
	if !near(r["2026-01-02"].netReturn, 0, 1e-12) {
		t.Fatalf("pure withdrawal day: got %v want 0", r["2026-01-02"].netReturn)
	}
}

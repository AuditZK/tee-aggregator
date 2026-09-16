package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func snap(ts time.Time, equity, deposits float64) *repository.Snapshot {
	return &repository.Snapshot{Timestamp: ts, TotalEquity: equity, Deposits: deposits}
}

// The bug this exists for, reproduced at the unit level. The connect hook runs
// the live sync first (it writes the equity anchor the rebuild dispatch needs),
// so the connection's first snapshot is TODAY, carrying an inception deposit
// worth the whole balance. The reconstruction then lands 79 earlier days and
// stamps the real inception underneath. One Bybit account came out reporting
// close to twice the capital ever deposited, halving its return.
func TestSupersededConnectStamp_ClearsTheConnectTimeStamp(t *testing.T) {
	live := snap(day(2026, time.August, 26), 10093.99, 10093.99)
	got := supersededConnectStamp([]*repository.Snapshot{live}, day(2026, time.June, 8))
	if got == nil {
		t.Fatal("the connect-time stamp was left in place, the account still double-counts its capital")
	}
	if got.Deposits != 0 {
		t.Fatalf("deposits = %v, want 0", got.Deposits)
	}
	if !got.Timestamp.Equal(live.Timestamp) {
		t.Fatalf("cleared the wrong day: %s", got.Timestamp)
	}
	// The caller upserts the returned row; everything but the deposit must
	// survive that round trip.
	if got.TotalEquity != live.TotalEquity {
		t.Fatalf("equity = %v, want %v — the upsert would overwrite the day with a wrong balance", got.TotalEquity, live.TotalEquity)
	}
	if live.Deposits == 0 {
		t.Fatal("the original row was mutated in place; the batch must carry a copy")
	}
}

// IBKR re-emits its whole Flex window on every sync, so a connection with real
// live history reaching back is a different situation entirely — a widened Flex
// window leaves its own stale stamp, which this must not try to guess at.
func TestSupersededConnectStamp_LeavesLongerHistoryAlone(t *testing.T) {
	existing := []*repository.Snapshot{
		snap(day(2026, time.August, 24), 1000, 1000),
		snap(day(2026, time.August, 25), 1000, 0),
		snap(day(2026, time.August, 26), 1000, 0),
	}
	if got := supersededConnectStamp(existing, day(2026, time.June, 8)); got != nil {
		t.Fatalf("cleared a deposit on a connection with real live history: %+v", got)
	}
}

// A row predating the reconstruction is not a connect-time stamp, and clearing
// it would erase a genuine older deposit.
func TestSupersededConnectStamp_IgnoresRowsBeforeInception(t *testing.T) {
	older := snap(day(2026, time.May, 1), 500, 500)
	if got := supersededConnectStamp([]*repository.Snapshot{older}, day(2026, time.June, 8)); got != nil {
		t.Fatal("cleared a deposit dated before the reconstructed inception")
	}
	sameDay := snap(day(2026, time.June, 8), 500, 500)
	if got := supersededConnectStamp([]*repository.Snapshot{sameDay}, day(2026, time.June, 8)); got != nil {
		t.Fatal("cleared the inception day itself")
	}
}

// Only a deposit worth the day's entire equity reads as a stamp. A partial
// transfer is a real cash flow and erasing it would invent a gain.
func TestSupersededConnectStamp_KeepsRealTransfers(t *testing.T) {
	partial := snap(day(2026, time.August, 26), 10000, 2500)
	if got := supersededConnectStamp([]*repository.Snapshot{partial}, day(2026, time.June, 8)); got != nil {
		t.Fatalf("erased a real 2500 deposit: %+v", got)
	}
	none := snap(day(2026, time.August, 26), 10000, 0)
	if got := supersededConnectStamp([]*repository.Snapshot{none}, day(2026, time.June, 8)); got != nil {
		t.Fatal("returned a row that carried no deposit to begin with")
	}
}

// Float equality cannot be exact across the equity/deposit round trip, so the
// match is bounded rather than strict.
func TestSupersededConnectStamp_ToleratesFloatDrift(t *testing.T) {
	drifted := snap(day(2026, time.August, 26), 10093.99233949, 10093.992339490001)
	if got := supersededConnectStamp([]*repository.Snapshot{drifted}, day(2026, time.June, 8)); got == nil {
		t.Fatal("a stamp missed over floating-point noise")
	}
	wayOff := snap(day(2026, time.August, 26), 10093.99, 10093.50)
	if got := supersededConnectStamp([]*repository.Snapshot{wayOff}, day(2026, time.June, 8)); got != nil {
		t.Fatal("matched a deposit that is nowhere near the balance")
	}
}

func TestSupersededConnectStamp_NoExistingHistory(t *testing.T) {
	if got := supersededConnectStamp(nil, day(2026, time.June, 8)); got != nil {
		t.Fatal("invented a correction for a connection with no prior snapshots")
	}
}

// The external rebuilder dates the inception deposit off the ledger, so the
// reconstructed series arrives with its earliest day already stamped. The
// connect-time stamp it lands under must still be cleared — coupling the two
// left it in place on a second Bybit account: the live sync wrote today's row
// as a first sync (deposit = whole balance), the rebuild filled 258 days
// beneath it a minute later, and the aggregate read the day as -31%.
func TestResolveInception_ClearsStampUnderALedgerDatedSeries(t *testing.T) {
	earliest := snap(day(2025, time.December, 14), 107.27, 107.27)
	live := snap(day(2026, time.August, 29), 8139.34, 8139.34)

	stamp, superseded := resolveInception(earliest, []*repository.Snapshot{live})
	if stamp {
		t.Fatal("re-stamped an earliest day that already carries its ledger deposit")
	}
	if superseded == nil {
		t.Fatal("the connect-time stamp survived beneath a ledger-dated series")
	}
	if superseded.Deposits != 0 || !superseded.Timestamp.Equal(live.Timestamp) {
		t.Fatalf("cleared the wrong row: %+v", superseded)
	}
}

// The original path: a series with no deposit on its earliest day gets the
// stamp AND clears the connect-time row.
func TestResolveInception_StampsAndClearsWhenSeriesCarriesNoDeposit(t *testing.T) {
	earliest := snap(day(2026, time.June, 8), 43, 0)
	live := snap(day(2026, time.August, 26), 10093.99, 10093.99)

	stamp, superseded := resolveInception(earliest, []*repository.Snapshot{live})
	if !stamp {
		t.Fatal("an unstamped inception day was left without its deposit")
	}
	if superseded == nil {
		t.Fatal("the connect-time stamp survived")
	}
}

// Rows older than the series mean the batch is a window, not the inception:
// neither stamp nor clearing, whatever the earliest day carries.
func TestResolveInception_OlderHistoryDisablesBoth(t *testing.T) {
	older := snap(day(2026, time.May, 1), 500, 500)
	for _, earliest := range []*repository.Snapshot{
		snap(day(2026, time.June, 8), 43, 0),
		snap(day(2026, time.June, 8), 43, 43),
	} {
		stamp, superseded := resolveInception(earliest, []*repository.Snapshot{older})
		if stamp || superseded != nil {
			t.Fatalf("acted on a window inside longer history: stamp=%v superseded=%+v", stamp, superseded)
		}
	}
}

// The defect one OKX account exposed on 2026-08-30. The rebuild opened
// on zero-equity padding, so the earliest day of the batch carried no equity;
// the rule bailed on it and never reached the clearing, leaving the whole
// balance booked as a deposit on connection day. Whichever end the padding is
// read from, the funded day is the one that counts.
func TestEarliestFundedDay_SkipsTheZeroPadding(t *testing.T) {
	funded := snap(day(2026, time.August, 21), 3310.97, 0)
	batch := []*repository.Snapshot{
		snap(day(2026, time.June, 3), 0, 0),
		snap(day(2026, time.June, 4), 0, 0),
		funded,
		snap(day(2026, time.August, 22), 3551.59, 0),
	}

	got := earliestFundedDay(batch)

	if got == nil {
		t.Fatal("no funded day found — the inception rule abandons a zero-padded series")
	}
	if !got.Timestamp.Equal(funded.Timestamp) {
		t.Fatalf("earliest funded day = %s, want %s", got.Timestamp.Format("2006-01-02"), funded.Timestamp.Format("2006-01-02"))
	}
}

func TestEarliestFundedDay_NothingFundedIsNil(t *testing.T) {
	batch := []*repository.Snapshot{
		snap(day(2026, time.June, 3), 0, 0),
		snap(day(2026, time.June, 4), 0, 0),
	}
	if got := earliestFundedDay(batch); got != nil {
		t.Fatalf("got %+v, want nil on an entirely empty series", got)
	}
}

func TestEarliestFundedDay_EmptyBatchIsNil(t *testing.T) {
	if got := earliestFundedDay(nil); got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}

// End to end over the pure half of the rule: a zero-padded reconstruction
// landing under a connect-time stamp must clear it. This is the $3,569.14
// phantom deposit, reproduced.
func TestZeroPaddedReconstruction_ClearsTheConnectStamp(t *testing.T) {
	live := snap(day(2026, time.August, 30), 3569.14, 3569.14)
	batch := []*repository.Snapshot{
		snap(day(2026, time.June, 3), 0, 0),
		snap(day(2026, time.August, 21), 3310.97, 0),
		snap(day(2026, time.August, 29), 3529.17, 0),
	}

	earliest := earliestFundedDay(batch)
	if earliest == nil {
		t.Fatal("the padded series yielded no inception day")
	}
	stamp, superseded := resolveInception(earliest, []*repository.Snapshot{live})

	if !stamp {
		t.Error("the real inception day was left unstamped, the account reports its opening balance as pure gain")
	}
	if superseded == nil {
		t.Fatal("the connect-time stamp survived — the account still reports a $3,569.14 deposit it never received")
	}
	if superseded.Deposits != 0 {
		t.Fatalf("cleared deposits = %v, want 0", superseded.Deposits)
	}
	if !superseded.Timestamp.Equal(live.Timestamp) {
		t.Fatalf("cleared the wrong day: %s", superseded.Timestamp.Format("2006-01-02"))
	}
}

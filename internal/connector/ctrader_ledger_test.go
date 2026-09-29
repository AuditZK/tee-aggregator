package connector

import (
	"context"
	"math"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func ptrInt64(v int64) *int64 { return &v }

// E-M5: the demo-reset heuristic compares a deposit's claimed prior balance
// against the balance we reconstructed. That reconstruction used to advance
// only on deposits and withdrawals, ignoring the swaps, commissions, rebates
// and dividends that also move the balance and also report it. Drift far
// enough and a GENUINE deposit is rewritten as a reset — real capital erased
// from the curve, and the account shows a phantom loss.
func TestCTraderCashflowsByDay_LedgerChargesAdvanceTheRunningBalance(t *testing.T) {
	day := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	ms := func(h int) int64 { return day.Add(time.Duration(h) * time.Hour).UnixMilli() }

	cashflows := []ctraderDepositWithdraw{
		// 10:00 — inception deposit of 1000 onto an empty account.
		{OperationType: ctraderOpDeposit, Delta: 100000, Balance: 100000, Timestamp: ms(10), MoneyDigits: 2},
		// 11:00 — a 900 charge (op 21, a swap): not a cash flow, but it
		// reports the balance after, 100. This is the entry that used to be
		// skipped, leaving `running` stuck at 1000.
		{OperationType: 21, Delta: -90000, Balance: 10000, Timestamp: ms(11), MoneyDigits: 2},
		// 12:00 — a real 50 deposit. balanceAfter 150, delta 50, so the
		// ledger's implied prior balance is 100 — which matches the true,
		// charge-adjusted balance. Against the stale 1000 it looks like a
		// zero-then-fund reset (100 < 0.5 * 1000) and the heuristic rewrites
		// it as a 850 WITHDRAWAL.
		{OperationType: ctraderOpDeposit, Delta: 5000, Balance: 15000, Timestamp: ms(12), MoneyDigits: 2},
	}

	byDay := ctraderCashflowsByDay(nil, cashflows)
	got := byDay[day.Format("20060102")]
	if got == nil {
		t.Fatal("no cashflow bucket for the day")
	}
	if want := 1050.0; math.Abs(got.deposits-want) > 1e-9 {
		t.Fatalf("deposits = %v, want %v — the second deposit was rewritten as a reset", got.deposits, want)
	}
	if got.withdrawals != 0 {
		t.Fatalf("withdrawals = %v, want 0 — a genuine deposit was booked as a withdrawal", got.withdrawals)
	}
}

// A genuine demo reset is still caught: the ledger claims an empty prior
// account while the balance curve says otherwise.
func TestCTraderCashflowsByDay_StillCorrectsADemoReset(t *testing.T) {
	day := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	ms := func(h int) int64 { return day.Add(time.Duration(h) * time.Hour).UnixMilli() }

	deals := []cTraderDeal{{
		ExecutionTimestamp: ms(9),
		MoneyDigits:        2,
		ClosePositionDetail: &struct {
			GrossProfit int64 `json:"grossProfit"`
			Commission  int64 `json:"commission"`
			Swap        int64 `json:"swap"`
			// A POINTER so an absent field and a genuine zero stay
			// distinguishable: the old `Balance == 0` filter dropped every
			// deal that closed the account at exactly 0.00, and the
			// carry-forward builder then drew a flat line at the last
			// positive balance — a blown account rendered as a plateau (E-B).
			Balance     *int64 `json:"balance"`
			MoneyDigits int    `json:"moneyDigits"`
		}{Balance: ptrInt64(500000), MoneyDigits: 2}, // account holds 5000
	}}
	// The reset re-funds 1000 and claims it landed on an empty account.
	cashflows := []ctraderDepositWithdraw{
		{OperationType: ctraderOpDeposit, Delta: 100000, Balance: 100000, Timestamp: ms(10), MoneyDigits: 2},
	}

	byDay := ctraderCashflowsByDay(deals, cashflows)
	got := byDay[day.Format("20060102")]
	if got == nil {
		t.Fatal("no cashflow bucket for the day")
	}
	// Net capital change is 1000 - 5000 = -4000, i.e. a withdrawal.
	if want := 4000.0; math.Abs(got.withdrawals-want) > 1e-9 {
		t.Fatalf("withdrawals = %v, want %v", got.withdrawals, want)
	}
	if got.deposits != 0 {
		t.Fatalf("deposits = %v, want 0 — the discarded balance was counted as fresh capital", got.deposits)
	}
}

// Money that reaches the account without it having traded for it is capital.
// Only deposits and withdrawals used to count, so a transfer from the client's
// other account on the same server read as profit.
func TestCTraderCashflowAmount_ClassifiesEveryCapitalOperation(t *testing.T) {
	capital := map[int]float64{
		ctraderOpDeposit:  +10,
		ctraderOpWithdraw: -10,
		36:                +10, // transfer in from another account on the server
		37:                -10, // transfer out
		30:                -10, // withdrawal to create a cTrader Copy subaccount
		31:                +10, // deposit into it
		32:                -10,
		33:                +10,
		5:                 +10, // IB commissions
		3:                 +10, // mirroring commission received from copiers
		29:                +10, // performance fee received from copiers
		38:                +10, // bonus converted into real balance
	}
	for op, want := range capital {
		// The sign comes from the operation, whatever sign the delta carries.
		for _, delta := range []int64{1000, -1000} {
			got, ok := ctraderCashflowAmount(ctraderDepositWithdraw{OperationType: op, Delta: delta, MoneyDigits: 2})
			if !ok || got != want {
				t.Errorf("op %d delta %d: got (%v, %v), want (%v, true)", op, delta, got, ok, want)
			}
		}
	}

	trading := []int{
		4, 34, // fees a copier pays
		9, 10, // volume rebates
		15, 16, // dividends
		17, 18, // GSL and rollover charges
		19, 20, // non-withdrawable bonus, kept outside balance
		21, 22, // swaps
		35, // inactivity fee
		39, // negative balance protection floors a loss at zero
	}
	for _, op := range trading {
		if got, ok := ctraderCashflowAmount(ctraderDepositWithdraw{OperationType: op, Delta: 1000, MoneyDigits: 2}); ok {
			t.Errorf("op %d counted as capital (%v); it is a result of trading", op, got)
		}
	}
}

// A transfer in is capital on the day it lands and the equity curve steps up
// with it; before, it entered no row's flows and read as profit.
func TestBuildCTraderHistoricalSnapshots_TransferInIsCapital(t *testing.T) {
	day := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	at := func(d, h int) int64 {
		return day.Add(time.Duration(d)*24*time.Hour + time.Duration(h)*time.Hour).UnixMilli()
	}

	cashflows := []ctraderDepositWithdraw{
		{OperationType: ctraderOpDeposit, Delta: 100000, Balance: 100000, Timestamp: at(0, 9), MoneyDigits: 2},
		{OperationType: 36, Delta: 50000, Balance: 150000, Timestamp: at(1, 9), MoneyDigits: 2},
	}
	snaps := buildCTraderHistoricalSnapshots(nil, cashflows, day.Add(4*24*time.Hour))

	byDay := map[string]*HistoricalSnapshot{}
	for _, s := range snaps {
		byDay[s.Date.Format("20060102")] = s
	}
	row := byDay[day.Add(2*24*time.Hour).Format("20060102")]
	if row == nil || row.Deposits != 500 || row.TotalEquity != 1500 {
		t.Fatalf("row after the transfer: want 500 deposited and equity 1500, got %+v", row)
	}
}

// Every ledger entry reports the balance after it. The curve used to follow
// only deposits and withdrawals, so a dividend or a charge moved the equity
// only at the next closing deal, days later.
func TestBuildCTraderHistoricalSnapshots_EveryLedgerEntryMovesTheCurve(t *testing.T) {
	day := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	at := func(d, h int) int64 {
		return day.Add(time.Duration(d)*24*time.Hour + time.Duration(h)*time.Hour).UnixMilli()
	}

	cashflows := []ctraderDepositWithdraw{
		{OperationType: ctraderOpDeposit, Delta: 100000, Balance: 100000, Timestamp: at(0, 9), MoneyDigits: 2},
		{OperationType: 15, Delta: 2500, Balance: 102500, Timestamp: at(1, 9), MoneyDigits: 2},  // dividend
		{OperationType: 35, Delta: -1000, Balance: 101500, Timestamp: at(2, 9), MoneyDigits: 2}, // inactivity fee
	}
	snaps := buildCTraderHistoricalSnapshots(nil, cashflows, day.Add(4*24*time.Hour))

	want := map[string]float64{
		day.Add(1 * 24 * time.Hour).Format("20060102"): 1000,
		day.Add(2 * 24 * time.Hour).Format("20060102"): 1025,
		day.Add(3 * 24 * time.Hour).Format("20060102"): 1015,
	}
	if len(snaps) != len(want) {
		t.Fatalf("got %d rows, want %d", len(snaps), len(want))
	}
	for _, s := range snaps {
		key := s.Date.Format("20060102")
		if !floatNear(s.TotalEquity, want[key], 1e-9) {
			t.Errorf("%s: equity %v, want %v", key, s.TotalEquity, want[key])
		}
		if s.Deposits != 0 && key != day.Add(24*time.Hour).Format("20060102") {
			t.Errorf("%s: a dividend or a fee was booked as capital (%v)", key, s.Deposits)
		}
	}
}

// E-M8: an unresolvable symbol used to cost one request per deal, hundreds per
// sync, all of them feeding the per-payload-type rate limit that then blocks
// the history walk.
func TestCTraderGetSymbolName_MemoizesTheMiss(t *testing.T) {
	var symbolCalls atomic.Int32

	server := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadAccountAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAccountAuthRes, map[string]any{})
		case ctraderPayloadSymbolByIDReq:
			symbolCalls.Add(1)
			sendWSError(t, conn, msg.ClientMsgID, "SYMBOL_NOT_FOUND", "unknown symbol")
		default:
			t.Errorf("unexpected payloadType: %d", msg.PayloadType)
		}
	})
	defer server.Close()

	c := &CTrader{
		clientID:     "client-id",
		clientSecret: "client-secret",
		accessToken:  "token",
		isLive:       true,
		wsLiveURL:    toWSURL(server.URL),
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}

	for i := 0; i < 5; i++ {
		if got := c.getSymbolName(context.Background(), 42, 12345); got != "SYMBOL_42" {
			t.Fatalf("got %q, want SYMBOL_42", got)
		}
	}
	if got := symbolCalls.Load(); got != 1 {
		t.Fatalf("resolved the same unknown symbol %d times, want 1", got)
	}
}

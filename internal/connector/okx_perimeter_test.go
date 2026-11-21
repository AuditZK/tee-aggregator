package connector

import (
	"testing"
	"time"
)

func okxInstant(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

// One real account's ledger, kept to the amounts and instants OKX printed: the
// pairing turns on the two legs agreeing to the last decimal, so rounding the
// fixture would lose the property under test.
func okxTwoWalletLedger() (trading, funding []okxBill) {
	trading = []okxBill{
		{BillID: "t1", T: okxInstant("2026-09-02T13:49:44.777Z"), Ccy: "BTC", BalChg: -0.045336056485, Type: "1"},
		{BillID: "t2", T: okxInstant("2026-09-03T07:00:34.383Z"), Ccy: "BTC", BalChg: 0.045336056485, Type: "1"},
	}
	funding = []okxBill{
		{BillID: "f1", T: okxInstant("2026-07-20T17:52:41Z"), Ccy: "USDT", BalChg: 150, Type: "1"},
		{BillID: "f2", T: okxInstant("2026-07-20T17:52:41Z"), Ccy: "USDT", BalChg: -150, Type: "131"},
		{BillID: "f3", T: okxInstant("2026-08-16T23:25:24Z"), Ccy: "USDT", BalChg: 300, Type: "130"},
		{BillID: "f4", T: okxInstant("2026-08-16T23:25:24Z"), Ccy: "USDT", BalChg: -300, Type: "20"},
		{BillID: "f5", T: okxInstant("2026-09-02T13:49:41Z"), Ccy: "USDT", BalChg: 0, Type: "327"},
		{BillID: "f6", T: okxInstant("2026-09-02T13:49:45Z"), Ccy: "BTC", BalChg: 0.045336056485, Type: "130"},
		{BillID: "f7", T: okxInstant("2026-09-03T07:00:34Z"), Ccy: "BTC", BalChg: -0.045336056485, Type: "131"},
	}
	return trading, funding
}

// The defect: the same coins leave the trading account and come back, and the
// account is charged a withdrawal and credited a deposit for it. Whatever the
// holding did in between then reads as capital rather than performance.
func TestOKXClassify_RoundTripThroughFundingIsNotACashflow(t *testing.T) {
	trading, funding := okxTwoWalletLedger()
	flows, _ := okxClassifyCashflows(trading, funding, map[string]float64{"BTC": 81358.7})

	for _, f := range flows {
		if f.Currency == "BTC" {
			t.Errorf("a move between two wallets we hold booked %v", f.Amount)
		}
	}
}

// The deposit lands on-chain in funding and is forwarded to trading a second
// later. Counted twice it doubles the capital; counted not at all it becomes
// performance.
func TestOKXClassify_DepositCountsOnceOnItsWayThrough(t *testing.T) {
	trading, funding := okxTwoWalletLedger()
	flows, _ := okxClassifyCashflows(trading, funding, nil)

	var deposits int
	var total float64
	for _, f := range flows {
		if f.Amount > 0 {
			deposits++
			total += f.Amount
		}
	}
	if deposits != 1 || total != 150 {
		t.Fatalf("got %d deposits totalling %v, want one of 150: %+v", deposits, total, flows)
	}
}

// A sub-account is a separate connection with its own key, so money sent there
// has left. The customer's own arithmetic depends on it.
func TestOKXClassify_SubAccountTransferStillLeaves(t *testing.T) {
	trading, funding := okxTwoWalletLedger()
	flows, _ := okxClassifyCashflows(trading, funding, nil)

	var out float64
	for _, f := range flows {
		if f.Amount < 0 {
			out += f.Amount
		}
	}
	if out != -300 {
		t.Fatalf("withdrawals total %v, want -300 (the sub-account transfer, once)", out)
	}
}

// A lone trading transfer crossed the perimeter — to a sub-account, or to
// another user — and keeps booking.
func TestOKXClassify_LoneTradingTransferStillBooks(t *testing.T) {
	trading := []okxBill{
		{BillID: "t1", T: okxInstant("2026-09-02T13:49:44Z"), Ccy: "USDT", BalChg: -500, Type: "1"},
	}
	flows, _ := okxClassifyCashflows(trading, nil, nil)
	if len(flows) != 1 || flows[0].Amount != -500 {
		t.Fatalf("got %+v, want one withdrawal of 500", flows)
	}
}

// The live rule and the rebuilder's must agree day by day, or the gate that
// holds one against the other reads two rules as a corrupted rebuild. This is
// the live half of that contract: internal is 130/131 and nothing else.
func TestOKXClassify_UnknownFundingTypeBooksAsACrossing(t *testing.T) {
	funding := []okxBill{
		{BillID: "f1", T: okxInstant("2026-09-02T10:00:00Z"), Ccy: "USDT", BalChg: 42, Type: "999"},
	}
	flows, _ := okxClassifyCashflows(nil, funding, nil)
	if len(flows) != 1 || flows[0].Amount != 42 {
		t.Fatalf("got %+v, want the unrecognised type booked as an inflow", flows)
	}
}

func TestOKXClassify_FlowsComeBackChronological(t *testing.T) {
	trading, funding := okxTwoWalletLedger()
	flows, _ := okxClassifyCashflows(trading, funding, map[string]float64{"BTC": 81358.7})
	for i := 1; i < len(flows); i++ {
		if flows[i].Timestamp.Before(flows[i-1].Timestamp) {
			t.Fatalf("flows are out of order: %v then %v", flows[i-1].Timestamp, flows[i].Timestamp)
		}
	}
}

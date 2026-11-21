package connector

import (
	"context"
	"testing"
	"time"
)

// Money sent to a trading bot leaves the equity this connector reads, and its
// return comes back with the bot's result. Both are flows: absorbed, the day
// the bot starts reads as a loss of everything it took.
func TestOKXGetCashflows_StrategyTransferIsAFlow(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[` +
			billRow("b2", msAgo(1*time.Hour), "USDT", "54", "12", "200") + `,` +
			billRow("b1", msAgo(3*time.Hour), "USDT", "-50", "12", "202") +
			`]}`,
	}, nil)

	conn := s.connector()
	flows, err := conn.GetCashflows(context.Background(), time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 2 || flows[0].Amount != -50 || flows[1].Amount != 54 {
		t.Fatalf("flows = %+v, want -50 then +54", flows)
	}
	if warns := conn.CapabilityWarnings(); len(warns) != 0 {
		t.Fatalf("warnings = %v, want none for a known type", warns)
	}
}

// Interest the funding wallet earns is the account's yield, not money put in.
func TestOKXGetCashflows_FundingInterestIsNotADeposit(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxAssetBillsPath: `{"code":"0","data":[` +
			`{"billId":"f1","ts":"` + msAgo(2*time.Hour) + `","ccy":"USDT","balChg":"0.01","type":"400","subType":""}` +
			`]}`,
	}, nil)

	flows, err := s.connector().GetCashflows(context.Background(), time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 0 {
		t.Fatalf("flows = %+v, want none for auto lend interest", flows)
	}
}

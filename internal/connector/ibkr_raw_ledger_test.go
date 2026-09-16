package connector

import (
	"testing"
	"time"
)

// A position transfer carries no cash, so GetCashflows drops it on purpose and
// its market value reaches equity looking like a trading gain. The raw ledger
// exists to show that entry; a filter that hides it here would leave the
// phantom-performance case unobservable.
func TestParseRawLedgerFromReport_KeepsCashlessTransfer(t *testing.T) {
	const report = `<FlexQueryResponse>
  <FlexStatements>
    <FlexStatement accountId="U1234567">
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="400000" currency="USD" dateTime="20260720;120000"/>
        <CashTransaction type="Broker Interest Received" amount="143.21" currency="USD" dateTime="20260803;235959"/>
      </CashTransactions>
      <Transfers>
        <Transfer type="ACATS" direction="IN" cashTransfer="0" positionAmount="578062.03" quantity="4210" assetCategory="STK" symbol="XYZ" currency="USD" dateTime="20260730;140000"/>
        <Transfer type="INTERNAL" direction="OUT" cashTransfer="-549000" currency="USD" date="20260828"/>
      </Transfers>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`

	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	ops, err := parseRawLedgerFromReport([]byte(report), since)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(ops) != 4 {
		t.Fatalf("got %d entries, want 4 — the raw ledger must not filter: %+v", len(ops), ops)
	}

	var transfer *RawBalanceOp
	for i := range ops {
		if ops[i].Label == "transfer:ACATS/IN/STK" {
			transfer = &ops[i]
		}
	}
	if transfer == nil {
		t.Fatalf("the cashless position transfer is absent — the entry this view exists for: %+v", ops)
	}
	if transfer.Delta != 0 {
		t.Errorf("cash delta = %v, want 0: a position transfer moves no cash", transfer.Delta)
	}
	if transfer.PositionValue != 578062.03 {
		t.Errorf("position value = %v, want 578062.03 — the amount that reaches equity", transfer.PositionValue)
	}
	if transfer.Symbol != "XYZ" {
		t.Errorf("symbol = %q, want XYZ", transfer.Symbol)
	}

	// Performance types are excluded from cashflows by design; the raw view
	// keeps them, otherwise an interest drip cannot be told from a deposit.
	var sawInterest bool
	for _, op := range ops {
		if op.Label == "cash:Broker Interest Received" {
			sawInterest = true
		}
	}
	if !sawInterest {
		t.Error("interest entry dropped — the raw ledger must carry every type")
	}

	for i := 1; i < len(ops); i++ {
		if ops[i].Timestamp.Before(ops[i-1].Timestamp) {
			t.Fatalf("entries are not chronological: %v then %v", ops[i-1].Timestamp, ops[i].Timestamp)
		}
	}
}

func TestParseRawLedgerFromReport_HonoursSince(t *testing.T) {
	const report = `<FlexQueryResponse>
  <FlexStatements>
    <FlexStatement accountId="U1234567">
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="100" currency="USD" dateTime="20260715;120000"/>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="125000" currency="USD" dateTime="20260724;120000"/>
      </CashTransactions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`

	ops, err := parseRawLedgerFromReport([]byte(report), time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(ops) != 1 || ops[0].Delta != 125000 {
		t.Fatalf("since filter ignored: %+v", ops)
	}
}

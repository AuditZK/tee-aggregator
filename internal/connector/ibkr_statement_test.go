package connector

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// A master account and its sub-account in one report: the master holds no
// equity but carries its own copy of the funding transfer.
const multiAccountReport = `<FlexQueryResponse queryName="q" type="AF">
  <FlexStatements count="2">
    <FlexStatement accountId="F0000001" fromDate="20260601" toDate="20260602">
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="20260601" total="0" cash="0"/>
        <EquitySummaryByReportDateInBase reportDate="20260602" total="0" cash="0"/>
      </EquitySummaryInBase>
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="2500" currency="USD" dateTime="20260602;120000"/>
      </CashTransactions>
    </FlexStatement>
    <FlexStatement accountId="DU0000002" fromDate="20260601" toDate="20260602">
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="20260601" total="1000" cash="1000"/>
        <EquitySummaryByReportDateInBase reportDate="20260602" total="3500" cash="3500"/>
      </EquitySummaryInBase>
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="2500" currency="USD" dateTime="20260602;120000"/>
      </CashTransactions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`

func TestSingleFlexStatement_MasterCopyOfTheDepositIsDropped(t *testing.T) {
	single, n := singleFlexStatement([]byte(multiAccountReport))
	if n != 2 {
		t.Fatalf("counted %d statements, want 2", n)
	}
	i := &IBKR{}
	snaps, err := i.parseHistoricalSnapshotsFromReport(single, time.Time{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("got %d snapshots, want the sub-account's 2 days", len(snaps))
	}
	if snaps[1].Deposits != 2500 {
		t.Fatalf("funding day deposits = %v, want 2500 once", snaps[1].Deposits)
	}
	if phantom := snaps[1].TotalEquity - snaps[0].TotalEquity - snaps[1].Deposits; phantom != 0 {
		t.Fatalf("funding day scores %v of performance, want 0", phantom)
	}
}

// The kept statement is re-read by every parser, so its attributes (paper
// detection reads accountId) and escaped text must survive the narrowing.
func TestSingleFlexStatement_KeptStatementReadsAsItCame(t *testing.T) {
	single, _ := singleFlexStatement([]byte(multiAccountReport))
	i := &IBKR{}
	if _, err := i.parseBalanceFromReport(single); err != nil {
		t.Fatalf("parse balance: %v", err)
	}
	if i.cachedIsPaper == nil || !*i.cachedIsPaper {
		t.Fatalf("paper flag lost: the kept statement's accountId did not survive")
	}
	flows, err := i.parseCashflowsFromReport(single, time.Time{})
	if err != nil {
		t.Fatalf("parse cashflows: %v", err)
	}
	if len(flows) != 1 {
		t.Fatalf("got %d flows, want the ampersand type parsed once", len(flows))
	}
}

func TestSingleFlexStatement_OneStatementIsUntouched(t *testing.T) {
	report := flexReport(`
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="100" currency="USD" dateTime="20260601;120000"/>
      </CashTransactions>`)
	single, n := singleFlexStatement(report)
	if n != 1 || !bytes.Equal(single, report) {
		t.Fatalf("a single-account report was rewritten (n=%d)", n)
	}
}

func TestSingleFlexStatement_LargestAccountIsKept(t *testing.T) {
	report := []byte(`<FlexQueryResponse><FlexStatements count="2">
    <FlexStatement accountId="U0000001">
      <EquitySummaryInBase><EquitySummaryByReportDateInBase reportDate="20260602" total="800"/></EquitySummaryInBase>
    </FlexStatement>
    <FlexStatement accountId="U0000002">
      <EquitySummaryInBase><EquitySummaryByReportDateInBase reportDate="20260602" total="5000"/></EquitySummaryInBase>
    </FlexStatement>
  </FlexStatements></FlexQueryResponse>`)
	single, _ := singleFlexStatement(report)
	i := &IBKR{}
	bal, err := i.parseBalanceFromReport(single)
	if err != nil {
		t.Fatalf("parse balance: %v", err)
	}
	if bal.Equity != 5000 {
		t.Fatalf("equity = %v, want the larger account's 5000", bal.Equity)
	}
}

// The narrowing must be visible: a connection silently measuring one of two
// accounts is the kind of gap CapabilityWarnings exists to surface.
func TestStatementReport_MultiAccountRaisesAWarning(t *testing.T) {
	i := &IBKR{token: "synthetic-token-multi", queryID: "000001"}
	key := i.token + ":" + i.queryID
	flexReportCacheMu.Lock()
	flexReportCache[key] = &flexReportEntry{xml: []byte(multiAccountReport), fetchedAt: time.Now()}
	flexReportCacheMu.Unlock()
	t.Cleanup(func() {
		flexReportCacheMu.Lock()
		delete(flexReportCache, key)
		flexReportCacheMu.Unlock()
	})

	flows, err := i.GetCashflows(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("cashflows: %v", err)
	}
	if len(flows) != 1 {
		t.Fatalf("got %d flows, want the deposit once", len(flows))
	}
	warned := false
	for _, w := range i.CapabilityWarnings() {
		warned = warned || w == "ibkr_multi_account_statement(x2)"
	}
	if !warned {
		t.Fatalf("warnings %v, want ibkr_multi_account_statement(x2)", i.CapabilityWarnings())
	}

	raw, _, err := i.GetRawStatement(context.Background())
	if err != nil {
		t.Fatalf("raw statement: %v", err)
	}
	if !bytes.Contains(raw, []byte("F0000001")) {
		t.Fatalf("the raw view must keep every account")
	}
}

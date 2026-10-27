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
	single, n, _ := singleFlexStatement([]byte(multiAccountReport))
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
	single, _, _ := singleFlexStatement([]byte(multiAccountReport))
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
	single, n, _ := singleFlexStatement(report)
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
	single, _, _ := singleFlexStatement(report)
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
	plantDailyStatement(t, i, []byte(multiAccountReport))

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

// dailyBlock is one day of a "Breakout by Day" report: the previous day's NAV
// as an opening row, the day's own rows, and whatever else the day carries.
func dailyBlock(account, day, prevDay, prevTotal, total, extra string) string {
	return `
    <FlexStatement accountId="` + account + `" fromDate="` + day + `" toDate="` + day + `" period="">
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase currency="USD" reportDate="` + prevDay + `" total="` + prevTotal + `" cash="` + prevTotal + `" stock="0" options="0" commodities="0"/>
        <EquitySummaryByReportDateInBase currency="USD" reportDate="` + day + `" total="` + total + `" cash="` + total + `" stock="0" options="0" commodities="0"/>
      </EquitySummaryInBase>` + extra + `
    </FlexStatement>`
}

func dailyReport(blocks ...string) []byte {
	report := `<FlexQueryResponse queryName="q" type="AF"><FlexStatements>`
	for _, b := range blocks {
		report += b
	}
	return []byte(report + `</FlexStatements></FlexQueryResponse>`)
}

const sweepIn = `
      <CashReport>
        <CashReportCurrency currency="BASE_SUMMARY" depositWithdrawals="0" internalTransfers="500" accountTransfers="0"/>
        <CashReportCurrency currency="USD" depositWithdrawals="0" internalTransfers="500" accountTransfers="0"/>
      </CashReport>`

const quietDay = `
      <CashReport>
        <CashReportCurrency currency="BASE_SUMMARY" depositWithdrawals="0" internalTransfers="0" accountTransfers="0"/>
      </CashReport>`

// Breakout by Day returns one statement per day for the same account. Kept as
// a multi-account report, a single day survived and the history collapsed.
func TestSingleFlexStatement_DailyStatementsAreMergedBack(t *testing.T) {
	report := dailyReport(
		dailyBlock("U0000001", "20260601", "20260529", "1000", "1000", quietDay),
		dailyBlock("U0000001", "20260602", "20260601", "1000", "1500", sweepIn),
		dailyBlock("U0000001", "20260603", "20260602", "1500", "1550", quietDay+`
      <OpenPositions>
        <OpenPosition symbol="SYN" position="1" markPrice="50" fifoPnlUnrealized="5" assetCategory="STK" levelOfDetail="SUMMARY"/>
      </OpenPositions>`),
	)
	single, n, err := singleFlexStatement(report)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if n != 1 {
		t.Fatalf("counted %d accounts, want 1: daily statements are not accounts", n)
	}

	i := &IBKR{}
	snaps, err := i.parseHistoricalSnapshotsFromReport(single, time.Time{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(snaps) != 4 {
		t.Fatalf("got %d days, want each report date once (4)", len(snaps))
	}
	for k := 1; k < len(snaps); k++ {
		if !snaps[k].Date.After(snaps[k-1].Date) {
			t.Fatalf("days out of order or repeated at %v", snaps[k].Date)
		}
	}
	sweep := snaps[2]
	if sweep.Deposits != 500 {
		t.Fatalf("sweep day deposits = %v, want the internal transfer 500", sweep.Deposits)
	}
	if phantom := sweep.TotalEquity - snaps[1].TotalEquity - sweep.Deposits; phantom != 0 {
		t.Fatalf("sweep day scores %v of performance, want 0", phantom)
	}

	bal, err := i.parseBalanceFromReport(single)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if bal.Equity != 1550 {
		t.Fatalf("equity = %v, want the last day's 1550", bal.Equity)
	}
}

func TestStatementReport_DailyStatementsRaiseNoMultiAccountWarning(t *testing.T) {
	i := &IBKR{token: "synthetic-token-daily", queryID: "000002"}
	plantDailyStatement(t, i, dailyReport(
		dailyBlock("U0000001", "20260601", "20260529", "1000", "1000", quietDay),
		dailyBlock("U0000001", "20260602", "20260601", "1000", "1500", sweepIn),
	))

	flows, err := i.GetCashflows(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("cashflows: %v", err)
	}
	if len(flows) != 1 || flows[0].Amount != 500 {
		t.Fatalf("flows %+v, want the sweep once", flows)
	}
	for _, w := range i.CapabilityWarnings() {
		if bytes.HasPrefix([]byte(w), []byte("ibkr_multi_account_statement")) {
			t.Fatalf("daily statements raised %q", w)
		}
	}
}

// Statements whose periods overlap are not a split by day: merging them would
// count the shared days twice, so the sync fails and keeps what it stored.
func TestSingleFlexStatement_OverlappingPeriodsAreRefused(t *testing.T) {
	report := []byte(`<FlexQueryResponse><FlexStatements count="2">
    <FlexStatement accountId="U0000001" fromDate="20260601" toDate="20260602">
      <EquitySummaryInBase><EquitySummaryByReportDateInBase reportDate="20260602" total="1000"/></EquitySummaryInBase>
    </FlexStatement>
    <FlexStatement accountId="U0000001" fromDate="20260602" toDate="20260603">
      <EquitySummaryInBase><EquitySummaryByReportDateInBase reportDate="20260603" total="1100"/></EquitySummaryInBase>
    </FlexStatement>
  </FlexStatements></FlexQueryResponse>`)
	if _, _, err := singleFlexStatement(report); err == nil {
		t.Fatal("overlapping statements of one account must be refused")
	}
}

func TestSingleFlexStatement_UndatedStatementsOfOneAccountAreRefused(t *testing.T) {
	report := []byte(`<FlexQueryResponse><FlexStatements count="2">
    <FlexStatement accountId="U0000001">
      <EquitySummaryInBase><EquitySummaryByReportDateInBase reportDate="20260602" total="1000"/></EquitySummaryInBase>
    </FlexStatement>
    <FlexStatement accountId="U0000001">
      <EquitySummaryInBase><EquitySummaryByReportDateInBase reportDate="20260603" total="1100"/></EquitySummaryInBase>
    </FlexStatement>
  </FlexStatements></FlexQueryResponse>`)
	if _, _, err := singleFlexStatement(report); err == nil {
		t.Fatal("statements without a period cannot be ordered and must be refused")
	}
}

func TestSingleFlexStatement_LargestAccountKeptAndItsDaysMerged(t *testing.T) {
	report := dailyReport(
		dailyBlock("U0000001", "20260601", "20260529", "100", "100", quietDay),
		dailyBlock("U0000002", "20260601", "20260529", "1000", "1000", quietDay),
		dailyBlock("U0000001", "20260602", "20260601", "100", "120", quietDay),
		dailyBlock("U0000002", "20260602", "20260601", "1000", "1500", sweepIn),
	)
	single, n, err := singleFlexStatement(report)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if n != 2 {
		t.Fatalf("counted %d accounts, want 2", n)
	}
	i := &IBKR{}
	snaps, err := i.parseHistoricalSnapshotsFromReport(single, time.Time{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(snaps) != 3 || snaps[2].TotalEquity != 1500 || snaps[2].Deposits != 500 {
		t.Fatalf("got %d days, last %+v; want the larger account's 3 days with its sweep", len(snaps), snaps[len(snaps)-1])
	}
}

// A row repeated verbatim in two daily statements is one movement.
func TestSingleFlexStatement_RowRepeatedAcrossDaysCountsOnce(t *testing.T) {
	deposit := `
      <CashTransactions>
        <CashTransaction type="Deposits/Withdrawals" amount="250" currency="USD" dateTime="20260601;100000"/>
      </CashTransactions>`
	single, _, err := singleFlexStatement(dailyReport(
		dailyBlock("U0000001", "20260601", "20260529", "1000", "1250", deposit),
		dailyBlock("U0000001", "20260602", "20260601", "1250", "1250", deposit),
	))
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	flows, err := (&IBKR{}).parseCashflowsFromReport(single, time.Time{})
	if err != nil {
		t.Fatalf("cashflows: %v", err)
	}
	if len(flows) != 1 {
		t.Fatalf("got %d flows, want the deposit once", len(flows))
	}
}

// A Cash Report over the whole period cannot place a transfer on a day, so it
// adds nothing rather than a guess.
func TestCashReport_PeriodTotalsAreNotFlows(t *testing.T) {
	flows, _ := parseFlows(t, `
      <CashReport>
        <CashReportCurrency currency="BASE_SUMMARY" fromDate="20260101" toDate="20260630" internalTransfers="5000" accountTransfers="0"/>
        <CashReportCurrency internalTransfers="5000" accountTransfers="0"/>
      </CashReport>`)
	if len(flows) != 0 {
		t.Fatalf("got %d flows from period totals, want none", len(flows))
	}
}

// A transfer from another account sits in both the Transfers section and the
// day's Cash Report; it is one movement.
func TestCashReport_TransferAlsoInTransfersSectionCountsOnce(t *testing.T) {
	flows, _ := parseFlows(t, `
      <Transfers>
        <Transfer type="INTERNAL" direction="IN" cashTransfer="300" currency="USD" date="20260602"/>
      </Transfers>
      <CashReport>
        <CashReportCurrency currency="BASE_SUMMARY" fromDate="20260602" toDate="20260602" internalTransfers="0" accountTransfers="300"/>
      </CashReport>`)
	total := 0.0
	for _, f := range flows {
		total += f.Amount
	}
	if len(flows) != 1 || total != 300 {
		t.Fatalf("flows %+v, want 300 once", flows)
	}
}

func TestCashReport_OutgoingSweepIsAWithdrawal(t *testing.T) {
	flows, _ := parseFlows(t, `
      <CashReport>
        <CashReportCurrency currency="BASE_SUMMARY" fromDate="2026-06-02" toDate="2026-06-02" internalTransfers="-750" accountTransfers="0"/>
      </CashReport>`)
	if len(flows) != 1 || flows[0].Amount != -750 || flows[0].Timestamp.Format("20060102") != "20260602" {
		t.Fatalf("flows %+v, want -750 on 2026-06-02", flows)
	}
}

// plantDailyStatement caches report as the connection's daily statement, wide
// enough to answer every read.
func plantDailyStatement(t *testing.T, i *IBKR, report []byte) {
	t.Helper()
	key := i.token + ":" + i.queryID
	to := LastStatementDay(time.Now())
	flexReportCacheMu.Lock()
	flexReportCache[key] = &flexReportEntry{xml: report, from: to.AddDate(-5, 0, 0), to: to, fetchedAt: time.Now()}
	flexReportCacheMu.Unlock()
	t.Cleanup(func() {
		flexReportCacheMu.Lock()
		delete(flexReportCache, key)
		flexReportCacheMu.Unlock()
	})
}

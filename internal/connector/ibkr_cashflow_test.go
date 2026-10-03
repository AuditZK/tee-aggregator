package connector

import (
	"strings"
	"testing"
	"time"
)

func flexReport(rows string) []byte {
	return []byte(`<FlexQueryResponse queryName="q" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U1234567">` + rows + `
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`)
}

func parseFlows(t *testing.T, rows string) ([]*Cashflow, *IBKR) {
	t.Helper()
	i := &IBKR{}
	flows, err := i.parseCashflowsFromReport(flexReport(rows), time.Time{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return flows, i
}

// The regression this file exists for. Current Flex statements spell the
// umbrella funding type "Deposits & Withdrawals"; the parser matched only the
// older slash spelling, so an ordinary wire deposit was dropped without a
// trace. The account's equity stepped up with no recorded inflow and the step
// read as a trading gain — a fresh account showed a +9,758 "gain" that was
// its own funding (2026-08-25).
func TestParseCashflows_AmpersandSpellingIsCapital(t *testing.T) {
	flows, i := parseFlows(t, `
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="9758.16" currency="USD" dateTime="20260602;093000"/>
      </CashTransactions>`)
	if len(flows) != 1 {
		t.Fatalf("got %d cashflows, want the deposit", len(flows))
	}
	if flows[0].Amount != 9758.16 {
		t.Fatalf("amount = %v, want 9758.16", flows[0].Amount)
	}
	if len(i.CapabilityWarnings()) != 0 {
		t.Fatalf("a known type raised warnings: %v", i.CapabilityWarnings())
	}
}

func TestParseCashflows_SlashSpellingStillWorks(t *testing.T) {
	flows, _ := parseFlows(t, `
      <CashTransactions>
        <CashTransaction type="Deposits/Withdrawals" amount="1000" currency="USD" dateTime="20260601;120000"/>
        <CashTransaction type="Deposits/Withdrawals" amount="-250" currency="USD" dateTime="20260603;120000"/>
      </CashTransactions>`)
	if len(flows) != 2 {
		t.Fatalf("got %d cashflows, want 2", len(flows))
	}
	if flows[0].Amount != 1000 || flows[1].Amount != -250 {
		t.Fatalf("amounts = %v/%v, want 1000/-250", flows[0].Amount, flows[1].Amount)
	}
}

// Dividends and interest are returns of holding the account. Booking them as
// deposits would erase the very performance they represent.
func TestParseCashflows_IncomeIsNotCapitalAndNotAWarning(t *testing.T) {
	flows, i := parseFlows(t, `
      <CashTransactions>
        <CashTransaction type="Dividends" amount="52.30" currency="USD" dateTime="20260610;120000"/>
        <CashTransaction type="Withholding Tax" amount="-7.85" currency="USD" dateTime="20260610;120000"/>
        <CashTransaction type="Broker Interest Received" amount="1.12" currency="USD" dateTime="20260630;120000"/>
      </CashTransactions>`)
	if len(flows) != 0 {
		t.Fatalf("income rows became cashflows: %d", len(flows))
	}
	if len(i.CapabilityWarnings()) != 0 {
		t.Fatalf("known income types raised warnings: %v", i.CapabilityWarnings())
	}
}

// A type in neither list is money whose meaning we cannot state. It stays out
// of the cashflows, but it must leave a mark someone can find — the silent
// drop is exactly what produced the phantom gain.
func TestParseCashflows_UnknownTypeLeavesAWarning(t *testing.T) {
	flows, i := parseFlows(t, `
      <CashTransactions>
        <CashTransaction type="Sales Tax" amount="-3.10" currency="USD" dateTime="20260612;120000"/>
        <CashTransaction type="Sales Tax" amount="-2.90" currency="USD" dateTime="20260613;120000"/>
      </CashTransactions>`)
	if len(flows) != 0 {
		t.Fatalf("an unclassified type was booked as capital: %d flows", len(flows))
	}
	warns := i.CapabilityWarnings()
	if len(warns) != 1 {
		t.Fatalf("warnings = %v, want exactly one", warns)
	}
	if !strings.Contains(warns[0], "Sales Tax") || !strings.Contains(warns[0], "x2") {
		t.Fatalf("warning %q should carry the type and the count", warns[0])
	}
	// Types and counts only — an amount in a warning would end up in
	// sync_statuses and on a dashboard.
	if strings.Contains(warns[0], "3.10") || strings.Contains(warns[0], "2.90") {
		t.Fatalf("warning %q leaks amounts", warns[0])
	}
}

// Cash moved between accounts (INTERNAL, ACATS) never appears under
// CashTransactions. Without the Transfers section, a user funding this
// account from another one crosses the perimeter invisibly.
func TestParseCashflows_TransferCashCrossesThePerimeter(t *testing.T) {
	flows, i := parseFlows(t, `
      <Transfers>
        <Transfer type="INTERNAL" direction="IN" cashTransfer="5000" currency="USD" dateTime="20260605;100000"/>
        <Transfer type="ACATS" direction="OUT" cashTransfer="1200" currency="USD" date="20260620"/>
      </Transfers>`)
	if len(flows) != 2 {
		t.Fatalf("got %d cashflows, want 2 transfer legs", len(flows))
	}
	if flows[0].Amount != 5000 {
		t.Fatalf("inbound = %v, want +5000", flows[0].Amount)
	}
	if flows[1].Amount != -1200 {
		t.Fatalf("outbound = %v, want -1200 regardless of how the statement signs it", flows[1].Amount)
	}
	if len(i.CapabilityWarnings()) != 0 {
		t.Fatalf("clean transfers raised warnings: %v", i.CapabilityWarnings())
	}
}

// A pure position transfer moves holdings, not cash; its market value reaches
// the curve through equity like any position. Booking it as a deposit would
// double-count it.
func TestParseCashflows_PositionOnlyTransferIsNotCash(t *testing.T) {
	flows, _ := parseFlows(t, `
      <Transfers>
        <Transfer type="FOP" direction="IN" cashTransfer="0" currency="USD" dateTime="20260607;100000"/>
      </Transfers>`)
	if len(flows) != 0 {
		t.Fatalf("a position-only transfer became a cashflow: %d", len(flows))
	}
}

func TestParseCashflows_SingularSpellingsNormalizeTheirSign(t *testing.T) {
	flows, _ := parseFlows(t, `
      <CashTransactions>
        <CashTransaction type="Deposits" amount="-500" currency="USD" dateTime="20260601;120000"/>
        <CashTransaction type="Withdrawals" amount="300" currency="USD" dateTime="20260602;120000"/>
      </CashTransactions>`)
	if len(flows) != 2 {
		t.Fatalf("got %d cashflows, want 2", len(flows))
	}
	if flows[0].Amount != 500 {
		t.Fatalf("a Deposits row must come out positive, got %v", flows[0].Amount)
	}
	if flows[1].Amount != -300 {
		t.Fatalf("a Withdrawals row must come out negative, got %v", flows[1].Amount)
	}
}

func TestParseCashflows_SinceFilterAppliesToBothSections(t *testing.T) {
	i := &IBKR{}
	report := flexReport(`
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="100" currency="USD" dateTime="20260101;120000"/>
      </CashTransactions>
      <Transfers>
        <Transfer type="INTERNAL" direction="IN" cashTransfer="200" currency="USD" dateTime="20260102;120000"/>
      </Transfers>`)
	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	flows, err := i.parseCashflowsFromReport(report, since)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(flows) != 0 {
		t.Fatalf("rows before the since bound leaked through: %d", len(flows))
	}
}

// A query with both "Summary" and "Detail" ticked under Cash Transactions
// restates every movement once per level. Summed together, each deposit was
// booked at twice its size and the excess scored as a loss on the funding day.
func TestParseCashflows_SummaryRowsAreNotCountedTwice(t *testing.T) {
	flows, _ := parseFlows(t, `
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="2500" currency="USD" dateTime="20260601;120000" levelOfDetail="DETAIL"/>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="2500" currency="USD" dateTime="20260601;120000" levelOfDetail="SUMMARY"/>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="-400" currency="USD" dateTime="20260610;120000" levelOfDetail="DETAIL"/>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="-400" currency="USD" dateTime="20260610;120000" levelOfDetail="SUMMARY"/>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="2500" currency="BASE_SUMMARY" dateTime="20260601;120000"/>
      </CashTransactions>`)
	if len(flows) != 2 {
		t.Fatalf("got %d cashflows, want the deposit and the withdrawal once each", len(flows))
	}
	if flows[0].Amount != 2500 || flows[1].Amount != -400 {
		t.Fatalf("amounts = %v/%v, want 2500/-400", flows[0].Amount, flows[1].Amount)
	}
}

// Two genuine movements of the same size on the same day are two flows; only
// the level of detail decides what is a restatement, never equality of rows.
func TestParseCashflows_IdenticalDetailRowsAreBothKept(t *testing.T) {
	flows, _ := parseFlows(t, `
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="1000" currency="USD" dateTime="20260601;120000" levelOfDetail="DETAIL"/>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="1000" currency="USD" dateTime="20260601;120000" levelOfDetail="DETAIL"/>
      </CashTransactions>`)
	if len(flows) != 2 {
		t.Fatalf("got %d cashflows, want both deposits", len(flows))
	}
}

// The historical walk is what a customer sees: the funding day must carry the
// deposit once, so equity minus flows leaves no phantom loss.
func TestParseHistoricalSnapshots_DepositBookedOnceWithSummaryRows(t *testing.T) {
	i := &IBKR{}
	snaps, err := i.parseHistoricalSnapshotsFromReport(flexReport(`
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="20260601" total="1000" cash="1000"/>
        <EquitySummaryByReportDateInBase reportDate="20260602" total="3500" cash="3500"/>
      </EquitySummaryInBase>
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="2500" currency="USD" dateTime="20260602;120000" levelOfDetail="DETAIL"/>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="2500" currency="USD" dateTime="20260602;120000" levelOfDetail="SUMMARY"/>
      </CashTransactions>`), time.Time{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("got %d snapshots, want 2", len(snaps))
	}
	if snaps[1].Deposits != 2500 {
		t.Fatalf("funding day deposits = %v, want 2500", snaps[1].Deposits)
	}
	if phantom := snaps[1].TotalEquity - snaps[0].TotalEquity - snaps[1].Deposits; phantom != 0 {
		t.Fatalf("funding day scores %v of performance, want 0", phantom)
	}
}

// A query with every Trades level ticked restates each execution as an order
// and as closed lots; counting them all inflated trade count, volume and fees.
func TestParseTrades_OnlyExecutionsCount(t *testing.T) {
	i := &IBKR{}
	trades, err := i.parseTradesFromReport(flexReport(`
      <Trades>
        <Trade tradeID="1" symbol="AAA" buySell="BUY" tradePrice="10" quantity="5" ibCommission="-1" currency="USD" dateTime="20260601;100000" assetCategory="STK" levelOfDetail="EXECUTION"/>
        <Trade tradeID="2" symbol="AAA" buySell="BUY" tradePrice="10" quantity="5" ibCommission="-1" currency="USD" dateTime="20260601;100500" assetCategory="STK" levelOfDetail="EXECUTION"/>
        <Trade tradeID="" symbol="AAA" buySell="BUY" tradePrice="10" quantity="10" ibCommission="-2" currency="USD" dateTime="20260601;100500" assetCategory="STK" levelOfDetail="ORDER"/>
        <Trade tradeID="" symbol="AAA" buySell="SELL" tradePrice="11" quantity="10" ibCommission="0" currency="USD" dateTime="20260602;100000" assetCategory="STK" levelOfDetail="CLOSED_LOT"/>
        <Trade tradeID="3" symbol="BBB" buySell="SELL" tradePrice="20" quantity="1" ibCommission="-1" currency="USD" dateTime="20260602;110000" assetCategory="STK"/>
      </Trades>`), time.Time{}, time.Now())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(trades) != 3 {
		t.Fatalf("got %d trades, want the 2 executions and the row without a level", len(trades))
	}
	var fees float64
	for _, tr := range trades {
		fees += tr.Fee
	}
	if fees != 3 {
		t.Fatalf("fees = %v, want 3", fees)
	}
}

// With "Summary" and "Lot" both ticked, a holding bought in two lots appears
// three times; counting every row doubled the position and its unrealized P&L.
func TestParsePositions_LotRowsDoNotDoubleTheHolding(t *testing.T) {
	report := flexReport(`
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="20260601" total="2000" cash="1000" stock="1000"/>
      </EquitySummaryInBase>
      <OpenPositions>
        <OpenPosition symbol="AAA" position="10" markPrice="100" costBasisMoney="900" fifoPnlUnrealized="100" assetCategory="STK" levelOfDetail="SUMMARY"/>
        <OpenPosition symbol="AAA" position="4" markPrice="100" costBasisMoney="380" fifoPnlUnrealized="20" assetCategory="STK" levelOfDetail="LOT"/>
        <OpenPosition symbol="AAA" position="6" markPrice="100" costBasisMoney="520" fifoPnlUnrealized="80" assetCategory="STK" levelOfDetail="LOT"/>
      </OpenPositions>`)
	i := &IBKR{}
	positions, err := i.parsePositionsFromReport(report)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(positions) != 1 || positions[0].Size != 10 {
		t.Fatalf("got %d positions (first size %v), want one holding of 10", len(positions), positions[0].Size)
	}
	bal, err := i.parseBalanceFromReport(report)
	if err != nil {
		t.Fatalf("parse balance: %v", err)
	}
	if bal.UnrealizedPnL != 100 {
		t.Fatalf("unrealized = %v, want 100", bal.UnrealizedPnL)
	}
}

// A query that asked for lots only has no summary rows to prefer; dropping the
// lots would empty the book.
func TestParsePositions_LotOnlyQueryKeepsItsRows(t *testing.T) {
	i := &IBKR{}
	positions, err := i.parsePositionsFromReport(flexReport(`
      <OpenPositions>
        <OpenPosition symbol="AAA" position="4" markPrice="100" costBasisMoney="380" assetCategory="STK" levelOfDetail="LOT"/>
        <OpenPosition symbol="BBB" position="2" markPrice="50" costBasisMoney="90" assetCategory="STK" levelOfDetail="LOT"/>
      </OpenPositions>`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(positions) != 2 {
		t.Fatalf("got %d positions, want 2", len(positions))
	}
}

// A query that asked for Summary alone has no detail rows to prefer; setting
// its rows aside would erase every deposit and score each one as a gain.
func TestParseCashflows_SummaryOnlyQueryKeepsItsFlows(t *testing.T) {
	flows, _ := parseFlows(t, `
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="2500" currency="USD" dateTime="20260601;120000" levelOfDetail="SUMMARY"/>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="-400" currency="USD" dateTime="20260610;120000" levelOfDetail="SUMMARY"/>
      </CashTransactions>`)
	if len(flows) != 2 {
		t.Fatalf("got %d cashflows, want both summary rows", len(flows))
	}
}

func TestParseTrades_NoExecutionLevelKeepsItsRows(t *testing.T) {
	i := &IBKR{}
	trades, err := i.parseTradesFromReport(flexReport(`
      <Trades>
        <Trade tradeID="" symbol="AAA" buySell="BUY" tradePrice="10" quantity="10" ibCommission="-2" currency="USD" dateTime="20260601;100500" assetCategory="STK" levelOfDetail="ORDER"/>
        <Trade tradeID="" symbol="BBB" buySell="SELL" tradePrice="20" quantity="1" ibCommission="-1" currency="USD" dateTime="20260602;110000" assetCategory="STK" levelOfDetail="ORDER"/>
      </Trades>`), time.Time{}, time.Now())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(trades) != 2 {
		t.Fatalf("got %d trades, want both order rows", len(trades))
	}
}

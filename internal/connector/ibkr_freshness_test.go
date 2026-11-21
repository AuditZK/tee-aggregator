package connector

import (
	"testing"
	"time"
)

const flexTwoSummaries = `<FlexQueryResponse queryName="q" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U1234567">
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="20260729" currency="EUR" total="14128.85" cash="11.45" stock="14117.40" unrealizedPnL="0"/>
        <EquitySummaryByReportDateInBase reportDate="20260730" currency="EUR" total="15314.30" cash="11.46" stock="15302.84" unrealizedPnL="0"/>
      </EquitySummaryInBase>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`

const flexNoReportDate = `<FlexQueryResponse queryName="q" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U1234567">
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase currency="EUR" total="100.00" cash="100.00" unrealizedPnL="0"/>
      </EquitySummaryInBase>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`

// The balance returned by GetBalance is the newest summary IN THE STATEMENT,
// which can be two days old. BalanceAsOf must expose that summary's
// reportDate so the sync layer can refuse to stamp it with today's date
// (phantom-snapshot defect, audit 2026-08-01).
func TestIBKRParseBalance_RecordsReportDate(t *testing.T) {
	i := &IBKR{}

	bal, err := i.parseBalanceFromReport([]byte(flexTwoSummaries))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if bal.Equity != 15314.30 {
		t.Fatalf("equity = %v, want 15314.30 (latest summary)", bal.Equity)
	}
	want := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	if got := i.BalanceAsOf(); !got.Equal(want) {
		t.Fatalf("BalanceAsOf = %s, want %s", got, want)
	}
}

// A statement without a parsable reportDate must reset the cached as-of to
// zero so the guard fails open instead of reusing a previous call's date.
func TestIBKRParseBalance_ResetsReportDateWhenAbsent(t *testing.T) {
	i := &IBKR{}

	if _, err := i.parseBalanceFromReport([]byte(flexTwoSummaries)); err != nil {
		t.Fatalf("parse (dated): %v", err)
	}
	if i.BalanceAsOf().IsZero() {
		t.Fatal("precondition failed: expected a non-zero as-of after dated statement")
	}

	if _, err := i.parseBalanceFromReport([]byte(flexNoReportDate)); err != nil {
		t.Fatalf("parse (undated): %v", err)
	}
	if got := i.BalanceAsOf(); !got.IsZero() {
		t.Fatalf("BalanceAsOf = %s, want zero after undated statement", got)
	}
}

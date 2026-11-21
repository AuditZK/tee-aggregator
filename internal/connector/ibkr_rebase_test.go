package connector

import (
	"math"
	"testing"
	"time"
)

// An account based in USD, funded in EUR: the statement values the euros at
// IBKR's own rate, and so must the flows, or the funding day reads as a gain.
const rebasedStatement = `
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="20260930" currency="USD" total="1000" cash="1000"/>
        <EquitySummaryByReportDateInBase reportDate="20261001" currency="USD" total="1112" cash="1112"/>
      </EquitySummaryInBase>
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="100" currency="EUR" fxRateToBase="1.12" dateTime="20261001;100000" levelOfDetail="DETAIL"/>
      </CashTransactions>
      <ConversionRates>
        <ConversionRate reportDate="20260930" fromCurrency="EUR" toCurrency="USD" rate="1.13"/>
        <ConversionRate reportDate="20261001" fromCurrency="EUR" toCurrency="USD" rate="1.12"/>
        <ConversionRate reportDate="20261001" fromCurrency="GBP" toCurrency="CHF" rate="1.05"/>
      </ConversionRates>`

func TestIBKR_FlowInAnotherCurrencyIsStatedInTheBase(t *testing.T) {
	flows, _ := parseFlows(t, rebasedStatement)
	if len(flows) != 1 {
		t.Fatalf("flows = %d, want 1", len(flows))
	}
	if flows[0].Currency != "USD" || math.Abs(flows[0].Amount-112) > 1e-9 {
		t.Fatalf("flow = %+v, want 112 USD (100 EUR at IBKR's 1.12)", flows[0])
	}
}

func TestIBKR_FlowWithoutBaseRateKeepsItsCurrency(t *testing.T) {
	flows, _ := parseFlows(t, `
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="20261001" currency="USD" total="1112" cash="1112"/>
      </EquitySummaryInBase>
      <CashTransactions>
        <CashTransaction type="Deposits &amp; Withdrawals" amount="100" currency="EUR" dateTime="20261001;100000" levelOfDetail="DETAIL"/>
      </CashTransactions>`)
	if len(flows) != 1 || flows[0].Currency != "EUR" || flows[0].Amount != 100 {
		t.Fatalf("flows = %+v, want 100 EUR for the sync to value", flows)
	}
}

func TestIBKR_StatementRatesReachTheBalanceAndEachDay(t *testing.T) {
	i := &IBKR{}
	bal, err := i.parseBalanceFromReport(flexReport(rebasedStatement))
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if bal.Currency != "USD" || bal.BaseRates["EUR"] != 1.12 {
		t.Fatalf("balance = %+v, want USD with EUR at 1.12", bal)
	}
	if _, ok := bal.BaseRates["GBP"]; ok {
		t.Fatal("kept a rate quoted against another currency than the base")
	}

	snaps, err := i.parseHistoricalSnapshotsFromReport(flexReport(rebasedStatement), time.Time{})
	if err != nil || len(snaps) != 2 {
		t.Fatalf("history: %v, %d rows", err, len(snaps))
	}
	if snaps[0].BaseRates["EUR"] != 1.13 || snaps[1].BaseRates["EUR"] != 1.12 {
		t.Fatalf("day rates = %v / %v, want 1.13 then 1.12", snaps[0].BaseRates, snaps[1].BaseRates)
	}
	if snaps[1].Currency != "USD" || math.Abs(snaps[1].Deposits-112) > 1e-9 {
		t.Fatalf("funding day = %+v, want 112 USD deposited", snaps[1])
	}
	if phantom := snaps[1].TotalEquity - snaps[0].TotalEquity - snaps[1].Deposits; math.Abs(phantom) > 1e-9 {
		t.Fatalf("funding day scores %v of performance, want 0", phantom)
	}
}

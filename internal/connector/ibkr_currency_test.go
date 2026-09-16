package connector

import (
	"testing"
	"time"
)

func summaryReport(currencyAttr string) []byte {
	return []byte(`<FlexQueryResponse>
  <FlexStatements>
    <FlexStatement accountId="U1234567">
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="20260915" ` + currencyAttr + `
          total="10000" cash="10000" stock="0" options="0" commodities="0"
          unrealizedPnL="0" />
      </EquitySummaryInBase>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`)
}

// Nothing downstream converts currencies, so a EUR account's figures travel as
// EUR under a USD label. A French PEA is EUR by construction and two of them
// were being served as dollars.
func TestGetBalance_NonUSDAccountIsReported(t *testing.T) {
	i := &IBKR{}
	bal, err := i.parseBalanceFromReport(summaryReport(`currency="EUR"`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if bal.Currency != "EUR" {
		t.Fatalf("currency: got %q, want EUR", bal.Currency)
	}
	if got := i.CapabilityWarnings(); len(got) != 1 || got[0] != "ibkr_account_currency_eur" {
		t.Fatalf("warnings: got %v, want [ibkr_account_currency_eur]", got)
	}
}

// A query that does not select the Currency field leaves the attribute out
// entirely, which is indistinguishable from a dollar account. Silence is not
// a dollar.
func TestGetBalance_MissingCurrencyIsNotAssumedToBeDollars(t *testing.T) {
	i := &IBKR{}
	if _, err := i.parseBalanceFromReport(summaryReport("")); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := i.CapabilityWarnings(); len(got) != 1 || got[0] != "ibkr_account_currency_unknown" {
		t.Fatalf("warnings: got %v, want [ibkr_account_currency_unknown]", got)
	}
}

func TestGetBalance_DollarAccountWarnsAboutNothing(t *testing.T) {
	i := &IBKR{}
	if _, err := i.parseBalanceFromReport(summaryReport(`currency="USD"`)); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := i.CapabilityWarnings(); len(got) != 0 {
		t.Fatalf("warnings: got %v, want none", got)
	}
}

// GetCashflows rebuilds capabilityWarnings from scratch on every call. The
// account's denomination must survive that, or the warning disappears on the
// sync step that follows the balance.
func TestCapabilityWarnings_CurrencySurvivesACashflowParse(t *testing.T) {
	i := &IBKR{}
	if _, err := i.parseBalanceFromReport(summaryReport(`currency="EUR"`)); err != nil {
		t.Fatalf("parse balance: %v", err)
	}
	if _, err := i.parseCashflowsFromReport([]byte(`<FlexQueryResponse>
  <FlexStatements><FlexStatement><CashTransactions>
    <CashTransaction type="Deposits/Withdrawals" amount="500" currency="EUR" dateTime="20260901;120000"/>
  </CashTransactions></FlexStatement></FlexStatements>
</FlexQueryResponse>`), time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("parse cashflows: %v", err)
	}

	found := false
	for _, w := range i.CapabilityWarnings() {
		if w == "ibkr_account_currency_eur" {
			found = true
		}
	}
	if !found {
		t.Fatalf("currency warning lost after a cashflow parse: %v", i.CapabilityWarnings())
	}
}

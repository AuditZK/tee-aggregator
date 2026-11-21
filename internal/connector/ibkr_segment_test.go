package connector

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func cashReport(internal string) string {
	return `
      <CashReport>
        <CashReportCurrency currency="BASE_SUMMARY" depositWithdrawals="0" internalTransfers="` + internal + `" accountTransfers="0"/>
      </CashReport>`
}

// An account and its IB-UKL segment, money swept from U to F on the second
// day: each alone reads the sweep as a flow, together they read nothing.
func segmentReport() []byte {
	return dailyReport(
		dailyBlock("U0000001", "20260601", "20260529", "1000", "1000", cashReport("0")),
		dailyBlock("U0000001", "20260602", "20260601", "1000", "600", cashReport("-400")),
		dailyBlock("U0000001", "20260603", "20260602", "600", "610", cashReport("0")+`
      <OpenPositions>
        <OpenPosition symbol="SYN" position="1" markPrice="10" fifoPnlUnrealized="1" assetCategory="STK" levelOfDetail="SUMMARY"/>
      </OpenPositions>`),
		dailyBlock("U0000001F", "20260601", "20260529", "200", "200", cashReport("0")),
		dailyBlock("U0000001F", "20260602", "20260601", "200", "600", cashReport("400")),
		dailyBlock("U0000001F", "20260603", "20260602", "600", "606", cashReport("0")+`
      <OpenPositions>
        <OpenPosition symbol="SYF" position="2" markPrice="3" fifoPnlUnrealized="0" assetCategory="STK" levelOfDetail="SUMMARY"/>
      </OpenPositions>`),
	)
}

func TestNarrowFlexStatement_SegmentIsConsolidated(t *testing.T) {
	n, err := narrowFlexStatement(segmentReport())
	if err != nil {
		t.Fatalf("narrow: %v", err)
	}
	if want := []string{"U0000001", "U0000001F"}; !reflect.DeepEqual(n.accounts, want) {
		t.Fatalf("accounts %v, want %v", n.accounts, want)
	}

	i := &IBKR{}
	snaps, err := i.parseHistoricalSnapshotsFromReport(n.report, time.Time{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	equity := map[string]float64{}
	for _, s := range snaps {
		equity[s.Date.Format("20060102")] = s.TotalEquity
		if s.Deposits != 0 || s.Withdrawals != 0 {
			t.Fatalf("%s books deposits %v withdrawals %v, want the sweep netted out", s.Date.Format("20060102"), s.Deposits, s.Withdrawals)
		}
	}
	want := map[string]float64{"20260529": 1200, "20260601": 1200, "20260602": 1200, "20260603": 1216}
	if !reflect.DeepEqual(equity, want) {
		t.Fatalf("equity %v, want both segments summed %v", equity, want)
	}

	bal, err := i.parseBalanceFromReport(n.report)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if bal.Equity != 1216 {
		t.Fatalf("balance %v, want 1216", bal.Equity)
	}
	if !strings.Contains(string(n.report), `symbol="SYN"`) || !strings.Contains(string(n.report), `symbol="SYF"`) {
		t.Fatalf("positions of both segments must be kept")
	}
}

func TestStatementReport_SegmentRaisesNoMultiAccountWarning(t *testing.T) {
	i := &IBKR{token: "synthetic-token-segment", queryID: "000003"}
	plantDailyStatement(t, i, segmentReport())
	flows, err := i.GetCashflows(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("cashflows: %v", err)
	}
	if len(flows) != 0 {
		t.Fatalf("flows %+v, want none: the sweep stays inside the account", flows)
	}
	for _, w := range i.CapabilityWarnings() {
		if strings.HasPrefix(w, "ibkr_multi_account_statement") {
			t.Fatalf("a consolidated segment raised %q", w)
		}
	}
	if want := []string{"U0000001", "U0000001F"}; !reflect.DeepEqual(i.MeasuredAccounts(), want) {
		t.Fatalf("measured %v, want %v", i.MeasuredAccounts(), want)
	}
}

// A segment without a NAV row on a day keeps its last balance that day
// instead of reading as money leaving.
func TestNarrowFlexStatement_SegmentSkippingADayKeepsItsBalance(t *testing.T) {
	report := dailyReport(
		dailyBlock("U0000001", "20260601", "20260529", "1000", "1000", cashReport("0")),
		dailyBlock("U0000001", "20260602", "20260601", "1000", "1010", cashReport("0")),
		dailyBlock("U0000001F", "20260601", "20260529", "300", "300", cashReport("0")),
	)
	n, err := narrowFlexStatement(report)
	if err != nil {
		t.Fatalf("narrow: %v", err)
	}
	bal, err := (&IBKR{}).parseBalanceFromReport(n.report)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if bal.Equity != 1310 {
		t.Fatalf("balance %v, want 1310 (the segment's 300 carried)", bal.Equity)
	}
}

// Two accounts that are not segments of each other stay apart: the larger
// group wins, and an account with its segment weighs as the pair.
func TestNarrowFlexStatement_PairWeighsAsOneAgainstAnotherAccount(t *testing.T) {
	report := dailyReport(
		dailyBlock("U0000001", "20260601", "20260529", "600", "600", cashReport("0")),
		dailyBlock("U0000001F", "20260601", "20260529", "600", "600", cashReport("0")),
		dailyBlock("U0000002", "20260601", "20260529", "1000", "1000", cashReport("0")),
	)
	n, err := narrowFlexStatement(report)
	if err != nil {
		t.Fatalf("narrow: %v", err)
	}
	if want := []string{"U0000001", "U0000001F"}; !reflect.DeepEqual(n.accounts, want) {
		t.Fatalf("accounts %v, want the pair %v", n.accounts, want)
	}
	if n.carried != 3 {
		t.Fatalf("carried %d, want 3", n.carried)
	}
}

func TestNarrowFlexStatement_MasterAndSubAccountAreNotSegments(t *testing.T) {
	n, err := narrowFlexStatement([]byte(multiAccountReport))
	if err != nil {
		t.Fatalf("narrow: %v", err)
	}
	if want := []string{"DU0000002"}; !reflect.DeepEqual(n.accounts, want) {
		t.Fatalf("accounts %v, want %v", n.accounts, want)
	}
}

func TestNarrowFlexStatement_SegmentsInTwoCurrenciesAreRefused(t *testing.T) {
	eur := strings.ReplaceAll(dailyBlock("U0000001F", "20260601", "20260529", "100", "100", cashReport("0")), `currency="USD"`, `currency="EUR"`)
	report := dailyReport(
		dailyBlock("U0000001", "20260601", "20260529", "1000", "1000", cashReport("0")),
		eur,
	)
	if _, err := narrowFlexStatement(report); err == nil {
		t.Fatalf("segments in two base currencies must not be added")
	}
}

func TestAccountGuard_RefusalStopsEveryRead(t *testing.T) {
	refused := errors.New("synthetic refusal")
	var seen []string
	i := &IBKR{token: "synthetic-token-guard", queryID: "000004"}
	i.SetAccountGuard(func(accounts []string, _ bool) error {
		seen = accounts
		return refused
	})
	plantDailyStatement(t, i, segmentReport())
	if _, err := i.GetBalance(context.Background()); !errors.Is(err, refused) {
		t.Fatalf("balance error %v, want the guard's refusal", err)
	}
	if _, err := i.GetCashflows(context.Background(), time.Time{}); !errors.Is(err, refused) {
		t.Fatalf("cashflows error %v, want the guard's refusal", err)
	}
	if want := []string{"U0000001", "U0000001F"}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("guard saw %v, want %v", seen, want)
	}
}

func TestNarrowFlexStatement_StatementWithoutAccountIdPinsNothing(t *testing.T) {
	report := strings.ReplaceAll(string(dailyReport(dailyBlock("U0000001", "20260601", "20260529", "1000", "1000", cashReport("0")))), `accountId="U0000001" `, "")
	n, err := narrowFlexStatement([]byte(report))
	if err != nil {
		t.Fatalf("narrow: %v", err)
	}
	if len(n.accounts) != 0 {
		t.Fatalf("accounts %q, want none", n.accounts)
	}
}

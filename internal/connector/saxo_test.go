package connector

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Resolved by runtime type assertion, so a drifted signature would silently
// switch a capability off rather than fail the build.
var (
	_ PaperAccountDetector  = (*Saxo)(nil)
	_ CashflowFetcher       = (*Saxo)(nil)
	_ FundingFeesFetcher    = (*Saxo)(nil)
	_ CapabilityWarner      = (*Saxo)(nil)
	_ OAuthCredentialSource = (*Saxo)(nil)
	_ RefreshGuarded        = (*Saxo)(nil)
	_ OAuthKeepAliver       = (*Saxo)(nil)
)

const (
	saxoTestAccess    = "synthetic-access-1"
	saxoTestRefresh   = "synthetic-refresh-1"
	saxoTestAppKey    = "synthetic-app-key"
	saxoTestAppSecret = "synthetic-app-secret"
)

func newSaxoTest(t *testing.T, sim bool, handler http.HandlerFunc) *Saxo {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	s := NewSaxo(&Credentials{
		APIKey:       saxoTestAccess,
		APISecret:    saxoTestRefresh,
		ClientID:     saxoTestAppKey,
		ClientSecret: saxoTestAppSecret,
	}, sim)
	s.baseURL = srv.URL
	s.authURL = srv.URL
	s.redirectURI = ""
	s.retryBackoff = time.Millisecond
	return s
}

func saxoWriteClient(w http.ResponseWriter) {
	fmt.Fprint(w, `{"ClientKey":"synthetic-client-key","DefaultCurrency":"EUR"}`)
}

func TestSaxoExchangeAndPaperDetection(t *testing.T) {
	for _, tc := range []struct {
		sim          bool
		wantExchange string
	}{
		{sim: false, wantExchange: "saxo"},
		{sim: true, wantExchange: "saxo_sim"},
	} {
		t.Run(tc.wantExchange, func(t *testing.T) {
			s := NewSaxo(&Credentials{APIKey: "a"}, tc.sim)
			if got := s.Exchange(); got != tc.wantExchange {
				t.Fatalf("Exchange() = %q, want %q", got, tc.wantExchange)
			}
			isPaper, err := s.DetectIsPaper(context.Background())
			if err != nil || isPaper != tc.sim {
				t.Fatalf("DetectIsPaper = %v, %v; want %v", isPaper, err, tc.sim)
			}
		})
	}
}

func TestSaxoGatewaysPerEnvironment(t *testing.T) {
	live := NewSaxo(&Credentials{}, false)
	if live.baseURL != saxoLiveAPI || live.authURL != saxoLiveAuth {
		t.Fatalf("live = %s / %s", live.baseURL, live.authURL)
	}
	sim := NewSaxo(&Credentials{}, true)
	if sim.baseURL != saxoSimAPI || sim.authURL != saxoSimAuth {
		t.Fatalf("sim = %s / %s", sim.baseURL, sim.authURL)
	}
}

func TestFactoryRegistersBothSaxoGateways(t *testing.T) {
	f := NewFactory()
	for _, ex := range []string{"saxo", "saxo_sim"} {
		conn, err := f.Create(&Credentials{Exchange: ex, APIKey: "a", APISecret: "r"})
		if err != nil {
			t.Fatalf("Create(%s): %v", ex, err)
		}
		if conn.Exchange() != ex {
			t.Fatalf("Create(%s).Exchange() = %q", ex, conn.Exchange())
		}
		if !slices.Contains(f.SupportedExchanges(), ex) {
			t.Fatalf("SupportedExchanges() is missing %s", ex)
		}
	}
}

// CashBalance + UnrealizedPositionsValue = TotalValue on Saxo's own figures, so
// the realised balance the sync derives (Equity - UnrealizedPnL) must land on
// CashBalance. The margin P&L figure excludes the closing costs and would not.
func TestSaxoGetBalanceReconcilesToCashBalance(t *testing.T) {
	s := newSaxoTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/port/v1/balances/me":
			fmt.Fprint(w, `{
				"CashBalance":10000.00,
				"CostToClosePositions":-12.50,
				"Currency":"EUR",
				"MarginAvailableForTrading":9100.00,
				"TotalValue":9749.50,
				"UnrealizedMarginProfitLoss":-238.00,
				"UnrealizedPositionsValue":-250.50
			}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	})

	bal, err := s.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 9749.50 || bal.Available != 9100.00 || bal.Currency != "EUR" {
		t.Fatalf("balance = %+v", bal)
	}
	if realized := bal.Equity - bal.UnrealizedPnL; math.Abs(realized-10000.00) > 1e-9 {
		t.Fatalf("Equity - UnrealizedPnL = %v, want the cash balance 10000", realized)
	}
}

// A cash account has no margin figure; the cash available stands in for it.
func TestSaxoGetBalanceCashAccount(t *testing.T) {
	s := newSaxoTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"CashBalance":500,"CashAvailableForTrading":480,"Currency":"CHF","TotalValue":1500,"UnrealizedPositionsValue":1000}`)
	})
	bal, err := s.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Available != 480 {
		t.Fatalf("Available = %v, want the cash available 480", bal.Available)
	}
}

// An answer without TotalValue is not an account worth zero.
func TestSaxoGetBalanceRefusesAMissingTotal(t *testing.T) {
	s := newSaxoTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"CashBalance":500,"Currency":"EUR"}`)
	})
	if _, err := s.GetBalance(context.Background()); err == nil {
		t.Fatal("GetBalance accepted a payload with no TotalValue")
	}
}

func TestSaxoMarketTypeMapping(t *testing.T) {
	for _, tc := range []struct{ assetType, want string }{
		{"FxSpot", MarketForex},
		{"FxForwards", MarketForex},
		{"FxVanillaOption", MarketOptions},
		{"StockOption", MarketOptions},
		{"CfdOnStock", MarketCFD},
		{"CfdOnIndex", MarketCFD},
		{"ContractFutures", MarketFutures},
		{"Stock", MarketStocks},
		{"Etf", MarketStocks},
	} {
		if got := saxoMarketType(tc.assetType); got != tc.want {
			t.Errorf("saxoMarketType(%q) = %q, want %q", tc.assetType, got, tc.want)
		}
	}
}

func TestSaxoSymbolFromNetPositionID(t *testing.T) {
	for _, tc := range []struct{ id, assetType, want string }{
		{"EURUSD_FxSpot", "FxSpot", "EURUSD"},
		{"EURUSD__FxSpot", "FxSpot", "EURUSD"},
		{"GBPCAD__FxSpot", "", "GBPCAD"},
		{"SYNTH_A_Stock", "Stock", "SYNTH_A"},
		{"NoSeparator", "", "NoSeparator"},
		{"", "", ""},
	} {
		if got := saxoSymbol(tc.id, tc.assetType); got != tc.want {
			t.Errorf("saxoSymbol(%q, %q) = %q, want %q", tc.id, tc.assetType, got, tc.want)
		}
	}
}

func TestSaxoGetPositions(t *testing.T) {
	s := newSaxoTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/port/v1/positions/me" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"Data":[
			{"NetPositionId":"EURUSD_FxSpot",
			 "PositionBase":{"Amount":-10000,"AssetType":"FxSpot","OpenPrice":1.1},
			 "PositionView":{"CurrentPrice":1.09,"ProfitLossOnTradeInBaseCurrency":100}},
			{"NetPositionId":"SYNTH_Stock",
			 "PositionBase":{"Amount":10,"AssetType":"Stock","OpenPrice":50},
			 "PositionView":{"CurrentPrice":55,"ProfitLossOnTradeInBaseCurrency":50}},
			{"NetPositionId":"FLAT_FxSpot",
			 "PositionBase":{"Amount":0,"AssetType":"FxSpot","OpenPrice":1},
			 "PositionView":{"CurrentPrice":1,"ProfitLossOnTradeInBaseCurrency":0}}
		]}`)
	})

	positions, err := s.GetPositions(context.Background())
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if len(positions) != 2 {
		t.Fatalf("got %d positions, want 2 (flat one dropped)", len(positions))
	}
	if p := positions[0]; p.Side != "short" || p.Size != 10000 || p.Symbol != "EURUSD" || p.MarketType != MarketForex || p.UnrealizedPnL != 100 {
		t.Fatalf("position[0] = %+v", p)
	}
	if p := positions[1]; p.Side != "long" || p.MarketType != MarketStocks {
		t.Fatalf("position[1] = %+v", p)
	}
}

func TestSaxoGetTradesWindowsAndCosts(t *testing.T) {
	s := newSaxoTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/port/v1/clients/me":
			saxoWriteClient(w)
		case "/port/v1/closedpositions/me":
			fmt.Fprint(w, `{"Data":[
				{"ClosedPositionUniqueId":"1-2","NetPositionId":"GBPCAD_FxSpot","ClosedPosition":{
					"AssetType":"FxSpot","Amount":20000,"BuyOrSell":"Buy",
					"ClosedProfitLossInBaseCurrency":-40,"ClosingPrice":1.7,
					"CostOpeningInBaseCurrency":-2.5,"CostClosingInBaseCurrency":-2.25,
					"ExecutionTimeClose":"2030-03-05T22:57:51.935866Z"}},
				{"ClosedPositionUniqueId":"3-4","NetPositionId":"EURUSD_FxSpot","ClosedPosition":{
					"AssetType":"FxSpot","Amount":-10000,"BuyOrSell":"Sell",
					"ClosedProfitLossInBaseCurrency":25,"ClosingPrice":1.1,
					"CostOpeningInBaseCurrency":-1,"CostClosingInBaseCurrency":-1,
					"ExecutionTimeClose":"2030-03-06T10:00:00Z"}},
				{"ClosedPositionUniqueId":"5-6","NetPositionId":"OLD_FxSpot","ClosedPosition":{
					"AssetType":"FxSpot","Amount":1000,"BuyOrSell":"Buy",
					"ClosedProfitLossInBaseCurrency":10,"ClosingPrice":1,
					"ExecutionTimeClose":"2030-01-01T10:00:00Z"}}
			]}`)
		}
	})

	trades, err := s.GetTrades(context.Background(),
		time.Date(2030, 3, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2030, 3, 10, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 2 {
		t.Fatalf("got %d trades, want 2 (out-of-window close dropped)", len(trades))
	}
	if tr := trades[0]; tr.Side != "buy" || tr.Quantity != 20000 || tr.Symbol != "GBPCAD" || tr.FeeCurrency != "EUR" {
		t.Fatalf("trade[0] = %+v", tr)
	}
	if math.Abs(trades[0].Fee-4.75) > 1e-9 {
		t.Fatalf("trade[0].Fee = %v, want 4.75 (|opening| + |closing|)", trades[0].Fee)
	}
	if trades[1].Side != "sell" || trades[1].RealizedPnL != 25 {
		t.Fatalf("trade[1] = %+v", trades[1])
	}
}

func TestSaxoClientKeyIsCached(t *testing.T) {
	var clientCalls atomic.Int32
	s := newSaxoTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/port/v1/clients/me" {
			clientCalls.Add(1)
			saxoWriteClient(w)
		}
	})
	for range 3 {
		if err := s.TestConnection(context.Background()); err != nil {
			t.Fatalf("TestConnection: %v", err)
		}
	}
	if n := clientCalls.Load(); n != 1 {
		t.Fatalf("clients/me called %d times, want 1", n)
	}
}

func TestSaxoPagesThroughAFeed(t *testing.T) {
	s := newSaxoTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("$skip") == "0" {
			fmt.Fprint(w, `{"Data":[{"NetPositionId":"A_Stock","PositionBase":{"Amount":1,"AssetType":"Stock"}}],"__next":"/openapi/port/v1/positions/me?$skip=1"}`)
			return
		}
		fmt.Fprint(w, `{"Data":[{"NetPositionId":"B_Stock","PositionBase":{"Amount":2,"AssetType":"Stock"}}]}`)
	})
	positions, err := s.GetPositions(context.Background())
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if len(positions) != 2 || positions[1].Symbol != "B" {
		t.Fatalf("positions = %+v, want both pages", positions)
	}
}

// --- ledger -----------------------------------------------------------------

func saxoDate(t time.Time) string { return t.Format(time.DateOnly) }

func TestSaxoDayWindow(t *testing.T) {
	now := time.Date(2030, 6, 10, 0, 5, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		since    time.Time
		from, to string
	}{
		{"daily read", time.Date(2030, 6, 9, 0, 0, 0, 0, time.UTC), "2030-06-09", "2030-06-10"},
		{"mid-day start keeps the whole day", time.Date(2030, 6, 9, 15, 0, 0, 0, time.UTC), "2030-06-09", "2030-06-10"},
		{"after a gap", time.Date(2030, 6, 6, 0, 0, 0, 0, time.UTC), "2030-06-06", "2030-06-10"},
		{"no start", time.Time{}, "2030-06-09", "2030-06-10"},
		{"start in the future", time.Date(2030, 6, 12, 0, 0, 0, 0, time.UTC), "2030-06-10", "2030-06-10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			from, to := saxoDayWindow(tc.since, now)
			if saxoDate(from) != tc.from || saxoDate(to) != tc.to {
				t.Fatalf("window = %s..%s, want %s..%s", saxoDate(from), saxoDate(to), tc.from, tc.to)
			}
		})
	}
}

func saxoLedgerFixture(yesterday, today, before string) []saxoTransaction {
	return []saxoTransaction{
		{TransactionType: "CashTransfer", FundingSubType: "DepositFromExternal", Date: yesterday, BookedAmount: 5000},
		{TransactionType: "CashTransfer", FundingSubType: "InstantDeposit", Date: yesterday + "T00:00:00Z", IntradayAmount: 1200, IsIntradayData: true},
		{TransactionType: "CashTransfer", FundingSubType: "WithdrawalToExternal", Date: yesterday, BookedAmount: -800, Currency: "USD"},
		{TransactionType: "CashTransfer", FundingSubType: "IntraClientTransfer", Date: yesterday, BookedAmount: 300},
		{TransactionType: "CashTransfer", FundingSubType: "IntraClientTransfer", Date: yesterday, BookedAmount: -300},
		{TransactionType: "CashTransfer", FundingSubType: "DepositFromExternal", Date: today, BookedAmount: 7000},
		{TransactionType: "CashTransfer", FundingSubType: "DepositFromExternal", Date: before, BookedAmount: 9000},
		{TransactionType: "CashAmount", Event: "Interest", Date: yesterday, BookedAmount: 12},
		{TransactionType: "CashAmount", Event: "Custody Fee", Date: yesterday, BookedAmount: -5},
		{TransactionType: "CorporateAction", Event: "Cash Dividend", Date: yesterday, BookedAmount: 40},
		{TransactionType: "Trade", Event: "Buy", Date: yesterday, BookedAmount: -2500},
		{TransactionType: "Trade", Event: "Transfer In", Date: yesterday},
	}
}

// Only external cash transfers are capital. Interest, fees, financing,
// dividends and trades belong to the return; a transfer between the client's
// own accounts brought no money in; today's transfers are tomorrow's read.
func TestSaxoCashflowClassification(t *testing.T) {
	from := time.Date(2030, 6, 9, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 0, 1)
	rows := saxoLedgerFixture("2030-06-09", "2030-06-10", "2030-06-08")

	flows, warnings, err := saxoCashflows(rows, from, to, "EUR")
	if err != nil {
		t.Fatalf("saxoCashflows: %v", err)
	}

	var got []float64
	for _, f := range flows {
		got = append(got, f.Amount)
		if !f.Timestamp.Equal(from) {
			t.Errorf("flow %v dated %s, want its booking day", f.Amount, f.Timestamp)
		}
	}
	if want := []float64{5000, 1200, -800}; !slices.Equal(got, want) {
		t.Fatalf("flows = %v, want %v", got, want)
	}
	if flows[0].Currency != "EUR" || flows[2].Currency != "USD" {
		t.Fatalf("currencies = %s/%s, want the client default when the row has none", flows[0].Currency, flows[2].Currency)
	}
	if !slices.Contains(warnings, "saxo_security_transfer_unbooked") {
		t.Fatalf("warnings = %v, want the securities transfer reported", warnings)
	}
}

// A capital row whose date will not parse cannot be placed in a window, and
// skipping it books the deposit as performance.
func TestSaxoCashflowUnreadableDateFails(t *testing.T) {
	from := time.Date(2030, 6, 9, 0, 0, 0, 0, time.UTC)
	rows := []saxoTransaction{{TransactionType: "CashTransfer", Date: "soon", BookedAmount: 100}}
	if _, _, err := saxoCashflows(rows, from, from.AddDate(0, 0, 1), "EUR"); err == nil {
		t.Fatal("a cash transfer with an unreadable date was skipped silently")
	}
}

func TestSaxoCashTransferWithoutAmountIsReported(t *testing.T) {
	from := time.Date(2030, 6, 9, 0, 0, 0, 0, time.UTC)
	rows := []saxoTransaction{{TransactionType: "CashTransfer", FundingSubType: "DepositFromExternal", Date: "2030-06-09", IsIntradayData: true}}
	flows, warnings, err := saxoCashflows(rows, from, from.AddDate(0, 0, 1), "EUR")
	if err != nil || len(flows) != 0 {
		t.Fatalf("flows = %v, err = %v", flows, err)
	}
	if !slices.Contains(warnings, "saxo_cash_transfer_without_amount") {
		t.Fatalf("warnings = %v, want the amount-less transfer reported", warnings)
	}
}

func TestSaxoFundingFeesClassification(t *testing.T) {
	from := time.Date(2030, 6, 9, 0, 0, 0, 0, time.UTC)
	rows := saxoLedgerFixture("2030-06-09", "2030-06-10", "2030-06-08")
	rows[7].Bookings = []struct {
		AmountType string `json:"AmountType"`
	}{{AmountType: "Interest"}}

	fees := saxoFundingFees(rows, from, from.AddDate(0, 0, 1))
	var got []float64
	for _, f := range fees {
		got = append(got, f.Amount)
	}
	if want := []float64{12, -5, 40}; !slices.Equal(got, want) {
		t.Fatalf("funding = %v, want %v (cash amounts and corporate actions only)", got, want)
	}
	if fees[0].Symbol != "Interest" {
		t.Fatalf("label = %q, want the booking's amount type", fees[0].Symbol)
	}
}

// The live read: the ledger is asked for the window's booking days, today's
// rows stay for tomorrow, and the read the funding fetch already made serves
// the cashflow fetch of the same sync.
func TestSaxoGetCashflowsReadsTheClosedDays(t *testing.T) {
	today := saxoDay(time.Now())
	yesterday := today.AddDate(0, 0, -1)

	var ledgerCalls atomic.Int32
	s := newSaxoTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/port/v1/clients/me":
			saxoWriteClient(w)
		case "/hist/v1/transactions":
			ledgerCalls.Add(1)
			q := r.URL.Query()
			if q.Get("ClientKey") != "synthetic-client-key" || q.Get("FromDate") != saxoDate(yesterday) || q.Get("ToDate") != saxoDate(yesterday) {
				t.Errorf("ledger query = %s", r.URL.RawQuery)
			}
			fmt.Fprintf(w, `{"Data":[
				{"TransactionType":"CashTransfer","FundingSubType":"DepositFromExternal","Date":%q,"BookedAmount":2500},
				{"TransactionType":"CashTransfer","FundingSubType":"DepositFromExternal","Date":%q,"BookedAmount":9999},
				{"TransactionType":"CashAmount","Date":%q,"BookedAmount":-3,"Bookings":[{"AmountType":"CFD Finance"}]}
			]}`, saxoDate(yesterday), saxoDate(today), saxoDate(yesterday))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	})

	ctx := context.Background()
	fees, err := s.GetFundingFees(ctx, nil, yesterday)
	if err != nil || len(fees) != 1 || fees[0].Amount != -3 {
		t.Fatalf("funding = %v, err = %v", fees, err)
	}
	flows, err := s.GetCashflows(ctx, yesterday)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 1 || flows[0].Amount != 2500 || flows[0].Currency != "EUR" {
		t.Fatalf("flows = %+v, want yesterday's deposit only", flows)
	}
	if n := ledgerCalls.Load(); n != 1 {
		t.Fatalf("ledger read %d times, want once for both fetches", n)
	}
}

// A ledger that cannot be read leaves the sync with no flows, so the gap has to
// reach the sync status rather than only a log line.
func TestSaxoUnreadableLedgerIsReported(t *testing.T) {
	s := newSaxoTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/port/v1/clients/me":
			saxoWriteClient(w)
		case "/port/v1/balances/me":
			fmt.Fprint(w, `{"TotalValue":100,"Currency":"EUR"}`)
		default:
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"ErrorCode":"Forbidden"}`)
		}
	})
	ctx := context.Background()
	if _, err := s.GetBalance(ctx); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	_, err := s.GetCashflows(ctx, saxoDay(time.Now()).AddDate(0, 0, -1))
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("err = %v, want the 403 surfaced", err)
	}
	if !slices.Contains(s.CapabilityWarnings(), "saxo_cashflows_unreadable") {
		t.Fatalf("warnings = %v", s.CapabilityWarnings())
	}
}

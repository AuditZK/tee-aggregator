package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	igTestUser     = "test-user"
	igTestPassword = "test-password-hunter2"
	igTestAccount  = "ABC123"
)

// Both are resolved by runtime type assertion, so a drifted signature would
// silently disable capital-flow capture and paper detection rather than fail
// the build.
var (
	_ CashflowFetcher      = (*IG)(nil)
	_ PaperAccountDetector = (*IG)(nil)
)

func igWriteSession(w http.ResponseWriter) {
	w.Header().Set("CST", "cst-token")
	w.Header().Set("X-SECURITY-TOKEN", "xst-token")
	w.Header().Set("Content-Type", igContentType)
	fmt.Fprintf(w, `{"currentAccountId":%q,"currencyIsoCode":"EUR"}`, igTestAccount)
}

func newIGTest(t *testing.T, demo bool, handler http.HandlerFunc) *IG {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	ig := NewIG(&Credentials{
		APIKey:     "test-key",
		APISecret:  igTestPassword,
		Passphrase: igTestUser,
	}, demo)
	ig.baseURL = srv.URL
	return ig
}

func TestIGExchangeAndPaperDetection(t *testing.T) {
	tests := []struct {
		name         string
		demo         bool
		wantExchange string
	}{
		{name: "live", demo: false, wantExchange: "ig"},
		{name: "demo", demo: true, wantExchange: "ig_demo"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ig := NewIG(&Credentials{APIKey: "k", APISecret: "p", Passphrase: "u"}, tc.demo)
			if got := ig.Exchange(); got != tc.wantExchange {
				t.Fatalf("Exchange() = %q, want %q", got, tc.wantExchange)
			}
			isPaper, err := ig.DetectIsPaper(context.Background())
			if err != nil {
				t.Fatalf("DetectIsPaper returned error: %v", err)
			}
			if isPaper != tc.demo {
				t.Fatalf("DetectIsPaper = %v, want %v", isPaper, tc.demo)
			}
		})
	}
}

func TestIGParseDecimal(t *testing.T) {
	tests := []struct {
		in      string
		want    float64
		wantErr bool
	}{
		{in: "E12.34", want: 12.34},
		{in: "£-5.00", want: -5},
		{in: "-£5.00", want: -5},
		{in: "$0.00", want: 0},
		{in: "+2", want: 2},
		{in: "-1", want: -1},
		{in: "1,234.56", want: 1234.56},
		{in: "", wantErr: true},
		{in: "£", wantErr: true},
		{in: "-", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseIGDecimal(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseIGDecimal(%q) = %v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseIGDecimal(%q) returned error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("ParseIGDecimal(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// The session's account is the one /positions and /history/transactions report
// on. Reading equity off any other row pairs one account's balance with
// another's trades.
func TestIGGetBalanceUsesSessionAccount(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/accounts":
			fmt.Fprint(w, `{"accounts":[
				{"accountId":"OTHER","currency":"GBP","balance":{"balance":9999,"profitLoss":1,"available":9999}},
				{"accountId":"ABC123","currency":"EUR","balance":{"balance":1000,"profitLoss":250,"available":700}}
			]}`)
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
	})

	bal, err := ig.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance returned error: %v", err)
	}
	if bal.Equity != 1250 {
		t.Fatalf("Equity = %v, want 1250 (balance + profitLoss)", bal.Equity)
	}
	if bal.Available != 700 {
		t.Fatalf("Available = %v, want 700", bal.Available)
	}
	if bal.UnrealizedPnL != 250 {
		t.Fatalf("UnrealizedPnL = %v, want 250", bal.UnrealizedPnL)
	}
	if bal.Currency != "EUR" {
		t.Fatalf("Currency = %q, want EUR", bal.Currency)
	}
}

func TestIGGetBalanceSessionAccountMissing(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/accounts":
			fmt.Fprint(w, `{"accounts":[{"accountId":"OTHER","currency":"GBP","balance":{"balance":9999,"profitLoss":1,"available":9999}}]}`)
		}
	})

	if _, err := ig.GetBalance(context.Background()); err == nil {
		t.Fatal("GetBalance must fail rather than fall back to an unrelated account")
	}
}

// Interest and dividend lines are cash transactions but are P&L, not capital.
// Booking one as a deposit produces a phantom inflow that craters TWR.
func TestIGGetCashflowsClassifiesByTransactionCode(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/history/transactions":
			fmt.Fprint(w, `{"transactions":[
				{"transactionType":"DEPO","cashTransaction":true,"profitAndLoss":"E1000.00","currency":"EUR","dateUtc":"2026-07-01T10:00:00"},
				{"transactionType":"WITH","cashTransaction":true,"profitAndLoss":"E-500.00","currency":"EUR","dateUtc":"2026-07-02T10:00:00"},
				{"transactionType":"WITH","cashTransaction":true,"profitAndLoss":"E500.00","currency":"EUR","dateUtc":"2026-07-03T10:00:00"},
				{"transactionType":"CASHIN","cashTransaction":true,"profitAndLoss":"E250.00","currency":"EUR","dateUtc":"2026-07-04T10:00:00"},
				{"transactionType":"DIVIDEND","cashTransaction":true,"profitAndLoss":"E10.00","currency":"EUR","dateUtc":"2026-07-05T10:00:00"},
				{"transactionType":"INTEREST","cashTransaction":true,"profitAndLoss":"E3.00","currency":"EUR","dateUtc":"2026-07-06T10:00:00"},
				{"transactionType":"DEAL","cashTransaction":false,"profitAndLoss":"E42.00","currency":"EUR","dateUtc":"2026-07-07T10:00:00"}
			],"metaData":{"pageData":{"totalPages":1}}}`)
		}
	})

	flows, err := ig.GetCashflows(context.Background(), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetCashflows returned error: %v", err)
	}

	want := []float64{1000, -500, -500, 250}
	if len(flows) != len(want) {
		t.Fatalf("got %d cashflows, want %d: %+v", len(flows), len(want), flows)
	}
	for idx, w := range want {
		if flows[idx].Amount != w {
			t.Fatalf("cashflow[%d].Amount = %v, want %v", idx, flows[idx].Amount, w)
		}
	}
}

// IG localises transactionType for deals to the account language but keeps the
// cash codes in English. Matching deals on a literal label would silently drop
// every trade on a non-English account.
func TestIGGetTradesIgnoresCashAndLocalisedLabels(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/history/transactions":
			fmt.Fprint(w, `{"transactions":[
				{"transactionType":"DEPO","cashTransaction":true,"profitAndLoss":"E1000.00","size":"0","closeLevel":"0","currency":"EUR","dateUtc":"2026-07-01T10:00:00","reference":"CASH1"},
				{"transactionType":"DEAL","cashTransaction":false,"profitAndLoss":"E50.00","size":"+2","closeLevel":"1.2345","currency":"EUR","dateUtc":"2026-07-02T10:00:00","reference":"D1","instrumentName":"EUR/USD"},
				{"transactionType":"Handel","cashTransaction":false,"profitAndLoss":"E-20.00","size":"-1","closeLevel":"1.5000","currency":"EUR","dateUtc":"2026-07-03T10:00:00","reference":"D2","instrumentName":"GBP/USD"}
			],"metaData":{"pageData":{"totalPages":1}}}`)
		}
	})

	trades, err := ig.GetTrades(context.Background(),
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetTrades returned error: %v", err)
	}
	if len(trades) != 2 {
		t.Fatalf("got %d trades, want 2 (cash excluded, localised label kept): %+v", len(trades), trades)
	}

	if trades[0].Side != "buy" || trades[0].Quantity != 2 || trades[0].RealizedPnL != 50 {
		t.Fatalf("trade[0] = %+v, want buy/2/50", trades[0])
	}
	if trades[1].Side != "sell" || trades[1].Quantity != 1 || trades[1].RealizedPnL != -20 {
		t.Fatalf("trade[1] = %+v, want sell/1/-20", trades[1])
	}
}

func TestIGTransactionsPaginate(t *testing.T) {
	var pages atomic.Int32
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/history/transactions":
			page := r.URL.Query().Get("pageNumber")
			pages.Add(1)
			fmt.Fprintf(w, `{"transactions":[
				{"transactionType":"DEPO","cashTransaction":true,"profitAndLoss":"E%s00.00","currency":"EUR","dateUtc":"2026-07-01T10:00:00"}
			],"metaData":{"pageData":{"totalPages":2}}}`, page)
		}
	})

	flows, err := ig.GetCashflows(context.Background(), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetCashflows returned error: %v", err)
	}
	if got := pages.Load(); got != 2 {
		t.Fatalf("fetched %d pages, want 2", got)
	}
	if len(flows) != 2 {
		t.Fatalf("got %d cashflows across pages, want 2", len(flows))
	}
}

// IG's 6h token TTL is a floor, not a guarantee: a login elsewhere invalidates
// the pair early. A rejected token must re-authenticate, not fail the sync.
func TestIGReLoginsOnRejectedSessionToken(t *testing.T) {
	var logins, accountCalls atomic.Int32

	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			logins.Add(1)
			igWriteSession(w)
		case "/accounts":
			if accountCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprint(w, `{"errorCode":"error.security.client-token-invalid"}`)
				return
			}
			fmt.Fprint(w, `{"accounts":[{"accountId":"ABC123","currency":"EUR","balance":{"balance":100,"profitLoss":0,"available":100}}]}`)
		}
	})

	if _, err := ig.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance should recover from a rejected token: %v", err)
	}
	if got := logins.Load(); got != 2 {
		t.Fatalf("logins = %d, want 2 (initial + re-auth)", got)
	}
}

// A rate-limited or unavailable login is not a bad password: connection create
// defers on ErrTransient instead of rejecting a valid account.
func TestIGLoginRateLimitIsTransient(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"errorCode":"error.public-api.exceeded-account-allowance"}`)
	})

	err := ig.TestConnection(context.Background())
	if err == nil {
		t.Fatal("expected error on rate-limited login")
	}
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("expected ErrTransient, got %v", err)
	}
}

func TestIGLoginBadCredentialsIsNotTransient(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"errorCode":"error.security.invalid-details"}`)
	})

	err := ig.TestConnection(context.Background())
	if err == nil {
		t.Fatal("expected error on bad credentials")
	}
	if errors.Is(err, ErrTransient) {
		t.Fatalf("bad credentials must not be transient: %v", err)
	}
}

// LOG-001: the login POSTs the account password, so an upstream that echoes the
// request back must not spill it into an error that reaches a log or a client.
func TestIGLoginErrorDoesNotLeakPassword(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"errorCode":"error.request.invalid","echo":{"password":%q}}`, igTestPassword)
	})

	err := ig.TestConnection(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), igTestPassword) {
		t.Fatalf("error leaked the account password: %v", err)
	}
}

func TestIGGetPositions(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/positions":
			fmt.Fprint(w, `{"positions":[
				{"market":{"instrumentName":"EUR/USD","instrumentType":"CURRENCIES","bid":1.10,"offer":1.11},
				 "position":{"dealId":"D1","direction":"BUY","size":2,"level":1.00,"contractSize":1}},
				{"market":{"instrumentName":"Gold","instrumentType":"COMMODITIES","bid":2000,"offer":2001},
				 "position":{"dealId":"D2","direction":"SELL","size":1,"level":2010,"contractSize":1}},
				{"market":{"instrumentName":"Closed","instrumentType":"SHARES","bid":1,"offer":1},
				 "position":{"dealId":"D3","direction":"BUY","size":0,"level":1,"contractSize":1}}
			]}`)
		}
	})

	positions, err := ig.GetPositions(context.Background())
	if err != nil {
		t.Fatalf("GetPositions returned error: %v", err)
	}
	if len(positions) != 2 {
		t.Fatalf("got %d positions, want 2 (zero-size dropped)", len(positions))
	}

	// Long marks at the bid: (1.10 - 1.00) * 2 = 0.20
	if positions[0].Side != "long" || positions[0].MarketType != MarketForex {
		t.Fatalf("position[0] = %+v, want long/forex", positions[0])
	}
	if diff := positions[0].UnrealizedPnL - 0.20; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("position[0].UnrealizedPnL = %v, want 0.20", positions[0].UnrealizedPnL)
	}

	// Short marks at the offer: (2010 - 2001) * 1 = 9
	if positions[1].Side != "short" || positions[1].MarketType != MarketCommodities {
		t.Fatalf("position[1] = %+v, want short/commodities", positions[1])
	}
	if positions[1].UnrealizedPnL != 9 {
		t.Fatalf("position[1].UnrealizedPnL = %v, want 9", positions[1].UnrealizedPnL)
	}
}

// The daily scheduler drives balance, positions and trades off one cached
// connector, so a cold connection opens concurrently. Logging in once per
// caller would burn IG's per-account login allowance and race the token pair.
func TestIGConcurrentCallersLoginOnce(t *testing.T) {
	var logins atomic.Int32
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			logins.Add(1)
			igWriteSession(w)
		case "/accounts":
			fmt.Fprint(w, `{"accounts":[{"accountId":"ABC123","currency":"EUR","balance":{"balance":100,"profitLoss":0,"available":100}}]}`)
		}
	})

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ig.GetBalance(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent GetBalance returned error: %v", err)
	}
	if got := logins.Load(); got != 1 {
		t.Fatalf("logins = %d, want 1 across concurrent callers", got)
	}
}

func TestIGSessionIsReusedAcrossCalls(t *testing.T) {
	var logins atomic.Int32
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			logins.Add(1)
			igWriteSession(w)
		case "/accounts":
			fmt.Fprint(w, `{"accounts":[{"accountId":"ABC123","currency":"EUR","balance":{"balance":100,"profitLoss":0,"available":100}}]}`)
		}
	})

	for range 3 {
		if _, err := ig.GetBalance(context.Background()); err != nil {
			t.Fatalf("GetBalance returned error: %v", err)
		}
	}
	if got := logins.Load(); got != 1 {
		t.Fatalf("logins = %d, want 1 (session cached across calls)", got)
	}
}

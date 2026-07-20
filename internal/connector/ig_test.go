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
	w.Header().Set("Content-Type", igContentType)
	fmt.Fprintf(w, `{"accountId":%q,"clientId":"C1","oauthToken":{"access_token":"tok-1","token_type":"Bearer","expires_in":"60"}}`, igTestAccount)
}

const igTestAccountsJSON = `{"accounts":[{"accountId":"ABC123","accountType":"CFD","currency":"EUR","balance":{"balance":1,"profitLoss":0,"available":1}}]}`

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

		// IG renders these for display in the account's locale, so the same
		// amount arrives both ways. Reading "1.234,56" with the English
		// convention yields 1.23456 — a thousandfold error nothing downstream
		// can catch.
		{in: "1.234,56", want: 1234.56},
		{in: "E1.234,56", want: 1234.56},
		{in: "-1.234,56", want: -1234.56},
		{in: "12,34", want: 12.34},
		{in: "1.234.567,89", want: 1234567.89},
		{in: "1,234,567.89", want: 1234567.89},
		// A lone separator with three digits behind it is ambiguous; grouping
		// is IG's English form and the safer miss.
		{in: "1,234", want: 1234},
		{in: "1.234", want: 1234},
		// Forex levels keep their fraction: not three digits, so not grouping.
		{in: "1.1050", want: 1.1050},
		{in: "0.85", want: 0.85},
		{in: ".", wantErr: true},
		{in: ",", wantErr: true},
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

// IG's published sample timestamps carry no zone designator, but the exact
// shape is unconfirmed against a live account, so the parser accepts RFC3339
// too. Both must land on the same UTC instant — a silently mis-parsed date
// files a trade under the wrong day.
func TestIGParseTime(t *testing.T) {
	want := time.Date(2026, 7, 15, 14, 30, 5, 0, time.UTC)

	tests := []struct {
		name    string
		in      string
		want    time.Time
		wantErr bool
	}{
		{name: "no zone designator", in: "2026-07-15T14:30:05", want: want},
		{name: "rfc3339 utc", in: "2026-07-15T14:30:05Z", want: want},
		{name: "rfc3339 offset", in: "2026-07-15T16:30:05+02:00", want: want},
		{name: "empty", in: "", wantErr: true},
		{name: "garbage", in: "not-a-date", wantErr: true},
		{name: "date only", in: "2026-07-15", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseIGTime(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseIGTime(%q) = %v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseIGTime(%q) returned error: %v", tc.in, err)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("parseIGTime(%q) = %v, want %v", tc.in, got, tc.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("parseIGTime(%q) location = %v, want UTC", tc.in, got.Location())
			}
		})
	}
}

// Replays a payload captured from a live demo account: a long Crypto 10 Index
// CFD, priced in USD on a EUR account.
//
// Two things it pins. The mark-to-market is computed against the bid for a
// long, and against the level the position actually opened at — getting either
// wrong is invisible in a synthetic fixture where the numbers are round. And
// the result is in the INSTRUMENT's currency: IG reported -70.79 on the
// account for what computes to -79.79 here, the two differing by the EUR/USD
// rate. Position carries no currency, so this figure is per-instrument only
// and must never be summed across them — the account's own profitLoss is the
// aggregate, and it is what the pipeline reads.
func TestIGPositionMathMatchesLiveCapture(t *testing.T) {
	ig := newIGTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/accounts":
			fmt.Fprint(w, igTestAccountsJSON)
		case "/positions":
			fmt.Fprint(w, `{"positions":[{
				"position":{"contractSize":1.0,"dealId":"DIAAAAX4C4YUDAC","size":1.0,
					"direction":"BUY","level":13072.18,"currency":"USD"},
				"market":{"instrumentName":"Crypto 10 Index","instrumentType":"CURRENCIES",
					"bid":12992.39,"offer":13072.39}
			}]}`)
		}
	})

	positions, err := ig.GetPositions(context.Background())
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("got %d positions, want 1", len(positions))
	}

	p := positions[0]
	if p.Side != "long" || p.Size != 1 || p.EntryPrice != 13072.18 {
		t.Fatalf("position = %+v, want long/1/13072.18", p)
	}
	// A long exits at the bid, so that is what it marks against.
	if p.MarkPrice != 12992.39 {
		t.Fatalf("MarkPrice = %v, want the bid 12992.39", p.MarkPrice)
	}
	if diff := p.UnrealizedPnL - -79.79; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("UnrealizedPnL = %v, want -79.79 (instrument currency)", p.UnrealizedPnL)
	}
	// IG types this crypto index as CURRENCIES; the account holds a CFD.
	if p.MarketType != MarketCFD {
		t.Fatalf("MarketType = %q, want %q", p.MarketType, MarketCFD)
	}
}

// instrumentType names the UNDERLYING, not the instrument — a live demo
// returned "CURRENCIES" for "Crypto 10 Index". Routing on it filed a crypto
// index under forex, and split positions away from the CFD bucket that the
// same account's trades and equity land in.
func TestIGMarketTypeIsAlwaysCFD(t *testing.T) {
	for _, instrumentType := range []string{
		"CURRENCIES", "currencies", " COMMODITIES ", "SHARES", "INDICES", "BINARY", "",
	} {
		t.Run(instrumentType, func(t *testing.T) {
			if got := igMarketType(instrumentType); got != MarketCFD {
				t.Fatalf("igMarketType(%q) = %q, want %q — the account holds a CFD whatever the underlying",
					instrumentType, got, MarketCFD)
			}
		})
	}
}

// RawTransactions is what the probe reads to settle the ledger questions the
// docs will not answer, so it must hand back every line unfiltered — including
// the cash and unclassifiable ones the parsed views drop.
func TestIGRawTransactionsKeepsEverything(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/accounts":
			fmt.Fprint(w, igTestAccountsJSON)
		case "/history/transactions":
			fmt.Fprint(w, `{"transactions":[
				{"transactionType":"DEPO","cashTransaction":true,"profitAndLoss":"E1000.00","size":"0","currency":"EUR","dateUtc":"2026-07-01T10:00:00"},
				{"transactionType":"UNKNOWN_CODE","cashTransaction":true,"profitAndLoss":"E7.00","size":"0","currency":"EUR","dateUtc":"2026-07-02T10:00:00"},
				{"transactionType":"DEAL","cashTransaction":false,"profitAndLoss":"unparseable","size":"+1","openLevel":"1.10","closeLevel":"1.20","currency":"EUR","dateUtc":"2026-07-03T10:00:00"}
			],"metaData":{"pageData":{"totalPages":1}}}`)
		}
	})

	ctx := context.Background()
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)

	raw, err := ig.RawTransactions(ctx, start, end)
	if err != nil {
		t.Fatalf("RawTransactions: %v", err)
	}
	if len(raw) != 3 {
		t.Fatalf("got %d raw lines, want 3 (nothing filtered): %+v", len(raw), raw)
	}
	if raw[1].TransactionType != "UNKNOWN_CODE" {
		t.Fatalf("unclassifiable code was dropped: %+v", raw)
	}
	if raw[2].OpenLevel != "1.10" {
		t.Fatalf("OpenLevel not surfaced: %+v", raw[2])
	}

	// The parsed views drop exactly what the raw view keeps.
	flows, err := ig.GetCashflows(ctx, start)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 1 {
		t.Fatalf("got %d cashflows, want 1 (unknown code not classified): %+v", len(flows), flows)
	}
	trades, err := ig.GetTrades(ctx, start, end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 0 {
		t.Fatalf("got %d trades, want 0 (unparseable P&L dropped): %+v", len(trades), trades)
	}
}

// Every read is pinned to the selected eligible account. Reading any other row
// pairs one account's balance with another's trades — and the login's default
// account is routinely an ineligible share-dealing one (that default is
// exactly what broke the v1/v2 CST login wholesale).
func TestIGGetBalanceReadsSelectedEligibleAccount(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/accounts":
			fmt.Fprint(w, `{"accounts":[
				{"accountId":"OTHER","accountType":"PHYSICAL","currency":"GBP","balance":{"balance":9999,"profitLoss":1,"available":9999}},
				{"accountId":"ABC123","accountType":"CFD","currency":"EUR","balance":{"balance":1000,"profitLoss":250,"available":700}}
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

func TestIGGetBalanceNoEligibleAccount(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/accounts":
			fmt.Fprint(w, `{"accounts":[{"accountId":"OTHER","accountType":"PHYSICAL","currency":"GBP","balance":{"balance":9999,"profitLoss":1,"available":9999}}]}`)
		}
	})

	_, err := ig.GetBalance(context.Background())
	if err == nil {
		t.Fatal("GetBalance must fail rather than read an account the Web API does not serve")
	}
	if !strings.Contains(err.Error(), "CFD or spread-bet") {
		t.Fatalf("error does not name the account requirement: %v", err)
	}
}

// A pinned account must drive both the request identity and the row that is
// read — pinning exists precisely because the login's default identity can be
// refused wholesale, taking account discovery down with it.
func TestIGPinnedAccount(t *testing.T) {
	var sawIdentity string
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/accounts":
			sawIdentity = r.Header.Get("IG-ACCOUNT-ID")
			fmt.Fprint(w, `{"accounts":[
				{"accountId":"DEF1","accountType":"PHYSICAL","preferred":true,"currency":"GBP","balance":{"balance":1,"profitLoss":0,"available":1}},
				{"accountId":"PIN1","accountType":"CFD","currency":"EUR","balance":{"balance":500,"profitLoss":25,"available":400}}
			]}`)
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(srv.Close)
	ig := NewIG(&Credentials{APIKey: "k", APISecret: "pw", Passphrase: "user1:PIN1"}, true)
	ig.baseURL = srv.URL

	bal, err := ig.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if sawIdentity != "PIN1" {
		t.Fatalf("accounts listed under identity %q, want PIN1 (default identity may be refused)", sawIdentity)
	}
	if bal.Equity != 525 || bal.Currency != "EUR" {
		t.Fatalf("balance = %+v, want the pinned row (525 EUR)", bal)
	}

	id, err := ig.ensureAccountID(context.Background())
	if err != nil {
		t.Fatalf("ensureAccountID: %v", err)
	}
	if id != "PIN1" {
		t.Fatalf("ensureAccountID = %q, want PIN1", id)
	}
}

// Discovery under a refused default identity is the one failure only the user
// can resolve; the error must name both ways out, not read as a broken key.
func TestIGBlockedDefaultAccountNamesTheFix(t *testing.T) {
	ig := newIGTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/accounts":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"errorCode":"error.public-api.failure.stockbroking-not-supported"}`)
		}
	})

	_, err := ig.GetBalance(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "identifier:ACCOUNTID") || !strings.Contains(err.Error(), "default") {
		t.Fatalf("error does not name the ways out: %v", err)
	}
}

// The user's preferred flag decides among eligible accounts; an ineligible
// preferred account (share dealing marked as default) must not hijack the
// selection.
func TestIGSelectAccount(t *testing.T) {
	mk := func(id, typ string, pref bool) igAccount {
		return igAccount{AccountID: id, AccountType: typ, Preferred: pref}
	}

	sel, err := selectIGAccount([]igAccount{mk("S", "PHYSICAL", true), mk("A", "CFD", false), mk("B", "SPREADBET", true)})
	if err != nil {
		t.Fatalf("selectIGAccount: %v", err)
	}
	if sel.AccountID != "B" {
		t.Fatalf("selected %q, want B (preferred eligible beats first eligible)", sel.AccountID)
	}

	sel, err = selectIGAccount([]igAccount{mk("S", "PHYSICAL", true), mk("A", "CFD", false), mk("B", "SPREADBET", false)})
	if err != nil {
		t.Fatalf("selectIGAccount: %v", err)
	}
	if sel.AccountID != "A" {
		t.Fatalf("selected %q, want A (first eligible when none preferred)", sel.AccountID)
	}

	if _, err := selectIGAccount([]igAccount{mk("S", "PHYSICAL", true)}); err == nil {
		t.Fatal("want error when no eligible account exists")
	}
}

// Interest and dividend lines are cash transactions but are P&L, not capital.
// Booking one as a deposit produces a phantom inflow that craters TWR.
func TestIGGetCashflowsClassifiesByTransactionCode(t *testing.T) {
	ig := newIGTest(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			igWriteSession(w)
		case "/accounts":
			fmt.Fprint(w, igTestAccountsJSON)
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
		case "/accounts":
			fmt.Fprint(w, igTestAccountsJSON)
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
		case "/accounts":
			fmt.Fprint(w, igTestAccountsJSON)
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
			fmt.Fprint(w, `{"accounts":[{"accountId":"ABC123","accountType":"CFD","currency":"EUR","balance":{"balance":100,"profitLoss":0,"available":100}}]}`)
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

// Observed against a real demo account: IG refuses the session outright for a
// share-dealing login. Reported as a bare status it reads as a bad password,
// which sends the holder of a working account chasing the wrong thing.
func TestIGShareDealingAccountIsNamed(t *testing.T) {
	ig := newIGTest(t, true, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"errorCode":"error.public-api.failure.stockbroking-not-supported"}`)
	})

	err := ig.TestConnection(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "CFD or spread-bet") {
		t.Fatalf("error does not say what to do about it: %v", err)
	}
	if errors.Is(err, ErrTransient) {
		t.Fatalf("account type is permanent, retrying cannot fix it: %v", err)
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
		case "/accounts":
			fmt.Fprint(w, igTestAccountsJSON)
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
	if positions[0].Side != "long" || positions[0].MarketType != MarketCFD {
		t.Fatalf("position[0] = %+v, want long/cfd", positions[0])
	}
	if diff := positions[0].UnrealizedPnL - 0.20; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("position[0].UnrealizedPnL = %v, want 0.20", positions[0].UnrealizedPnL)
	}

	// Short marks at the offer: (2010 - 2001) * 1 = 9
	if positions[1].Side != "short" || positions[1].MarketType != MarketCFD {
		t.Fatalf("position[1] = %+v, want short/cfd", positions[1])
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
			fmt.Fprint(w, `{"accounts":[{"accountId":"ABC123","accountType":"CFD","currency":"EUR","balance":{"balance":100,"profitLoss":0,"available":100}}]}`)
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
			fmt.Fprint(w, `{"accounts":[{"accountId":"ABC123","accountType":"CFD","currency":"EUR","balance":{"balance":100,"profitLoss":0,"available":100}}]}`)
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

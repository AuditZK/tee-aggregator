package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Synthetic credentials: the shape of the venue's, never issued by it.
const (
	tastytradeTestRefresh = "aaaa.bbbb.cccc"
	tastytradeTestSecret  = "synthetic-client-secret"
	tastytradeTestAccount = "5ZZ00001"
	tastytradeTestAccess  = "access-token-one"
)

// tastytradeFake is a venue double: it signs in, checks what every request
// must carry, and serves one fixed body per path.
type tastytradeFake struct {
	t        *testing.T
	routes   map[string]string
	signIns  atomic.Int32
	requests atomic.Int32
	refuse   atomic.Bool
	// expireFirst answers the first data request with 401, as an expired token.
	expireFirst atomic.Bool
	lastQuery   atomic.Value
}

func (f *tastytradeFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("User-Agent") != tastytradeUserAgent {
		f.t.Errorf("%s %s sent User-Agent %q", r.Method, r.URL.Path, r.Header.Get("User-Agent"))
	}
	if r.URL.Path == "/oauth/token" {
		f.signIns.Add(1)
		var body map[string]string
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		if body["grant_type"] != "refresh_token" || body["scope"] != "read" {
			f.t.Errorf("sign-in body = %v, want a refresh_token grant limited to read", body)
		}
		if f.refuse.Load() || body["refresh_token"] != tastytradeTestRefresh || body["client_secret"] != tastytradeTestSecret {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error_code":"invalid_grant","error_description":"Invalid JWT ` + body["refresh_token"] + `"}`))
			return
		}
		_, _ = w.Write([]byte(`{"access_token":"` + tastytradeTestAccess + `","token_type":"Bearer","expires_in":900}`))
		return
	}
	f.requests.Add(1)
	if f.expireFirst.CompareAndSwap(true, false) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":"token_invalid","message":"This token is invalid or has expired"}}`))
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+tastytradeTestAccess {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":"token_invalid","message":"no token"}}`))
		return
	}
	f.lastQuery.Store(r.URL.Query())
	body, ok := f.routes[r.URL.Path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":"not_found","message":"no such account ` + tastytradeTestAccount + `"}}`))
		return
	}
	_, _ = w.Write([]byte(body))
}

func newTastytradeFake(t *testing.T, routes map[string]string) (*Tastytrade, *tastytradeFake) {
	t.Helper()
	fake := &tastytradeFake{t: t, routes: routes}
	if _, ok := routes["/customers/me/accounts"]; !ok {
		routes["/customers/me/accounts"] = `{"data":{"items":[{"account":{"account-number":"` + tastytradeTestAccount + `","is-closed":false},"authority-level":"owner"}]}}`
	}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	c := NewTastytrade(&Credentials{APIKey: tastytradeTestRefresh, APISecret: tastytradeTestSecret})
	c.baseURL = srv.URL
	c.guard = &signInGuard{}
	return c, fake
}

const tastytradeTestBase = "/accounts/" + tastytradeTestAccount

func TestTastytradeGetBalance(t *testing.T) {
	c, fake := newTastytradeFake(t, map[string]string{
		tastytradeTestBase + "/balances": `{"data":{"currency":"USD","net-liquidating-value":"25000.50","cash-balance":"9000.0","derivative-buying-power":"12000.25"},"context":"x"}`,
		tastytradeTestBase + "/positions": `{"data":{"items":[
			{"symbol":"ACME","instrument-type":"Equity","quantity":"100","quantity-direction":"Long","average-open-price":"50.0","mark":"52.5","multiplier":1},
			{"symbol":"ACME  310117C00060000","instrument-type":"Equity Option","quantity":"2","quantity-direction":"Short","average-open-price":"1.50","mark":"1.10","multiplier":100},
			{"symbol":"/ZZH1","instrument-type":"Future","quantity":"1","quantity-direction":"Long","average-open-price":"4000","multiplier":50},
			{"symbol":"GONE","instrument-type":"Equity","quantity":"0","quantity-direction":"Zero","average-open-price":"9"}
		]}}`,
	})
	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !approxEq(bal.Equity, 25000.50) || !approxEq(bal.Available, 12000.25) || bal.Currency != "USD" {
		t.Errorf("equity, available, currency = %v, %v, %q", bal.Equity, bal.Available, bal.Currency)
	}
	// Long shares up 2.5 a share, short calls down 0.40 a contract of 100; the
	// future the venue did not mark adds nothing.
	if !approxEq(bal.UnrealizedPnL, 250+80) {
		t.Errorf("unrealized = %v, want 330", bal.UnrealizedPnL)
	}

	positions, err := c.GetPositions(context.Background())
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if len(positions) != 3 {
		t.Fatalf("got %d positions, want the three still open", len(positions))
	}
	if p := positions[1]; p.Side != "short" || p.MarketType != MarketOptions || !approxEq(p.Size, 2) || !approxEq(p.UnrealizedPnL, 80) {
		t.Errorf("short option = %+v", p)
	}
	if p := positions[2]; p.MarketType != MarketFutures || p.MarkPrice != 0 || p.UnrealizedPnL != 0 {
		t.Errorf("unmarked future = %+v", p)
	}
	if fake.signIns.Load() != 1 {
		t.Errorf("signed in %d times across three reads, want once", fake.signIns.Load())
	}
}

func TestTastytradeGetBalance_AvailableNeverExceedsEquity(t *testing.T) {
	c, _ := newTastytradeFake(t, map[string]string{
		tastytradeTestBase + "/balances":  `{"data":{"net-liquidating-value":"800","derivative-buying-power":"1500"}}`,
		tastytradeTestBase + "/positions": `{"data":{"items":[]}}`,
	})
	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !approxEq(bal.Available, 800) {
		t.Errorf("available = %v, want it held to the equity", bal.Available)
	}
}

func TestTastytrade_ExpiredTokenIsRenewedOnce(t *testing.T) {
	c, fake := newTastytradeFake(t, map[string]string{
		tastytradeTestBase + "/balances":  `{"data":{"net-liquidating-value":"100"}}`,
		tastytradeTestBase + "/positions": `{"data":{"items":[]}}`,
	})
	if _, err := c.accountNumber(context.Background()); err != nil {
		t.Fatalf("accountNumber: %v", err)
	}
	fake.expireFirst.Store(true)
	if _, err := c.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance after an expired token: %v", err)
	}
	if fake.signIns.Load() != 2 {
		t.Errorf("signed in %d times, want the first and one renewal", fake.signIns.Load())
	}
}

func TestTastytrade_AccountSelection(t *testing.T) {
	two := `{"data":{"items":[
		{"account":{"account-number":"5ZZ00001","is-closed":false}},
		{"account":{"account-number":"5ZZ00002","is-closed":false}},
		{"account":{"account-number":"5ZZ00003","is-closed":true}}
	]}}`
	cases := []struct {
		name, accounts, want, pick string
		refused                    bool
	}{
		{name: "the only open account", accounts: `{"data":{"items":[{"account":{"account-number":"5ZZ00009","is-closed":false}},{"account":{"account-number":"5ZZ00003","is-closed":true}}]}}`, pick: "5ZZ00009"},
		{name: "several accounts and none named", accounts: two, refused: true},
		{name: "the one named, whatever its case", accounts: two, want: " 5zz00002 ", pick: "5ZZ00002"},
		{name: "a closed account named", accounts: two, want: "5ZZ00003", refused: true},
		{name: "an account of someone else", accounts: two, want: "5ZZ77777", refused: true},
		{name: "no account at all", accounts: `{"data":{"items":[]}}`, refused: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newTastytradeFake(t, map[string]string{"/customers/me/accounts": tc.accounts})
			c.wantAccount = NewTastytrade(&Credentials{Passphrase: tc.want}).wantAccount
			got, err := c.accountNumber(context.Background())
			if tc.refused {
				if err == nil {
					t.Fatalf("resolved %q, want a refusal", got)
				}
				return
			}
			if err != nil || got != tc.pick {
				t.Fatalf("account = %q, %v, want %q", got, err, tc.pick)
			}
		})
	}
}

func TestTastytradeGetTrades_PagesAndFees(t *testing.T) {
	var pages atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			_, _ = w.Write([]byte(`{"access_token":"` + tastytradeTestAccess + `"}`))
		case tastytradeTestBase + "/transactions":
			q := r.URL.Query()
			if got := q["types[]"]; len(got) != 1 || got[0] != "Trade" {
				t.Errorf("types[] = %v", got)
			}
			if q.Get("start-at") != "2031-03-01T00:00:00Z" || q.Get("end-at") != "2031-03-02T00:00:00Z" || q.Get("sort") != "Asc" || q.Get("per-page") != "250" {
				t.Errorf("query = %s", r.URL.RawQuery)
			}
			pages.Add(1)
			switch q.Get("page-offset") {
			case "0":
				_, _ = w.Write([]byte(`{"data":{"items":[
					{"id":11,"transaction-type":"Trade","action":"Buy to Open","symbol":"ACME","instrument-type":"Equity","quantity":"10","price":"50.0","value":"500.0","value-effect":"Debit","executed-at":"2031-03-01T15:30:00.123+00:00",
					 "commission":"1.00","commission-effect":"Debit","clearing-fees":"0.08","clearing-fees-effect":"Debit","regulatory-fees":"0.02","regulatory-fees-effect":"Debit"}
				]},"pagination":{"per-page":250,"page-offset":0,"total-pages":2}}`))
			case "1":
				_, _ = w.Write([]byte(`{"data":{"items":[
					{"id":12,"transaction-type":"Trade","action":"Sell to Close","symbol":"ACME  310117C00060000","instrument-type":"Equity Option","quantity":"2","price":"1.25","value":"250.0","value-effect":"Credit","executed-at":"2031-03-01T16:00:00Z",
					 "commission":"0.0","commission-effect":"None","regulatory-fees":"0.05","regulatory-fees-effect":"Debit","other-charge":"0.01","other-charge-effect":"Credit"},
					{"id":13,"transaction-type":"Trade","action":"","symbol":"ODD","quantity":"1","price":"1","value":"1","executed-at":"2031-03-01T16:10:00Z"}
				]},"pagination":{"per-page":250,"page-offset":1,"total-pages":2}}`))
			default:
				t.Errorf("unexpected page %q", q.Get("page-offset"))
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := NewTastytrade(&Credentials{APIKey: tastytradeTestRefresh, APISecret: tastytradeTestSecret, Passphrase: tastytradeTestAccount})
	c.baseURL, c.guard, c.account = srv.URL, &signInGuard{}, tastytradeTestAccount

	day := time.Date(2031, time.March, 1, 0, 0, 0, 0, time.UTC)
	trades, err := c.GetTrades(context.Background(), day, day.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 2 || pages.Load() != 2 {
		t.Fatalf("got %d trades over %d pages, want 2 over 2", len(trades), pages.Load())
	}
	if tr := trades[0]; tr.ID != "11" || tr.Side != "buy" || tr.MarketType != MarketStocks || !approxEq(tr.Notional(), 500) || !approxEq(tr.Fee, 1.10) {
		t.Errorf("first trade = %+v", tr)
	}
	if !trades[0].Timestamp.Equal(day.Add(15*time.Hour + 30*time.Minute + 123*time.Millisecond)) {
		t.Errorf("first trade stamped %v", trades[0].Timestamp)
	}
	// The option leg trades in dollars of premium, and a credited charge comes
	// off the fees.
	if tr := trades[1]; tr.Side != "sell" || tr.MarketType != MarketOptions || !approxEq(tr.Notional(), 250) || !approxEq(tr.Fee, 0.04) {
		t.Errorf("second trade = %+v", tr)
	}
}

func TestTastytradeGetCashflows_CapitalOnly(t *testing.T) {
	c, fake := newTastytradeFake(t, map[string]string{
		tastytradeTestBase + "/transactions": `{"data":{"items":[
			{"id":1,"transaction-type":"Money Movement","transaction-sub-type":"Deposit","net-value":"5000.0","net-value-effect":"Credit","executed-at":"2031-03-01T14:00:00Z"},
			{"id":2,"transaction-type":"Money Movement","transaction-sub-type":"Dividend","symbol":"ACME","net-value":"12.5","net-value-effect":"Credit","executed-at":"2031-03-02T14:00:00Z"},
			{"id":3,"transaction-type":"Money Movement","transaction-sub-type":"Debit Interest","net-value":"3.1","net-value-effect":"Debit","executed-at":"2031-03-03T14:00:00Z"},
			{"id":4,"transaction-type":"Money Movement","transaction-sub-type":"Mark to Market","symbol":"/ZZH1","net-value":"140.0","net-value-effect":"Credit","executed-at":"2031-03-03T22:00:00Z"},
			{"id":5,"transaction-type":"Money Movement","transaction-sub-type":"Withdrawal","net-value":"750.0","net-value-effect":"Debit","executed-at":"2031-03-04T14:00:00Z"},
			{"id":6,"transaction-type":"Money Movement","transaction-sub-type":"Transfer","net-value":"200.0","net-value-effect":"Credit","executed-at":"2031-03-05T14:00:00Z"},
			{"id":7,"transaction-type":"Receive Deliver","transaction-sub-type":"Expiration","symbol":"ACME  310117C00060000","quantity":"2","net-value":"0.0","net-value-effect":"None","executed-at":"2031-03-06T21:00:00Z"},
			{"id":8,"transaction-type":"Money Movement","transaction-sub-type":"Withdrawal","net-value":"50.0","net-value-effect":"Credit","executed-at":"2031-03-07T14:00:00Z"}
		]},"pagination":{"total-pages":1}}`,
	})
	flows, err := c.GetCashflows(context.Background(), time.Date(2031, time.March, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	// The last one is a withdrawal coming back: the effect signs it, not the name.
	want := []float64{5000, -750, 200, 50}
	if len(flows) != len(want) {
		t.Fatalf("got %d flows, want %d: dividends, interest, the futures settlement and an expiry are not capital", len(flows), len(want))
	}
	for i, w := range want {
		if !approxEq(flows[i].Amount, w) {
			t.Errorf("flow %d = %v, want %v", i, flows[i].Amount, w)
		}
	}
	if len(c.CapabilityWarnings()) != 0 {
		t.Errorf("warnings = %v, want none", c.CapabilityWarnings())
	}
	q := fake.lastQuery.Load().(url.Values)
	if got := strings.Join(q["types[]"], ","); got != "Money Movement,Administrative Transfer,Receive Deliver" {
		t.Errorf("types[] = %q", got)
	}
}

func TestTastytradeGetCashflows_PositionsMovedInKindAreFlagged(t *testing.T) {
	c, _ := newTastytradeFake(t, map[string]string{
		tastytradeTestBase + "/transactions": `{"data":{"items":[
			{"id":1,"transaction-type":"Receive Deliver","transaction-sub-type":"ACAT","symbol":"ACME","quantity":"300","net-value":"15000.0","net-value-effect":"Credit","executed-at":"2031-03-01T14:00:00Z"},
			{"id":2,"transaction-type":"Administrative Transfer","transaction-sub-type":"Journal","net-value":"400.0","net-value-effect":"Debit","executed-at":"2031-03-02T14:00:00Z"}
		]}}`,
	})
	flows, err := c.GetCashflows(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 1 || !approxEq(flows[0].Amount, -400) {
		t.Fatalf("flows = %+v, want the cash journal alone", flows)
	}
	if w := c.CapabilityWarnings(); len(w) != 1 || w[0] != TastytradePositionTransferWarning {
		t.Errorf("warnings = %v, want the position transfer flagged", w)
	}
}

func TestTastytrade_MalformedCredentialsNeverLeave(t *testing.T) {
	for _, creds := range []Credentials{
		{APIKey: "", APISecret: tastytradeTestSecret},
		{APIKey: "not-a-token", APISecret: tastytradeTestSecret},
		{APIKey: tastytradeTestRefresh, APISecret: ""},
	} {
		c, fake := newTastytradeFake(t, map[string]string{})
		c.refreshToken, c.clientSecret = creds.APIKey, creds.APISecret
		err := c.TestConnection(context.Background())
		if err == nil || ClassifyConnectFailure(err) != ConnectFailureCredentials {
			t.Errorf("credentials %+v: error = %v, want a credential refusal", creds, err)
		}
		if fake.signIns.Load() != 0 {
			t.Errorf("credentials %+v reached the venue", creds)
		}
	}
}

func TestTastytrade_RepeatedRefusalsPauseSignIn(t *testing.T) {
	c, fake := newTastytradeFake(t, map[string]string{})
	fake.refuse.Store(true)
	for i := 0; i < tastytradeRefusalLimit; i++ {
		err := c.TestConnection(context.Background())
		if err == nil || errors.Is(err, ErrTransient) || ClassifyConnectFailure(err) != ConnectFailureCredentials {
			t.Fatalf("refusal %d: error = %v, want a credential refusal", i, err)
		}
	}
	err := c.TestConnection(context.Background())
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("error = %v, want sign-in paused as a transient failure", err)
	}
	if got := fake.signIns.Load(); got != tastytradeRefusalLimit {
		t.Errorf("the venue saw %d sign-ins, want none past the %d refused", got, tastytradeRefusalLimit)
	}
}

func TestSignInGuard_ReopensAfterThePause(t *testing.T) {
	g := &signInGuard{}
	start := time.Date(2031, time.March, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < tastytradeRefusalLimit-1; i++ {
		g.refused(start)
	}
	if g.paused(start) {
		t.Fatal("paused below the limit")
	}
	g.refused(start.Add(time.Minute))
	if !g.paused(start.Add(2 * time.Minute)) {
		t.Fatal("not paused at the limit")
	}
	if g.paused(start.Add(time.Minute + tastytradeRefusalPause)) {
		t.Fatal("still paused once the pause has run")
	}
	// Refusals spread over more than the window are the ordinary trickle of
	// revoked grants, not a burst.
	slow := &signInGuard{}
	for i := 0; i < tastytradeRefusalLimit; i++ {
		slow.refused(start.Add(time.Duration(i) * 2 * time.Minute))
	}
	if slow.paused(start.Add(time.Duration(tastytradeRefusalLimit) * 2 * time.Minute)) {
		t.Fatal("paused by refusals spread beyond the window")
	}
}

func TestTastytrade_ErrorsNeverCarrySecrets(t *testing.T) {
	leaks := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("want an error")
		}
		for _, secret := range []string{tastytradeTestRefresh, tastytradeTestSecret, tastytradeTestAccess, tastytradeTestAccount} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error carries %q: %v", secret, err)
			}
		}
	}
	t.Run("sign-in refused with the token echoed back", func(t *testing.T) {
		c, fake := newTastytradeFake(t, map[string]string{})
		fake.refuse.Store(true)
		leaks(t, c.TestConnection(context.Background()))
	})
	t.Run("account the venue does not know", func(t *testing.T) {
		c, _ := newTastytradeFake(t, map[string]string{})
		_, err := c.GetBalance(context.Background())
		leaks(t, err)
	})
	t.Run("unreachable venue", func(t *testing.T) {
		c, _ := newTastytradeFake(t, map[string]string{})
		c.baseURL = "http://127.0.0.1:1"
		_, err := c.GetBalance(context.Background())
		leaks(t, err)
		if !errors.Is(err, ErrTransient) {
			t.Errorf("error = %v, want transient", err)
		}
	})
}

func TestTastytrade_StatusMapping(t *testing.T) {
	cases := []struct {
		status    int
		transient bool
		kind      ConnectFailure
	}{
		{http.StatusForbidden, false, ConnectFailureCredentials},
		{http.StatusTooManyRequests, true, ConnectFailureUpstream},
		{http.StatusBadGateway, true, ConnectFailureCredentials},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/oauth/token" {
				_, _ = w.Write([]byte(`{"access_token":"` + tastytradeTestAccess + `"}`))
				return
			}
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(`{"error":{"code":"some_code","message":"m"}}`))
		}))
		c := NewTastytrade(&Credentials{APIKey: tastytradeTestRefresh, APISecret: tastytradeTestSecret})
		c.baseURL, c.guard = srv.URL, &signInGuard{}
		_, err := c.GetBalance(context.Background())
		srv.Close()
		if err == nil {
			t.Fatalf("status %d: want an error", tc.status)
		}
		if got := errors.Is(err, ErrTransient); got != tc.transient {
			t.Errorf("status %d: transient = %v, want %v (%v)", tc.status, got, tc.transient, err)
		}
		if !tc.transient && ClassifyConnectFailure(err) != tc.kind {
			t.Errorf("status %d: classified %v (%v)", tc.status, ClassifyConnectFailure(err), err)
		}
	}
}

func TestTastytrade_ConcurrentReadsSignInOnce(t *testing.T) {
	c, fake := newTastytradeFake(t, map[string]string{
		tastytradeTestBase + "/balances":  `{"data":{"net-liquidating-value":"100"}}`,
		tastytradeTestBase + "/positions": `{"data":{"items":[]}}`,
	})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.GetBalance(context.Background()); err != nil {
				t.Errorf("GetBalance: %v", err)
			}
			_ = c.CapabilityWarnings()
		}()
	}
	wg.Wait()
	if fake.signIns.Load() != 1 {
		t.Errorf("signed in %d times for 16 concurrent reads, want once", fake.signIns.Load())
	}
}

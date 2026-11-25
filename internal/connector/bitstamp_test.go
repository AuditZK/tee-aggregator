package connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	bitstampTestKey    = "synthetic-bitstamp-key"
	bitstampTestSecret = "synthetic-bitstamp-secret"
)

const bitstampTestTickers = `[
	{"market":"BTC/USD","market_type":"SPOT","last":"50000.00"},
	{"market":"EUR/USD","market_type":"SPOT","last":"1.10"},
	{"market":"EURC/EUR","market_type":"SPOT","last":"1.00"},
	{"market":"XYZ/BTC","market_type":"SPOT","last":"0.001"},
	{"market":"BTC/USD-PERP","market_type":"PERPETUAL","last":"51000.00"}
]`

// bitstampFake checks every private request's v2 signature the way the venue
// does, then serves one body per path.
type bitstampFake struct {
	t      *testing.T
	routes map[string]string
	forms  map[string][]url.Values
	calls  atomic.Int32
	host   string
}

func (f *bitstampFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)
	if r.URL.Path == "/api/v2/ticker/" {
		_, _ = w.Write([]byte(f.routes[r.URL.Path]))
		return
	}
	body, _ := io.ReadAll(r.Body)
	query := ""
	if r.URL.RawQuery != "" {
		query = "?" + r.URL.RawQuery
	}
	msg := "BITSTAMP " + bitstampTestKey + r.Method + f.host + r.URL.Path + query + r.Header.Get("Content-Type") +
		r.Header.Get("X-Auth-Nonce") + r.Header.Get("X-Auth-Timestamp") + "v2" + string(body)
	mac := hmac.New(sha256.New, []byte(bitstampTestSecret))
	mac.Write([]byte(msg))
	if r.Header.Get("X-Auth") != "BITSTAMP "+bitstampTestKey || r.Header.Get("X-Auth-Version") != "v2" ||
		!hmac.Equal([]byte(r.Header.Get("X-Auth-Signature")), []byte(hex.EncodeToString(mac.Sum(nil)))) ||
		len(r.Header.Get("X-Auth-Nonce")) != 36 {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"status":"error","reason":"Invalid signature","code":"API0005"}`))
		return
	}
	if len(body) == 0 && r.Header.Get("Content-Type") != "" {
		f.t.Errorf("%s sent a Content-Type without a body", r.URL.Path)
	}
	if form, err := url.ParseQuery(string(body)); err == nil && len(body) > 0 {
		f.forms[r.URL.Path] = append(f.forms[r.URL.Path], form)
	}
	resp, ok := f.routes[r.URL.Path]
	if !ok {
		f.t.Errorf("unexpected request %s", r.URL.Path)
		http.NotFound(w, r)
		return
	}
	if strings.HasPrefix(resp, "status:") {
		code, payload, _ := strings.Cut(strings.TrimPrefix(resp, "status:"), " ")
		w.WriteHeader(map[string]int{"403": http.StatusForbidden, "400": http.StatusBadRequest, "500": http.StatusInternalServerError}[code])
		resp = payload
	}
	_, _ = w.Write([]byte(resp))
}

func newBitstampFake(t *testing.T, routes map[string]string) (*Bitstamp, *bitstampFake) {
	t.Helper()
	if _, ok := routes["/api/v2/ticker/"]; !ok {
		routes["/api/v2/ticker/"] = bitstampTestTickers
	}
	fake := &bitstampFake{t: t, routes: routes, forms: map[string][]url.Values{}}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	fake.host = strings.TrimPrefix(srv.URL, "http://")
	b := NewBitstamp(&Credentials{APIKey: bitstampTestKey, APISecret: bitstampTestSecret})
	b.baseURL, b.host = srv.URL, fake.host
	return b, fake
}

func TestBitstampGetBalance_ValuesEverythingInUSD(t *testing.T) {
	b, _ := newBitstampFake(t, map[string]string{
		"/api/v2/account_balances/": `[
			{"currency":"usd","total":"1000.00","available":"900.00","reserved":"100.00"},
			{"currency":"btc","total":"0.10000000","available":"0.10000000","reserved":"0"},
			{"currency":"eur","total":"100.00","available":"100.00","reserved":"0"},
			{"currency":"eurc","total":"10","available":"10","reserved":"0"},
			{"currency":"xyz","total":"1000","available":"0","reserved":"1000"},
			{"currency":"dead","total":"5","available":"5","reserved":"0"}
		]`,
		"/api/v2/earn/subscriptions/": `[{"currency":"BTC","amount":0.02,"available_amount":0.02,"amount_earned":0.001}]`,
		"/api/v2/open_positions/":     `[{"market":"BTC/USD-PERP","side":"buy","size":"0.5","entry_price":"49000","mark_price":"51000","pnl_unrealized":"1000"}]`,
	})
	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// 1000 USD + 0.1 BTC + 100 EUR + 10 EURC (through EUR) + 1000 XYZ (through
	// BTC) + 0.02 staked BTC + 1000 of open PnL; an asset with no market is 0.
	want := 1000 + 5000 + 110 + 11 + 50000 + 1000 + 1000.0
	if !approxEq(bal.Equity, want) || !approxEq(bal.UnrealizedPnL, 1000) || bal.Currency != "USD" {
		t.Errorf("equity, unrealized = %v, %v, want %v, 1000", bal.Equity, bal.UnrealizedPnL, want)
	}
	if !approxEq(bal.Available, 900+5000+110+11) {
		t.Errorf("available = %v", bal.Available)
	}
	if len(b.CapabilityWarnings()) != 0 {
		t.Errorf("warnings = %v", b.CapabilityWarnings())
	}

	positions, err := b.GetPositions(context.Background())
	if err != nil || len(positions) != 1 || positions[0].Side != "long" || positions[0].MarketType != MarketSwap {
		t.Fatalf("positions = %+v, %v", positions, err)
	}
}

func TestBitstampGetBalance_UnreadablePartsWarnInsteadOfFailing(t *testing.T) {
	b, _ := newBitstampFake(t, map[string]string{
		"/api/v2/account_balances/":   `[{"currency":"usd","total":"500","available":"500","reserved":"0"}]`,
		"/api/v2/earn/subscriptions/": `status:403 {"status":"error","reason":"No permission","code":"API0003"}`,
		"/api/v2/open_positions/":     `status:403 {"status":"error","reason":"No permission","code":"API0003"}`,
	})
	bal, err := b.GetBalance(context.Background())
	if err != nil || !approxEq(bal.Equity, 500) {
		t.Fatalf("balance = %+v, %v", bal, err)
	}
	if w := b.CapabilityWarnings(); len(w) != 2 || w[0] != BitstampEarnWarning || w[1] != BitstampDerivativesWarning {
		t.Errorf("warnings = %v", w)
	}
}

func TestBitstampGetBalance_AccountWithoutDerivatives(t *testing.T) {
	b, _ := newBitstampFake(t, map[string]string{
		"/api/v2/account_balances/":   `[{"currency":"usd","total":"500","available":"500","reserved":"0"}]`,
		"/api/v2/earn/subscriptions/": `[]`,
		"/api/v2/open_positions/":     `status:400 {"status":"error","reason":"Trade account does not have derivatives","code":"API5506"}`,
	})
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if len(b.CapabilityWarnings()) != 0 {
		t.Errorf("an account without derivatives is complete, warnings = %v", b.CapabilityWarnings())
	}
}

const bitstampTestLedger = `[
	{"id":1,"datetime":"2031-03-01 09:00:00.000000","type":"0","fee":"0","usd":"2000.00","btc":"0","eur":"0"},
	{"id":2,"datetime":"2031-03-01 10:00:00.500000","type":"2","fee":"2.50","usd":"-1000.00","btc":"0.02000000","btc_usd":"50000.00","order_id":11},
	{"id":3,"datetime":"2031-03-01 11:00:00.000000","type":"2","fee":"1.00","eur":"200.00","btc":"-0.00400000","btc_eur":"50000.00","order_id":12},
	{"id":4,"datetime":"2031-03-01 12:00:00.000000","type":"1","fee":"0","btc":"-0.01000000","usd":"0"},
	{"id":5,"datetime":"2031-03-01 13:00:00.000000","type":"27","fee":"0","btc":"0.00010000"},
	{"id":6,"datetime":"2031-03-01 14:00:00.000000","type":"14","fee":"0","eur":"-100.00"},
	{"id":7,"datetime":"2031-03-01 15:00:00.000000","type":"58","fee":"0","usd":"42.00"}
]`

func TestBitstampGetCashflows_CapitalOnly(t *testing.T) {
	b, fake := newBitstampFake(t, map[string]string{"/api/v2/user_transactions/": bitstampTestLedger})
	since := time.Now().Add(-2 * time.Hour)
	flows, err := b.GetCashflows(context.Background(), since)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	// The deposit, the BTC withdrawal at today's price, the transfer of EUR to
	// a sub-account; the trades, the staking reward and the settlement are not.
	want := []float64{2000, -500, -110}
	if len(flows) != len(want) {
		t.Fatalf("got %d flows, want %d", len(flows), len(want))
	}
	for i, w := range want {
		if !approxEq(flows[i].Amount, w) {
			t.Errorf("flow %d = %v, want %v", i, flows[i].Amount, w)
		}
	}
	form := fake.forms["/api/v2/user_transactions/"][0]
	if form.Get("sort") != "asc" || form.Get("limit") != "1000" || form.Get("since_timestamp") != strconv.FormatInt(since.Unix(), 10) {
		t.Errorf("form = %v", form)
	}
}

func TestBitstampGetCashflows_LedgerReachIsThirtyDays(t *testing.T) {
	b, fake := newBitstampFake(t, map[string]string{"/api/v2/user_transactions/": `[]`})
	if _, err := b.GetCashflows(context.Background(), time.Now().Add(-90*24*time.Hour)); err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	asked, _ := strconv.ParseInt(fake.forms["/api/v2/user_transactions/"][0].Get("since_timestamp"), 10, 64)
	if floor := time.Now().Add(-bitstampLedgerReach).Unix(); asked < floor {
		t.Errorf("asked since %d, older than the thirty days the venue serves", asked)
	}
}

func TestBitstampGetTrades_SpotAndPerps(t *testing.T) {
	b, _ := newBitstampFake(t, map[string]string{
		"/api/v2/user_transactions/": bitstampTestLedger,
		"/api/v2/trade_history/":     `[{"trade_id":"p1","datetime":"2031-03-01T16:00:00Z","fee":"0.5","fee_currency":"USD","market":"BTC/USD-PERP","side":"sell","price":"51000","amount":"0.01"}]`,
	})
	trades, err := b.GetTrades(context.Background(), time.Now().Add(-time.Hour), time.Now())
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 3 {
		t.Fatalf("got %d trades, want two spot fills and one perp fill", len(trades))
	}
	if tr := trades[0]; tr.Symbol != "BTC/USD" || tr.Side != "buy" || !approxEq(tr.Quantity, 0.02) || !approxEq(tr.Notional(), 1000) || !approxEq(tr.Fee, 2.5) {
		t.Errorf("first trade = %+v", tr)
	}
	// A EUR fill is counted in dollars, its fee too.
	if tr := trades[1]; tr.Symbol != "BTC/EUR" || tr.Side != "sell" || !approxEq(tr.Notional(), 220) || !approxEq(tr.Fee, 1.1) {
		t.Errorf("second trade = %+v", tr)
	}
	if tr := trades[2]; tr.MarketType != MarketSwap || tr.Side != "sell" || !approxEq(tr.Notional(), 510) {
		t.Errorf("perp trade = %+v", tr)
	}
}

func TestBitstamp_ErrorClasses(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		ip        bool
		transient bool
		kind      ConnectFailure
	}{
		{name: "bad signature", body: `status:403 {"status":"error","reason":"Invalid signature","code":"API0005"}`, kind: ConnectFailureCredentials},
		{name: "unknown key", body: `status:403 {"status":"error","reason":"API key not found","code":"API0001"}`, kind: ConnectFailureCredentials},
		{name: "ip not allowed", body: `status:403 {"status":"error","reason":"IP address not allowed","code":"API0002"}`, ip: true},
		{name: "throttled", body: `status:400 {"status":"error","reason":"Rate limit","code":"400.002"}`, transient: true},
		{name: "down", body: `status:500 {"status":"error","reason":"oops"}`, transient: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := newBitstampFake(t, map[string]string{"/api/v2/account_balances/": tc.body})
			err := b.TestConnection(context.Background())
			if err == nil {
				t.Fatal("want an error")
			}
			if strings.Contains(err.Error(), bitstampTestSecret) || strings.Contains(err.Error(), bitstampTestKey) {
				t.Errorf("error carries a credential: %v", err)
			}
			if got := IsIPRestriction(err); got != tc.ip {
				t.Errorf("ip restriction = %v (%v)", got, err)
			}
			if got := errors.Is(err, ErrTransient); got != tc.transient {
				t.Errorf("transient = %v (%v)", got, err)
			}
			if !tc.ip && !tc.transient && ClassifyConnectFailure(err) != tc.kind {
				t.Errorf("classified %v (%v)", ClassifyConnectFailure(err), err)
			}
		})
	}
	t.Run("a key without the ledger permission is refused at connect", func(t *testing.T) {
		b, _ := newBitstampFake(t, map[string]string{
			"/api/v2/account_balances/":   `[{"currency":"usd","total":"1","available":"1","reserved":"0"}]`,
			"/api/v2/earn/subscriptions/": `[]`,
			"/api/v2/open_positions/":     `[]`,
			"/api/v2/user_transactions/":  `status:403 {"status":"error","reason":"No permission found","code":"API0003"}`,
		})
		err := b.TestConnection(context.Background())
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("error = %v, want the missing permission named", err)
		}
	})
	t.Run("wrong secret is refused by the signature check", func(t *testing.T) {
		b, _ := newBitstampFake(t, map[string]string{"/api/v2/account_balances/": `[]`})
		b.secret = "another-secret"
		if err := b.TestConnection(context.Background()); ClassifyConnectFailure(err) != ConnectFailureCredentials {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("missing secret never leaves", func(t *testing.T) {
		b, fake := newBitstampFake(t, map[string]string{})
		b.secret = ""
		if err := b.TestConnection(context.Background()); err == nil || fake.calls.Load() != 0 {
			t.Fatalf("error = %v, calls = %d", err, fake.calls.Load())
		}
	})
}

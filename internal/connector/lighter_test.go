package connector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Synthetic token — index 713194, never a real signature (QUAL: synthetic data only).
const lighterTestToken = "ro:713194:single:0:testsig"

func approxEq(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

// lighterAt builds a token-form Lighter connector pointed at a test server.
func lighterAt(url string) *Lighter {
	l := NewLighter(&Credentials{APIKey: lighterTestToken})
	l.baseURL = url
	return l
}

// lighterAccountServer serves a fixed body on /api/v1/account.
func lighterAccountServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/account" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestLighterNewParsesCredentials(t *testing.T) {
	t.Run("ro token extracts account index", func(t *testing.T) {
		l := NewLighter(&Credentials{APIKey: lighterTestToken})
		if l.authToken != lighterTestToken {
			t.Errorf("authToken = %q", l.authToken)
		}
		if l.accountIndex == nil || *l.accountIndex != 713194 {
			t.Errorf("accountIndex = %v, want 713194", l.accountIndex)
		}
		if l.walletAddress != "" {
			t.Errorf("walletAddress = %q, want empty", l.walletAddress)
		}
	})
	t.Run("wallet address form", func(t *testing.T) {
		l := NewLighter(&Credentials{APIKey: "0xAbCd1234567890"})
		if l.walletAddress != "0xAbCd1234567890" {
			t.Errorf("walletAddress = %q", l.walletAddress)
		}
		if l.accountIndex != nil {
			t.Errorf("accountIndex = %v, want nil", *l.accountIndex)
		}
		if l.authToken != "" {
			t.Errorf("authToken = %q, want empty", l.authToken)
		}
	})
	t.Run("malformed token leaves index unresolved", func(t *testing.T) {
		l := NewLighter(&Credentials{APIKey: "ro:notanumber:single"})
		if l.accountIndex != nil {
			t.Errorf("accountIndex = %v, want nil", *l.accountIndex)
		}
		if l.authToken != "ro:notanumber:single" {
			t.Errorf("authToken = %q", l.authToken)
		}
	})
}

func TestLighterGetBalance(t *testing.T) {
	srv := lighterAccountServer(t, `{"accounts":[{"index":713194,"available_balance":"4000.5","collateral":"4200","total_asset_value":"4500.25","positions":[]}]}`)
	bal, err := lighterAt(srv.URL).GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !approxEq(bal.Equity, 4500.25) {
		t.Errorf("equity = %v, want 4500.25", bal.Equity)
	}
	if !approxEq(bal.Available, 4000.5) {
		t.Errorf("available = %v, want 4000.5", bal.Available)
	}
	if !approxEq(bal.UnrealizedPnL, 300.25) {
		t.Errorf("unrealizedPnL = %v, want 300.25 (equity-collateral)", bal.UnrealizedPnL)
	}
	if bal.Currency != "USD" {
		t.Errorf("currency = %q, want USD", bal.Currency)
	}
}

func TestLighterGetBalanceByWalletAddress(t *testing.T) {
	const addr = "0xAbC1234567890abcdef01234567890abcdef0123"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("by") != "l1_address" || r.URL.Query().Get("value") != addr {
			t.Errorf("unexpected query: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"accounts":[{"index":42,"total_asset_value":"100","collateral":"100","available_balance":"100","positions":[]}]}`))
	}))
	t.Cleanup(srv.Close)

	l := NewLighter(&Credentials{APIKey: addr})
	l.baseURL = srv.URL
	if _, err := l.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if l.accountIndex == nil || *l.accountIndex != 42 {
		t.Errorf("account index not cached after l1_address lookup: %v", l.accountIndex)
	}
}

func TestLighterGetPositions(t *testing.T) {
	srv := lighterAccountServer(t, `{"accounts":[{"index":713194,"positions":[
		{"symbol":"BTC","sign":1,"position":"2.5","avg_entry_price":"60000","unrealized_pnl":"150.5"},
		{"symbol":"ETH","sign":-1,"position":"10","avg_entry_price":"2500","unrealized_pnl":"-40"},
		{"symbol":"SOL","sign":1,"position":"0","avg_entry_price":"100","unrealized_pnl":"0"}
	]}]}`)
	pos, err := lighterAt(srv.URL).GetPositions(context.Background())
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if len(pos) != 2 {
		t.Fatalf("got %d positions, want 2 (zero-size filtered out)", len(pos))
	}
	if pos[0].Symbol != "BTC" || pos[0].Side != "long" || !approxEq(pos[0].Size, 2.5) {
		t.Errorf("pos[0] = %+v", pos[0])
	}
	if !approxEq(pos[0].UnrealizedPnL, 150.5) {
		t.Errorf("pos[0].UnrealizedPnL = %v, want 150.5", pos[0].UnrealizedPnL)
	}
	if pos[1].Symbol != "ETH" || pos[1].Side != "short" {
		t.Errorf("pos[1] = %+v", pos[1])
	}
	if pos[0].MarketType != MarketSwap {
		t.Errorf("marketType = %q, want %q", pos[0].MarketType, MarketSwap)
	}
}

func TestLighterGetTrades(t *testing.T) {
	const csvBody = "Market,Side,Date,Trade Value,Size,Price,Closed PnL,Fee,Role,Type\n" +
		"BTC,Long,2026-01-15 10:30:00,30000,0.5,60000,120.5,1.25,Taker,Trade\n" +
		"ETH,Short,2026-01-20 14:00:00,5000,2,2500,15,0.4,Maker,Trade\n" +
		"SOL,Long,2025-06-01 00:00:00,100,1,100,0,0,Taker,Trade\n"

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/export":
			_, _ = w.Write([]byte(`{"code":200,"data_url":"` + srv.URL + `/csv"}`))
		case "/csv":
			_, _ = w.Write([]byte(csvBody))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	trades, err := lighterAt(srv.URL).GetTrades(context.Background(), start, end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 2 {
		t.Fatalf("got %d trades, want 2 (2025 row is out of range)", len(trades))
	}
	if trades[0].Symbol != "BTC" || trades[0].Side != "buy" {
		t.Errorf("trades[0] = %+v", trades[0])
	}
	if !approxEq(trades[0].Price, 60000) || !approxEq(trades[0].Quantity, 0.5) || !approxEq(trades[0].Fee, 1.25) {
		t.Errorf("trades[0] numbers = %+v", trades[0])
	}
	if trades[0].MarketType != MarketSwap || trades[0].FeeCurrency != "USDC" {
		t.Errorf("trades[0] metadata = %+v", trades[0])
	}
	if trades[1].Symbol != "ETH" || trades[1].Side != "sell" {
		t.Errorf("trades[1] = %+v", trades[1])
	}
}

func TestLighterGetTradesNoData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":22504,"message":"no export data"}`))
	}))
	t.Cleanup(srv.Close)

	trades, err := lighterAt(srv.URL).GetTrades(context.Background(),
		time.Now().AddDate(0, -1, 0), time.Now())
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 0 {
		t.Errorf("got %d trades, want 0 (export code 22504)", len(trades))
	}
}

func TestLighterGetTradesWalletFormReturnsEmpty(t *testing.T) {
	// Wallet-address form carries no signed token; /api/v1/export is
	// unavailable, so GetTrades returns empty rather than erroring the sync.
	l := NewLighter(&Credentials{APIKey: "0xAbC1234567890abcdef01234567890abcdef0123"})
	l.baseURL = "http://127.0.0.1:1" // must not be reached
	trades, err := l.GetTrades(context.Background(),
		time.Now().AddDate(0, -1, 0), time.Now())
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if trades != nil {
		t.Errorf("got %v, want nil", trades)
	}
}

func TestLighterRefusesASigningKeyBeforeAnyRequest(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()

	for name, cred := range map[string]string{
		"signing key":   "0x" + strings.Repeat("ab", 40),
		"short address": "0xAbCd1234567890",
	} {
		l := NewLighter(&Credentials{APIKey: cred})
		l.baseURL = srv.URL
		err := l.TestConnection(context.Background())
		if err == nil {
			t.Errorf("%s: connection accepted", name)
			continue
		}
		if errors.Is(err, ErrTransient) {
			t.Errorf("%s: classified transient, the holder must be told now: %v", name, err)
		}
		if strings.Contains(err.Error(), cred[2:10]) {
			t.Errorf("%s: error quotes the credential: %v", name, err)
		}
	}
	if hits.Load() != 0 {
		t.Errorf("%d requests left for the venue, want 0", hits.Load())
	}
}

func TestLighterTransientClassification(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		transient bool
	}{
		{"503 service unavailable", http.StatusServiceUnavailable, true},
		{"429 too many requests", http.StatusTooManyRequests, true},
		{"500 internal error", http.StatusInternalServerError, true},
		{"401 unauthorized", http.StatusUnauthorized, false},
		{"404 not found", http.StatusNotFound, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":"x"}`))
			}))
			t.Cleanup(srv.Close)
			_, err := lighterAt(srv.URL).GetBalance(context.Background())
			if err == nil {
				t.Fatal("expected an error")
			}
			if got := errors.Is(err, ErrTransient); got != tc.transient {
				t.Errorf("errors.Is(err, ErrTransient) = %v, want %v (err: %v)", got, tc.transient, err)
			}
		})
	}
}

func TestLighterNetworkErrorIsTransient(t *testing.T) {
	l := NewLighter(&Credentials{APIKey: lighterTestToken})
	l.baseURL = "http://127.0.0.1:1" // nothing listening — connection refused
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := l.GetBalance(ctx); err == nil || !errors.Is(err, ErrTransient) {
		t.Errorf("network failure not classified transient: %v", err)
	}
}

func TestLighterGetBalanceByMarket(t *testing.T) {
	srv := lighterAccountServer(t, `{"accounts":[{"index":713194,"total_asset_value":"5000","collateral":"5000","available_balance":"4800","positions":[]}]}`)
	mb, err := lighterAt(srv.URL).GetBalanceByMarket(context.Background())
	if err != nil {
		t.Fatalf("GetBalanceByMarket: %v", err)
	}
	if len(mb) != 1 || mb[0].MarketType != MarketSwap {
		t.Fatalf("got %+v, want a single swap entry", mb)
	}
	if !approxEq(mb[0].Equity, 5000) || !approxEq(mb[0].AvailableMargin, 4800) {
		t.Errorf("mb[0] = %+v", mb[0])
	}
}

func TestLighterWalletPrefixRedaction(t *testing.T) {
	full := "0x1234567890abcdef1234567890abcdef12345678"
	got := walletPrefix(full)
	if got == full || len(got) >= len(full) {
		t.Errorf("full address not redacted: %q", got)
	}
	if short := "0xABCD"; walletPrefix(short) != short {
		t.Errorf("short address altered: %q", walletPrefix(short))
	}
}

func TestLighterGetCashflowsFromTheHourlySeries(t *testing.T) {
	now := time.Now().UTC()
	h0 := now.Truncate(time.Hour).Add(-4 * time.Hour)
	since := h0.Add(30 * time.Minute)
	type pt struct {
		at          time.Time
		in, out, pn float64
	}
	series := []pt{
		{h0.Add(-time.Hour), 1000, 0, 0},
		{h0, 1200, 0, 5},                      // before since: already read
		{h0.Add(time.Hour), 1500, 0, -40},     // deposit 300
		{h0.Add(2 * time.Hour), 1500, 250, 2}, // withdrawal 250
		{h0.Add(3 * time.Hour), 1500, 250, 9}, // trading only
		{h0.Add(4 * time.Hour), 1600, 260, 9}, // both in the same hour
		{now, 1900, 260, 9},                   // hour still open
	}
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/pnl" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("authorization") != "" {
			t.Errorf("token sent on a public read")
		}
		gotQuery = r.URL.Query()
		var b strings.Builder
		b.WriteString(`{"code":200,"resolution":"1h","pnl":[`)
		for i, p := range series {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"timestamp":%d,"inflow":%v,"outflow":%v,"trade_pnl":%v}`, p.at.Unix(), p.in, p.out, p.pn)
		}
		b.WriteString(`]}`)
		_, _ = w.Write([]byte(b.String()))
	}))
	t.Cleanup(srv.Close)

	l := NewLighter(&Credentials{APIKey: lighterTestToken})
	l.baseURL = srv.URL
	flows, err := l.GetCashflows(context.Background(), since)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if gotQuery.Get("by") != "index" || gotQuery.Get("value") != "713194" || gotQuery.Get("resolution") != "1h" {
		t.Errorf("query = %v", gotQuery)
	}
	want := []struct {
		at     time.Time
		amount float64
	}{
		{h0.Add(time.Hour), 300},
		{h0.Add(2 * time.Hour), -250},
		{h0.Add(4 * time.Hour), 100},
		{h0.Add(4 * time.Hour), -10},
	}
	if len(flows) != len(want) {
		t.Fatalf("got %d flows, want %d: %+v", len(flows), len(want), flows)
	}
	for i, w := range want {
		if !flows[i].Timestamp.Equal(w.at) || !approxEq(flows[i].Amount, w.amount) || flows[i].Currency != "USDC" {
			t.Errorf("[%d] = %v %v %s, want %v %v USDC", i, flows[i].Timestamp, flows[i].Amount, flows[i].Currency, w.at, w.amount)
		}
	}
}

func TestLighterGetCashflowsTransportErrorIsTransient(t *testing.T) {
	l := NewLighter(&Credentials{APIKey: lighterTestToken})
	l.baseURL = "http://127.0.0.1:1"
	_, err := l.GetCashflows(context.Background(), time.Now().Add(-time.Hour))
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("got %v, want ErrTransient", err)
	}
}

func TestLighterGetCashflowsSplitsAWideWindow(t *testing.T) {
	now := time.Now().UTC()
	since := now.Add(-20 * 24 * time.Hour)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		start, _ := strconv.ParseInt(q.Get("start_timestamp"), 10, 64)
		end, _ := strconv.ParseInt(q.Get("end_timestamp"), 10, 64)
		if time.Duration(end-start)*time.Second > lighterHourlySpan {
			_, _ = w.Write([]byte(`{"code":22403,"message":"time range exceeds maximum allowed range for the specified resolution"}`))
			return
		}
		// One deposit of 1 per hour, cumulative since the epoch hour.
		var b strings.Builder
		b.WriteString(`{"code":200,"pnl":[`)
		first := true
		for h := time.Unix(start, 0).UTC().Truncate(time.Hour); !h.After(time.Unix(end, 0)); h = h.Add(time.Hour) {
			if h.Unix() < start {
				continue
			}
			if !first {
				b.WriteString(",")
			}
			first = false
			fmt.Fprintf(&b, `{"timestamp":%d,"inflow":%d,"outflow":0,"trade_pnl":0}`, h.Unix(), h.Unix()/3600)
		}
		b.WriteString(`]}`)
		_, _ = w.Write([]byte(b.String()))
	}))
	t.Cleanup(srv.Close)

	l := NewLighter(&Credentials{APIKey: lighterTestToken})
	l.baseURL = srv.URL
	flows, err := l.GetCashflows(context.Background(), since)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if calls < 3 {
		t.Errorf("%d requests for a 20-day window, want at least 3", calls)
	}
	want := int(now.Truncate(time.Hour).Sub(since.Truncate(time.Hour)) / time.Hour)
	if len(flows) != want {
		t.Fatalf("got %d hourly deposits, want %d (none lost or doubled at a seam)", len(flows), want)
	}
	for _, f := range flows {
		if !approxEq(f.Amount, 1) {
			t.Fatalf("flow %v at %v, want 1", f.Amount, f.Timestamp)
		}
	}
}

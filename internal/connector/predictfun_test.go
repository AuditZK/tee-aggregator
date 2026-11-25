package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	predictFunTestWallet = "0x00000000000000000000000000000000000b0b00"
	predictFunTestKey    = "synthetic-app-key"
)

// predictFunFake answers both the venue's API and the chain node.
type predictFunFake struct {
	t       *testing.T
	routes  map[string]string
	cashWei string
	calls   atomic.Int32
	status  int
}

func (f *predictFunFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)
	if r.URL.Path == "/rpc" {
		var call struct {
			Method string `json:"method"`
			Params []json.RawMessage
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &call)
		if call.Method != "eth_call" || !strings.Contains(string(raw), strings.ToLower(predictFunTestWallet[2:])) {
			f.t.Errorf("chain call = %s", raw)
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x` + f.cashWei + `"}`))
		return
	}
	if r.Header.Get("x-api-key") != predictFunTestKey {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"success":false,"code":401,"error":"unauthorized"}`))
		return
	}
	if f.status != 0 {
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(`{"success":false,"error":"not_found","message":"no account ` + predictFunTestWallet + `"}`))
		return
	}
	key := r.URL.Path
	if p := r.URL.Query().Get("period"); p != "" {
		key += "?period=" + p
	}
	if a := r.URL.Query().Get("after"); a != "" {
		key += "?after=" + a
	}
	body, ok := f.routes[key]
	if !ok {
		f.t.Errorf("unexpected request %s", key)
		http.NotFound(w, r)
		return
	}
	_, _ = w.Write([]byte(body))
}

func newPredictFunFake(t *testing.T, routes map[string]string) (*PredictFun, *predictFunFake) {
	t.Helper()
	fake := &predictFunFake{t: t, routes: routes, cashWei: "0"}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	t.Setenv(PredictFunAPIKeyEnv, predictFunTestKey)
	p := NewPredictFun(&Credentials{APIKey: predictFunTestWallet})
	p.baseURL, p.rpcURL = srv.URL, srv.URL+"/rpc"
	return p, fake
}

const predictFunTestPositions = "/positions/" + predictFunTestWallet

func TestPredictFunGetBalance_ChainCashPlusPositions(t *testing.T) {
	p, fake := newPredictFunFake(t, map[string]string{
		predictFunTestPositions: `{"success":true,"cursor":"c2","data":[
			{"amount":"100000000000000000000","averageBuyPriceUsd":"0.40","pnlUsd":"-12.5","valueUsd":"27.5","market":{"categorySlug":"rain-tomorrow"},"outcome":{"name":"Yes"}}
		]}`,
		predictFunTestPositions + "?after=c2": `{"success":true,"cursor":null,"data":[
			{"amount":"50000000000000000000","averageBuyPriceUsd":"0.70","pnlUsd":"10","valueUsd":"45","market":{"categorySlug":"rain-tomorrow"},"outcome":{"name":"No"}}
		]}`,
	})
	// 1234.5 USDT at 18 decimals.
	fake.cashWei = "42ec210956b3ba0000"
	bal, err := p.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !approxEq(bal.Equity, 1234.5+27.5+45) || !approxEq(bal.Available, 1234.5) || !approxEq(bal.UnrealizedPnL, -2.5) {
		t.Errorf("equity, available, unrealized = %v, %v, %v", bal.Equity, bal.Available, bal.UnrealizedPnL)
	}

	positions, err := p.GetPositions(context.Background())
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if len(positions) != 2 || positions[0].Symbol != "rain-tomorrow/Yes" || !approxEq(positions[0].Size, 100) || !approxEq(positions[0].MarkPrice, 0.275) {
		t.Errorf("positions = %+v", positions[0])
	}
}

func TestPredictFunCumulativePnL(t *testing.T) {
	day := time.Now().UTC().Truncate(24 * time.Hour).Add(-72 * time.Hour)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	all := `{"success":true,"data":{"period":"all","points":[
		{"x":` + epoch(day) + `,"y":0},
		{"x":` + epoch(day.Add(24*time.Hour)) + `,"y":40},
		{"x":` + epoch(day.Add(48*time.Hour)) + `,"y":65.5}
	]}}`
	oneDay := `{"success":true,"data":{"period":"1d","points":[
		{"x":` + epoch(hour) + `,"y":80},
		{"x":` + epoch(hour.Add(time.Hour)) + `,"y":81},
		{"x":` + epoch(hour.Add(2*time.Hour)) + `,"y":83.25}
	]}}`
	p, _ := newPredictFunFake(t, map[string]string{
		"/account/" + predictFunTestWallet + "/pnl?period=all": all,
		"/account/" + predictFunTestWallet + "/pnl?period=1d":  oneDay,
	})
	ctx := context.Background()
	cases := []struct {
		name string
		at   time.Time
		want float64
		err  error
	}{
		{"now is the series' last point", time.Time{}, 65.5, nil},
		{"a day back reads the midnight that opens it", day.Add(24*time.Hour + 7*time.Hour), 40, nil},
		{"before the first point the account had no pnl", day.Add(-time.Hour), 0, nil},
		{"within the last day reads the hourly series", hour.Add(time.Hour + 20*time.Minute), 81, nil},
		{"before the hourly series is not covered", hour.Add(-time.Hour), 0, ErrPnLNotCovered},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := p.CumulativePnL(ctx, tc.at)
			if !errors.Is(err, tc.err) || (tc.err == nil && !approxEq(got, tc.want)) {
				t.Fatalf("CumulativePnL = %v, %v, want %v, %v", got, err, tc.want, tc.err)
			}
		})
	}
}

func epoch(t time.Time) string { return strconv.FormatInt(t.Unix(), 10) }

func TestPredictFunGetTrades_OwnSidesOnly(t *testing.T) {
	p, _ := newPredictFunFake(t, map[string]string{
		"/orders/matches": `{"success":true,"cursor":null,"data":[
			{"market":{"title":"Rain tomorrow"},"executedAt":"2031-03-01T10:00:00.000Z",
			 "taker":{"quoteType":"Bid","amount":"10000000000000000000","price":"300000000000000000","signer":"0x00000000000000000000000000000000000c0c00","hash":"0xt","outcome":{"name":"Yes"},"fee":{"amount":"200000000000000000","type":"SHARES"}},
			 "makers":[{"quoteType":"Ask","amount":"10000000000000000000","price":"300000000000000000","signer":"` + strings.ToUpper(predictFunTestWallet[:2]) + predictFunTestWallet[2:] + `","hash":"0xm","outcome":{"name":"Yes"},"fee":{"amount":"0","type":"SHARES"}}]}
		]}`,
	})
	trades, err := p.GetTrades(context.Background(), time.Date(2031, 3, 1, 0, 0, 0, 0, time.UTC), time.Time{})
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("got %d trades, want the wallet's maker side alone", len(trades))
	}
	if tr := trades[0]; tr.Side != "sell" || tr.ID != "0xm" || !approxEq(tr.Price, 0.3) || !approxEq(tr.Quantity, 10) || !approxEq(tr.Notional(), 3) || tr.Fee != 0 {
		t.Errorf("trade = %+v", tr)
	}
}

func TestPredictFun_Refusals(t *testing.T) {
	t.Run("no application key on the enclave", func(t *testing.T) {
		p, fake := newPredictFunFake(t, map[string]string{})
		p.apiKey = ""
		err := p.TestConnection(context.Background())
		if ClassifyConnectFailure(err) != ConnectFailureNotConfigured || fake.calls.Load() != 0 {
			t.Fatalf("error = %v (%d calls), want not configured and no request", err, fake.calls.Load())
		}
	})
	t.Run("application key refused", func(t *testing.T) {
		p, _ := newPredictFunFake(t, map[string]string{})
		p.apiKey = "another-key"
		if err := p.TestConnection(context.Background()); ClassifyConnectFailure(err) != ConnectFailureNotConfigured {
			t.Fatalf("error = %v, want not configured: the user holds no key", err)
		}
	})
	t.Run("malformed address", func(t *testing.T) {
		for _, addr := range []string{"", "alice", "https://example.test/portfolio/alice", "0x1234"} {
			p, fake := newPredictFunFake(t, map[string]string{})
			p.wallet = addr
			err := p.TestConnection(context.Background())
			if ClassifyConnectFailure(err) != ConnectFailureCredentials || fake.calls.Load() != 0 {
				t.Errorf("address %q: error = %v (%d calls)", addr, err, fake.calls.Load())
			}
		}
	})
	t.Run("an address that never used predict.fun", func(t *testing.T) {
		p, _ := newPredictFunFake(t, map[string]string{
			predictFunTestPositions:                                `{"success":true,"cursor":null,"data":[]}`,
			"/account/" + predictFunTestWallet + "/pnl?period=all": `{"success":true,"data":{"period":"all","points":[]}}`,
		})
		err := p.TestConnection(context.Background())
		if ClassifyConnectFailure(err) != ConnectFailureCredentials || !strings.Contains(err.Error(), "portfolio page") {
			t.Fatalf("error = %v, want a refusal that says where the address is", err)
		}
	})
	t.Run("a wallet holding USDT but no Predict activity is refused", func(t *testing.T) {
		p, fake := newPredictFunFake(t, map[string]string{
			predictFunTestPositions:                                `{"success":true,"cursor":null,"data":[]}`,
			"/account/" + predictFunTestWallet + "/pnl?period=all": `{"success":true,"data":{"period":"all","points":[]}}`,
		})
		fake.cashWei = "de0b6b3a7640000"
		if err := p.TestConnection(context.Background()); ClassifyConnectFailure(err) != ConnectFailureCredentials {
			t.Fatalf("error = %v, want a refusal: a sign-in wallet holds USDT too", err)
		}
	})
	t.Run("an account with an open position but no PnL point yet connects", func(t *testing.T) {
		p, _ := newPredictFunFake(t, map[string]string{
			predictFunTestPositions: `{"success":true,"cursor":null,"data":[
				{"amount":"100000000000000000000","averageBuyPriceUsd":"0.40","pnlUsd":"0","valueUsd":"40","market":{"categorySlug":"rain-tomorrow"},"outcome":{"name":"Yes"}}
			]}`,
			"/account/" + predictFunTestWallet + "/pnl?period=all": `{"success":true,"data":{"period":"all","points":[]}}`,
		})
		if err := p.TestConnection(context.Background()); err != nil {
			t.Fatalf("TestConnection: %v", err)
		}
	})
	t.Run("errors never carry the address", func(t *testing.T) {
		p, fake := newPredictFunFake(t, map[string]string{})
		fake.status = http.StatusNotFound
		_, err := p.GetBalance(context.Background())
		if err == nil || strings.Contains(strings.ToLower(err.Error()), strings.ToLower(predictFunTestWallet)) {
			t.Fatalf("error = %v", err)
		}
		if ClassifyConnectFailure(err) != ConnectFailureCredentials {
			t.Errorf("an unknown address reads as %v", ClassifyConnectFailure(err))
		}
	})
	t.Run("throttled or down is transient", func(t *testing.T) {
		for _, status := range []int{http.StatusTooManyRequests, http.StatusBadGateway} {
			p, fake := newPredictFunFake(t, map[string]string{})
			fake.status = status
			if _, err := p.GetBalance(context.Background()); !errors.Is(err, ErrTransient) {
				t.Errorf("status %d: error = %v, want transient", status, err)
			}
		}
	})
}

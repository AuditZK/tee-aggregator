package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
)

type binancePMServer struct {
	mu    sync.Mutex
	paths []string
}

func (s *binancePMServer) hit(prefix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.ContainsFunc(s.paths, func(p string) bool { return strings.Contains(p, prefix) })
}

// newBinancePMConnector serves a spot wallet of 100 USDT and a cross margin
// read worth 2 BTC at 50000. futures is "ok" or "denied"; papi is "ok",
// "denied", "throttled" or "banned" (418).
func newBinancePMConnector(t *testing.T, futures, papi string) (*Binance, *binancePMServer) {
	t.Helper()
	state := &binancePMServer{}
	denied := func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		state.mu.Lock()
		state.paths = append(state.paths, p)
		state.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(p, "/api/v3/ticker"):
			_, _ = w.Write([]byte(`[{"symbol":"BTCUSDT","price":"50000"}]`))
		case strings.Contains(p, "/api/v3/account"):
			_, _ = w.Write([]byte(`{"balances":[{"asset":"USDT","free":"100","locked":"0"}]}`))
		case strings.Contains(p, "/papi/"):
			switch {
			case papi == "denied":
				denied(w)
			case papi == "banned":
				w.WriteHeader(http.StatusTeapot)
				_, _ = w.Write([]byte(`{"code":-1003,"msg":"Way too many requests; IP banned."}`))
			case papi == "throttled":
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"code":-1003,"msg":"Too many requests"}`))
			case strings.Contains(p, "/papi/v1/account"):
				_, _ = w.Write([]byte(`{"accountEquity":"4800","actualEquity":"5000","accountInitialMargin":"1200","totalMarginOpenLoss":"100","totalAvailableBalance":"3500"}`))
			case strings.Contains(p, "/papi/v1/balance"):
				_, _ = w.Write([]byte(`[{"asset":"USDT","umUnrealizedPNL":"50","cmUnrealizedPNL":"0"},{"asset":"BTC","umUnrealizedPNL":"0","cmUnrealizedPNL":"0.001"}]`))
			case strings.Contains(p, "/papi/v1/um/positionRisk"):
				_, _ = w.Write([]byte(`[{"symbol":"BTCUSDT","positionAmt":"-0.5","entryPrice":"51000","markPrice":"50000","unRealizedProfit":"500","positionSide":"BOTH"}]`))
			default:
				_, _ = w.Write([]byte(`{}`))
			}
		case strings.Contains(p, "/fapi/"), strings.Contains(p, "/dapi/"):
			if futures == "denied" {
				denied(w)
				return
			}
			if strings.Contains(p, "/fapi/v2/account") {
				_, _ = w.Write([]byte(`{"totalMarginBalance":"700","totalUnrealizedProfit":"0","availableBalance":"600"}`))
				return
			}
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(p, "/sapi/v1/margin/account"):
			_, _ = w.Write([]byte(`{"totalNetAssetOfBtc":"2"}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	return NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client), state
}

func TestBinance_PortfolioMargin_EquityReadOnceFromPapi(t *testing.T) {
	b, srv := newBinancePMConnector(t, "denied", "ok")

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 5100 {
		t.Fatalf("equity = %v, want 5100 (spot 100 + portfolio margin 5000, cross margin not added on top)", bal.Equity)
	}
	if bal.UnrealizedPnL != 100 {
		t.Fatalf("unrealized = %v, want 100 (50 USDT + 0.001 BTC at 50000)", bal.UnrealizedPnL)
	}
	if bal.Available != 3800 {
		t.Fatalf("available = %v, want 3800 (spot 100 + portfolio margin 5000 less 1300 held by positions and orders)", bal.Available)
	}
	if srv.hit("/sapi/v1/margin/account") || srv.hit("/dapi/") {
		t.Fatal("cross margin or COIN-M read separately on a portfolio margin account")
	}
	if w := b.CapabilityWarnings(); len(w) != 0 {
		t.Fatalf("warnings = %v, want none", w)
	}
	m := marketMap(t, mustMarket(t, b))
	if swap := m[MarketSwap]; swap == nil || swap.Equity != 5000 || swap.AvailableMargin != 3700 {
		t.Fatalf("swap bucket = %+v, want equity 5000 / available 3700", swap)
	}
	measured := b.measuredWallets()
	for _, w := range []string{binanceWalletUM, binanceWalletCoinM, binanceWalletCross} {
		if !measured[w] {
			t.Fatalf("wallet %s not measured: a transfer into it would read as a withdrawal", w)
		}
	}
}

func TestBinance_PortfolioMargin_RefusedByPapiKeepsFuturesWarning(t *testing.T) {
	b, _ := newBinancePMConnector(t, "denied", "denied")

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 100100 {
		t.Fatalf("equity = %v, want 100100 (spot 100 + cross margin 2 BTC)", bal.Equity)
	}
	if w := b.CapabilityWarnings(); !slices.Contains(w, "futures_permission_missing") || !slices.Contains(w, "coinm_unreadable") {
		t.Fatalf("warnings = %v, want the futures and COIN-M gaps", w)
	}
}

func TestBinance_PortfolioMargin_TransientPapiFailsTheSync(t *testing.T) {
	b, _ := newBinancePMConnector(t, "denied", "throttled")

	if _, err := b.GetBalance(context.Background()); !errors.Is(err, ErrTransient) {
		t.Fatalf("err = %v, want ErrTransient", err)
	}
}

func TestBinance_PortfolioMargin_NeverTriedWhenFuturesAnswer(t *testing.T) {
	b, srv := newBinancePMConnector(t, "ok", "ok")

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 100800 {
		t.Fatalf("equity = %v, want 100800 (spot 100 + futures 700 + cross margin 2 BTC)", bal.Equity)
	}
	if _, err := b.GetPositions(context.Background()); err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if srv.hit("/papi/") {
		t.Fatal("papi called for an account whose futures read answers")
	}
}

func TestBinance_PortfolioMargin_PositionsFromPapi(t *testing.T) {
	b, _ := newBinancePMConnector(t, "denied", "ok")

	pos, err := b.GetPositions(context.Background())
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if len(pos) != 1 || pos[0].Side != "short" || pos[0].Size != 0.5 || pos[0].UnrealizedPnL != 500 {
		t.Fatalf("positions = %+v, want one 0.5 short with 500 unrealized", pos)
	}
}

// A 418 ban on papi says nothing about the account: read as "not Portfolio
// Margin", a PM account's equity would be written without its futures pool.
func TestBinance_PortfolioMargin_BannedPapiFailsTheSync(t *testing.T) {
	b, _ := newBinancePMConnector(t, "denied", "banned")

	if _, err := b.GetBalance(context.Background()); !errors.Is(err, ErrTransient) {
		t.Fatalf("err = %v, want ErrTransient", err)
	}
}

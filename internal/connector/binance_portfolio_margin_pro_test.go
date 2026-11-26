package connector

import (
	"context"
	"errors"
	"math"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// pmProAccount is a Portfolio Margin Pro account: papi refuses, fapi answers
// (UM wallet 700), the pool reads as cross margin (4.8 BTC at 50000 =
// 240000) and holds ETH and the unlisted yield token USCX. Measured equity:
// spot 100 + UM 700 + cross margin 240000 = 240800; the pool is 240700.
func pmProAccount() *binancePMActivityServer {
	now := time.Now().UTC()
	return &binancePMActivityServer{
		crossNetBtc:      "4.8",
		portfolioAccount: `{"accountType":"PM_1","accountEquity":"239000","actualEquity":"240600","accountInitialMargin":"0","accountStatus":"NORMAL"}`,
		crossAssets:      []string{"ETH", "USCX"},
		marginErrors: map[string]pmError{
			"USCXUSDT": {http.StatusBadRequest, `{"code":-1121,"msg":"Invalid symbol."}`},
			"USCXUSDC": {http.StatusBadRequest, `{"code":-1121,"msg":"Invalid symbol."}`},
		},
		mFills: []pmFill{
			{symbol: "ETHUSDT", time: pmDay.Add(7 * time.Hour), row: map[string]any{"id": 31, "isBuyer": true, "price": "3000", "qty": "0.1", "commission": "0.0001", "commissionAsset": "ETH"}},
		},
		umIncome: []pmIncomeRow{
			{symbol: "BTCUSDT", incomeType: "FUNDING_FEE", income: "-12.5", asset: "USDT", time: now.Add(-16 * time.Hour), tranID: 1},
			{symbol: "ETHUSDT", incomeType: "FUNDING_FEE", income: "2.5", asset: "USDT", time: now.Add(-8 * time.Hour), tranID: 2},
			{symbol: "BTCUSDT", incomeType: "COMMISSION", income: "-9", asset: "USDT", time: now.Add(-8 * time.Hour), tranID: 3},
		},
		cmIncome: []pmIncomeRow{
			{symbol: "BTCUSD_PERP", incomeType: "FUNDING_FEE", income: "-0.0001", asset: "BTC", time: now.Add(-4 * time.Hour), tranID: 1},
		},
	}
}

func sumFunding(fees []*FundingFee) float64 {
	sum := 0.0
	for _, f := range fees {
		sum += f.Amount
	}
	return sum
}

func TestBinance_PMPro_FundingAndMarginFillsRead(t *testing.T) {
	srv := pmProAccount()
	b := srv.connector(t)
	ctx := context.Background()

	bal, err := b.GetBalance(ctx)
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 240800 {
		t.Fatalf("equity = %v, want 240800: the basis does not change", bal.Equity)
	}
	if pro, coinM := b.portfolioMarginProState(); pro != binancePMYes || coinM {
		t.Fatalf("verdict = %v (COIN-M %v), want Pro without COIN-M", pro, coinM)
	}
	if w := b.CapabilityWarnings(); len(w) != 0 {
		t.Fatalf("warnings = %v, want none: the pool is 0.04%% off actualEquity", w)
	}
	check, ok := b.BalanceCrossCheck()
	if !ok || check.Measured != 240700 || check.Reference != 240600 {
		t.Fatalf("cross-check = %+v (%v), want pool 240700 against 240600", check, ok)
	}

	trades, err := b.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	var spot, swap int
	for _, tr := range trades {
		switch tr.MarketType {
		case MarketSpot:
			spot++
			if tr.Symbol != "ETHUSDT" || tr.Notional() != 300 {
				t.Fatalf("margin fill = %+v", tr)
			}
		case MarketSwap:
			swap++
		}
	}
	if spot != 1 || swap != 1 {
		t.Fatalf("trades = %d spot / %d swap, want the margin fill and the fapi fill", spot, swap)
	}
	if got := srv.symbolsAsked("/sapi/v1/margin/myTrades"); !slices.Equal(got, []string{"ETHUSDC", "ETHUSDT", "USCXUSDC", "USCXUSDT"}) {
		t.Fatalf("margin symbols asked = %v", got)
	}

	fees, err := b.GetFundingFees(ctx, nil, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if len(fees) != 2 || sumFunding(fees) != -10 {
		t.Fatalf("funding = %d rows summing %v, want the 2 fapi FUNDING_FEE rows summing -10", len(fees), sumFunding(fees))
	}
	if srv.count("/dapi/v1/income") != 0 {
		t.Fatal("dapi income read on an account without a COIN-M wallet")
	}
	if w := b.CapabilityWarnings(); slices.Contains(w, binancePMTradesUnreadable) {
		t.Fatalf("warnings = %v after clean reads", w)
	}

	// A second balance read on the same instance does not probe again.
	if _, err := b.GetBalance(ctx); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if n := srv.count("/sapi/v1/portfolio/account"); n != 1 {
		t.Fatalf("portfolio account probed %d times, want once per instance", n)
	}
	if n := srv.countPrefix("/papi/"); n != 0 {
		t.Fatalf("%d papi calls on a PM Pro account, want 0 (fapi answers)", n)
	}
}

func TestBinance_PMPro_CoinMFundingFromDapi(t *testing.T) {
	srv := pmProAccount()
	srv.coinMBalance = `[{"asset":"BTC","balance":"0.1","crossUnPnl":"0"}]`
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	fees, err := b.GetFundingFees(context.Background(), nil, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	// -12.5 + 2.5 USDT on fapi, and 0.0001 BTC at 50000 = -5 on dapi.
	if len(fees) != 3 || math.Abs(sumFunding(fees)-(-15)) > 1e-9 {
		t.Fatalf("funding = %d rows summing %v, want 3 rows summing -15", len(fees), sumFunding(fees))
	}
}

func TestBinance_PMPro_ResidualRaisesMarkerOnly(t *testing.T) {
	srv := pmProAccount()
	srv.portfolioAccount = `{"accountType":"PM_3","actualEquity":"230000"}`
	b := srv.connector(t)
	for range 2 {
		bal, err := b.GetBalance(context.Background())
		if err != nil {
			t.Fatalf("GetBalance: %v", err)
		}
		if bal.Equity != 240800 {
			t.Fatalf("equity = %v, want 240800 kept", bal.Equity)
		}
		if w := b.CapabilityWarnings(); !slices.Equal(w, []string{binanceMarkerPMProResidual}) {
			t.Fatalf("warnings = %v, want only %s on every read of the instance", w, binanceMarkerPMProResidual)
		}
	}
}

func TestBinance_PMPro_UnreadableMarginFillsRaiseMarker(t *testing.T) {
	srv := pmProAccount()
	srv.marginErrors["ETHUSDC"] = pmError{http.StatusTeapot, `{"code":-1003,"msg":"Way too many requests; IP banned."}`}
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	trades, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour))
	if err == nil {
		t.Fatal("GetTrades hid the ban")
	}
	if len(trades) == 0 {
		t.Fatal("the fapi fill read before the ban was dropped")
	}
	if !slices.Contains(b.CapabilityWarnings(), binancePMTradesUnreadable) {
		t.Fatalf("warnings = %v, want %s", b.CapabilityWarnings(), binancePMTradesUnreadable)
	}
}

// Whatever the probe answers short of PM_1/PM_3, the account is read exactly
// as a classic one: same balance, no marker, no funding, no sapi fills. A
// throttled probe is asked again on the next read; any other answer is kept.
func TestBinance_PMPro_ProbeFailureChangesNothing(t *testing.T) {
	cases := map[string]struct {
		srv        func(*binancePMActivityServer)
		wantProbes int
	}{
		"refused": {func(s *binancePMActivityServer) { s.portfolioStatus = http.StatusUnauthorized }, 1},
		"banned":  {func(s *binancePMActivityServer) { s.portfolioStatus = http.StatusTeapot }, 2},
		"papi flavour": {func(s *binancePMActivityServer) {
			s.portfolioAccount = `{"accountType":"PM_2","actualEquity":"240600"}`
		}, 1},
		"no account type": {func(s *binancePMActivityServer) { s.portfolioAccount = `{}` }, 1},
		"not json":        {func(s *binancePMActivityServer) { s.portfolioAccount = `<html>` }, 1},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			srv := pmProAccount()
			c.srv(srv)
			b := srv.connector(t)
			ctx := context.Background()
			for range 2 {
				bal, err := b.GetBalance(ctx)
				if err != nil {
					t.Fatalf("GetBalance: %v", err)
				}
				if bal.Equity != 240800 {
					t.Fatalf("equity = %v, want 240800", bal.Equity)
				}
				if w := b.CapabilityWarnings(); len(w) != 0 {
					t.Fatalf("warnings = %v, want none", w)
				}
			}
			if n := srv.count("/sapi/v1/portfolio/account"); n != c.wantProbes {
				t.Fatalf("probed %d times over two reads, want %d", n, c.wantProbes)
			}
			if _, err := b.GetFundingFees(ctx, nil, pmDay); !errors.Is(err, ErrFundingNotRead) {
				t.Fatalf("GetFundingFees err = %v, want ErrFundingNotRead", err)
			}
			if _, err := b.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour)); err != nil {
				t.Fatalf("GetTrades: %v", err)
			}
			if n := srv.count("/sapi/v1/margin/myTrades") + srv.count("/sapi/v1/portfolio/balance"); n != 0 {
				t.Fatalf("%d PM Pro reads on an account that is not Pro", n)
			}
		})
	}
}

// pmProPaths are the reads only a Portfolio Margin Pro account may cause.
func pmProPaths(s *binancePMActivityServer) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, u := range s.calls {
		if strings.HasPrefix(u.Path, "/sapi/v1/portfolio/") || u.Path == "/sapi/v1/margin/myTrades" ||
			u.Path == "/fapi/v1/income" || u.Path == "/dapi/v1/income" {
			n++
		}
	}
	return n
}

// A classic account with an empty cross margin wallet is never probed, and
// reads exactly as before: same paths, same results.
func TestBinance_PMPro_ClassicWithoutCrossMarginNeverProbed(t *testing.T) {
	srv := &binancePMActivityServer{}
	b := srv.connector(t)
	ctx := context.Background()
	for range 2 {
		bal, err := b.GetBalance(ctx)
		if err != nil || bal.Equity != 800 {
			t.Fatalf("GetBalance = %+v, %v; want 800", bal, err)
		}
	}
	trades, err := b.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour))
	if err != nil || len(trades) != 1 || trades[0].ID != "1" {
		t.Fatalf("GetTrades = %+v, %v; want the one fapi fill", trades, err)
	}
	if _, err := b.GetFundingFees(ctx, nil, pmDay); !errors.Is(err, ErrFundingNotRead) {
		t.Fatalf("GetFundingFees err = %v, want ErrFundingNotRead", err)
	}
	if n := pmProPaths(srv) + srv.countPrefix("/papi/"); n != 0 {
		t.Fatalf("%d Portfolio Margin reads on a classic account, want 0", n)
	}
	if _, ok := b.BalanceCrossCheck(); ok {
		t.Fatal("cross-check reported on a classic account")
	}
}

// A classic account using cross margin is asked once per instance, refused,
// and from then on reads exactly as before.
func TestBinance_PMPro_ClassicWithCrossMarginProbedOnce(t *testing.T) {
	srv := &binancePMActivityServer{crossNetBtc: "2"}
	b := srv.connector(t)
	ctx := context.Background()
	for range 3 {
		bal, err := b.GetBalance(ctx)
		if err != nil || bal.Equity != 100800 {
			t.Fatalf("GetBalance = %+v, %v; want 100800", bal, err)
		}
		if w := b.CapabilityWarnings(); len(w) != 0 {
			t.Fatalf("warnings = %v, want none", w)
		}
	}
	if _, err := b.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour)); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if _, err := b.GetFundingFees(ctx, nil, pmDay); !errors.Is(err, ErrFundingNotRead) {
		t.Fatalf("GetFundingFees err = %v, want ErrFundingNotRead", err)
	}
	if n := srv.count("/sapi/v1/portfolio/account"); n != 1 {
		t.Fatalf("probed %d times over three reads, want 1", n)
	}
	if n := pmProPaths(srv); n != 1 {
		t.Fatalf("%d Portfolio Margin Pro reads, want only the one probe", n)
	}
}

// A papi Portfolio Margin account (PM_2) never reaches the Pro probe.
func TestBinance_PMPro_PapiAccountNeverProbed(t *testing.T) {
	srv := pmAccountWithActivity()
	srv.crossNetBtc = "2"
	srv.portfolioAccount = `{"accountType":"PM_2"}`
	b := srv.connector(t)
	ctx := context.Background()
	if _, err := b.GetBalance(ctx); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	trades, err := b.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour))
	if err != nil || len(trades) != 5 {
		t.Fatalf("GetTrades = %d trades, %v; want the 5 papi fills", len(trades), err)
	}
	if _, err := b.GetFundingFees(ctx, nil, pmDay); err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if n := pmProPaths(srv); n != 0 {
		t.Fatalf("%d Portfolio Margin Pro reads on a papi account, want 0", n)
	}
}

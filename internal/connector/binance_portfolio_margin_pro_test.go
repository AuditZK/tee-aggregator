package connector

import (
	"context"
	"errors"
	"math"
	"net/http"
	"slices"
	"strconv"
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
	// ETHUSDC and the USCX pairs are not listed spot pairs in the ticker map:
	// not asked, so no -1121 is spent on them.
	if got := srv.symbolsAsked("/sapi/v1/margin/myTrades"); !slices.Equal(got, []string{"ETHUSDT"}) {
		t.Fatalf("margin symbols asked = %v, want only the listed pair", got)
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
	for read := range 2 {
		bal, err := b.GetBalance(context.Background())
		if err != nil {
			t.Fatalf("GetBalance: %v", err)
		}
		if bal.Equity != 240800 {
			t.Fatalf("equity = %v, want 240800 kept", bal.Equity)
		}
		// Raised and handed out by the read that measured it, not repeated by
		// the later reads of the instance, which have no new figure.
		check, ok := b.BalanceCrossCheck()
		if w := b.CapabilityWarnings(); read == 0 && (!slices.Equal(w, []string{binanceMarkerPMProResidual}) || !ok || check.Reference != 230000) {
			t.Fatalf("first read: warnings = %v, check = %+v (%v); want the marker and the check", w, check, ok)
		} else if read == 1 && (len(w) != 0 || ok) {
			t.Fatalf("second read: warnings = %v, check reported again = %v; want neither", w, ok)
		}
	}
}

func TestBinance_PMPro_UnreadableMarginFillsRaiseMarker(t *testing.T) {
	srv := pmProAccount()
	srv.marginErrors["ETHUSDT"] = pmError{http.StatusTeapot, `{"code":-1003,"msg":"Way too many requests; IP banned."}`}
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	trades, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour))
	if err == nil {
		t.Fatal("GetTrades hid the ban")
	}
	if len(trades) != 1 || trades[0].MarketType != MarketSwap {
		t.Fatalf("trades = %+v, want the fapi fill read before the ban", trades)
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
		// open: the answer said nothing about the account, so the verdict
		// stays open (asked again) and pm_trades_unreadable is raised.
		open bool
	}{
		"binance refusal": {func(s *binancePMActivityServer) { s.portfolioStatus, s.portfolioAccount = http.StatusUnauthorized, "" }, 1, false},
		"banned":          {func(s *binancePMActivityServer) { s.portfolioStatus, s.portfolioAccount = http.StatusTeapot, "" }, 2, true},
		"waf page": {func(s *binancePMActivityServer) {
			s.portfolioStatus, s.portfolioAccount = http.StatusForbidden, `<html>Request blocked</html>`
		}, 2, true},
		"proxy json without code": {func(s *binancePMActivityServer) {
			s.portfolioStatus, s.portfolioAccount = http.StatusBadRequest, `{"error":"bad request"}`
		}, 2, true},
		"papi flavour": {func(s *binancePMActivityServer) {
			s.portfolioAccount = `{"accountType":"PM_2","actualEquity":"240600"}`
		}, 1, false},
		"no account type": {func(s *binancePMActivityServer) { s.portfolioAccount = `{}` }, 1, false},
		"truncated":       {func(s *binancePMActivityServer) { s.portfolioAccount = `{"accountType":"PM_` }, 2, true},
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
				w := b.CapabilityWarnings()
				if want := c.open; want != slices.Equal(w, []string{binancePMTradesUnreadable}) || (!want && len(w) != 0) {
					t.Fatalf("warnings = %v, want pm_trades_unreadable only when the verdict stays open (%v)", w, c.open)
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

// A catch-up whose margin fills need minutes of pacing must not spend the
// sync's deadline: the read gives up with what it has and the marker, and
// the cashflow read after it, on the same context, still returns the flows.
func TestBinance_PMPro_SlowMarginReadLeavesTimeForCashflows(t *testing.T) {
	srv := pmProAccount()
	srv.deposits = `[{"coin":"USDT","amount":"500","insertTime":` +
		strconvItoa(time.Now().Add(-time.Hour).UnixMilli()) + `,"walletType":0}]`
	b := srv.connector(t)
	// 100 a minute: the 30 daily margin windows (300 weight) alone need three
	// minutes, as ~24 pool pairs would at the real 1200.
	b.pacer.budget = 100
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	start := time.Now()

	if _, err := b.GetBalance(ctx); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	now := time.Now().UTC()
	if _, err := b.GetTrades(ctx, now.Add(-30*24*time.Hour), now); err == nil {
		t.Fatal("GetTrades read 30 days of margin fills inside its budget")
	}
	_, _ = b.GetFundingFees(ctx, nil, now.Add(-30*24*time.Hour))
	flows, err := b.GetCashflows(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows after the margin read: %v", err)
	}
	if len(flows) != 1 || flows[0].Amount != 500 {
		t.Fatalf("flows = %+v, want the 500 deposit", flows)
	}
	if !slices.Contains(b.CapabilityWarnings(), binancePMTradesUnreadable) {
		t.Fatalf("warnings = %v, want %s", b.CapabilityWarnings(), binancePMTradesUnreadable)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("reads took %v of a 4 s deadline", elapsed)
	}
}

func TestBinanceWeightPacer_GivesUpBeforeTheDeadline(t *testing.T) {
	p := &binanceWeightPacer{budget: 10}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.wait(ctx, 10); err != nil {
		t.Fatalf("first wait: %v", err)
	}
	start := time.Now()
	if err := p.wait(ctx, 10); !errors.Is(err, ErrTransient) {
		t.Fatalf("wait whose window opens after the deadline = %v, want ErrTransient", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("the pacer slept toward a deadline it could not beat")
	}
}

// A pool asset worth under $1 is not asked for.
func TestBinance_PMPro_DustPoolAssetNotAsked(t *testing.T) {
	srv := pmProAccount()
	srv.crossAssets = []string{"ETH", "BTC"}
	srv.poolAmounts = map[string]string{"BTC": "0.00001"} // $0.50
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if _, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour)); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if got := srv.symbolsAsked("/sapi/v1/margin/myTrades"); !slices.Equal(got, []string{"ETHUSDT"}) {
		t.Fatalf("margin symbols asked = %v, want the dust BTC left out", got)
	}
}

// The recount scopes its read to swap: no margin fill and no pool read, on a
// Pro account (sapi) and on a papi one alike, the swap fills still read.
func TestBinance_SwapScopedReadSkipsMarginFills(t *testing.T) {
	ctx := WithTradeMarkets(context.Background(), []string{MarketSwap})

	pro := pmProAccount()
	b := pro.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	trades, err := b.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour))
	if err != nil || len(trades) != 1 || trades[0].MarketType != MarketSwap {
		t.Fatalf("Pro swap read = %+v, %v; want the fapi fill alone", trades, err)
	}
	if n := pro.count("/sapi/v1/margin/myTrades") + pro.count("/sapi/v1/portfolio/balance") + pro.count("/api/v3/myTrades"); n != 0 {
		t.Fatalf("%d spot or margin reads in a swap-scoped read", n)
	}

	papi := pmAccountWithActivity()
	pb := papi.connector(t)
	if _, err := pb.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	trades, err = pb.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour))
	if err != nil || len(trades) != 4 {
		t.Fatalf("papi swap read = %d trades, %v; want the 3 UM and 1 COIN-M fills", len(trades), err)
	}
	if n := papi.count("/papi/v1/margin/myTrades") + papi.count("/papi/v1/balance"); n != 1 {
		// The one /papi/v1/balance is the balance read's own.
		t.Fatalf("%d papi margin reads in a swap-scoped read, want only the balance read's", n)
	}
}

// A Pro key without the Futures scope is not probed: its fills and funding
// cannot be read, and futures_permission_missing already says so.
func TestBinance_PMPro_NoProbeWithoutFuturesScope(t *testing.T) {
	srv := pmProAccount()
	srv.fapiDenied = true
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if n := srv.count("/sapi/v1/portfolio/account"); n != 0 {
		t.Fatalf("probed %d times without the Futures scope, want 0", n)
	}
	if slices.Contains(b.CapabilityWarnings(), binancePMTradesUnreadable) {
		t.Fatalf("warnings = %v: pm_trades_unreadable on top of the scope warning", b.CapabilityWarnings())
	}
}

// The gate reads the cross margin wallet gross: a pool whose assets and
// debts net to zero is still a pool.
func TestBinance_PMPro_ProbedOnGrossCrossMargin(t *testing.T) {
	srv := pmProAccount()
	srv.crossNetBtc, srv.crossAssetBtc = "0", "1.5"
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if pro, _ := b.portfolioMarginProState(); pro != binancePMYes || srv.count("/sapi/v1/portfolio/account") != 1 {
		t.Fatalf("verdict = %v after %d probes, want Pro after one", pro, srv.count("/sapi/v1/portfolio/account"))
	}
}

func strconvItoa(v int64) string { return strconv.FormatInt(v, 10) }

// The sync reads funding over whole UTC days from midnight: on both
// Portfolio Margin flavours a settlement stamped exactly at that midnight is
// inside the read, and the income is asked from that very millisecond.
func TestBinance_PortfolioMarginFunding_SettlementOnTheMidnightItStartsFrom(t *testing.T) {
	midnight := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	rows := []pmIncomeRow{
		{symbol: "BTCUSDT", incomeType: "FUNDING_FEE", income: "-3", asset: "USDT", time: midnight, tranID: 1},
		{symbol: "BTCUSDT", incomeType: "FUNDING_FEE", income: "-7", asset: "USDT", time: midnight.Add(-time.Millisecond), tranID: 2},
	}
	for name, c := range map[string]struct {
		srv  *binancePMActivityServer
		path string
	}{
		"papi":   {&binancePMActivityServer{portfolioMargin: true, umIncome: rows}, "/papi/v1/um/income"},
		"pm pro": {func() *binancePMActivityServer { s := pmProAccount(); s.umIncome = rows; return s }(), "/fapi/v1/income"},
	} {
		t.Run(name, func(t *testing.T) {
			b := c.srv.connector(t)
			if _, err := b.GetBalance(context.Background()); err != nil {
				t.Fatalf("GetBalance: %v", err)
			}
			fees, err := b.GetFundingFees(context.Background(), nil, midnight)
			if err != nil {
				t.Fatalf("GetFundingFees: %v", err)
			}
			if len(fees) != 1 || fees[0].Amount != -3 || !fees[0].Timestamp.Equal(midnight) {
				t.Fatalf("fees = %+v, want the midnight settlement alone", fees)
			}
			c.srv.mu.Lock()
			defer c.srv.mu.Unlock()
			for _, u := range c.srv.calls {
				if u.Path == c.path {
					if st := u.Query().Get("startTime"); st != strconv.FormatInt(midnight.UnixMilli(), 10) {
						t.Fatalf("startTime = %s, want the midnight itself", st)
					}
					return
				}
			}
			t.Fatalf("%s never read", c.path)
		})
	}
}

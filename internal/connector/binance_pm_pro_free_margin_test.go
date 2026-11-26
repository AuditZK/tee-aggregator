package connector

import (
	"context"
	"math"
	"net/http"
	"strings"
	"testing"
)

// The PM Pro fixture's USDⓈ-M wallet under Pro: availableBalance 0 (the
// positions draw on the pool), a BTCUSDT long of 1 at 50000 marked 51000 at
// 10x (5100 held), and ETHUSDT hedge legs +2 and -1 at 3000 at 5x, netting
// to 1 ETH (600 held, as the rebuilder nets them). 5700 held in all.
const pmProFapiAccount = `{"totalMarginBalance":"700","totalUnrealizedProfit":"1000","availableBalance":"0","positions":[` +
	`{"symbol":"BTCUSDT","positionAmt":"1","entryPrice":"50000","unrealizedProfit":"1000","leverage":"10","positionSide":"BOTH"},` +
	`{"symbol":"ETHUSDT","positionAmt":"2","entryPrice":"3000","unrealizedProfit":"0","leverage":"5","positionSide":"LONG"},` +
	`{"symbol":"ETHUSDT","positionAmt":"-1","entryPrice":"3000","unrealizedProfit":"0","leverage":"5","positionSide":"SHORT"},` +
	`{"symbol":"SOLUSDT","positionAmt":"0","entryPrice":"0","unrealizedProfit":"0","leverage":"20","positionSide":"BOTH"}]}`

func splitOf(t *testing.T, b *Binance) map[string]*MarketBalance {
	t.Helper()
	out := map[string]*MarketBalance{}
	for _, m := range mustMarket(t, b) {
		out[m.MarketType] = m
	}
	return out
}

func TestBinance_PMPro_FreeMarginIsPoolLessPositionMargin(t *testing.T) {
	srv := pmProAccount()
	srv.fapiAccount = pmProFapiAccount
	b := srv.connector(t)

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// Equity unchanged: spot 100 + UM 700 + cross margin 240000.
	if bal.Equity != 240800 {
		t.Fatalf("equity = %v, want 240800", bal.Equity)
	}
	split := splitOf(t, b)
	// Swap is the pool (240000 + 700) less 5700 held; spot is the spot wallet.
	if s := split[MarketSwap]; s == nil || s.Equity != 240700 || math.Abs(s.AvailableMargin-235000) > 1e-6 {
		t.Fatalf("swap = %+v, want pool 240700 with 235000 free", s)
	}
	if s := split[MarketSpot]; s == nil || s.Equity != 100 || s.AvailableMargin != 100 {
		t.Fatalf("spot = %+v, want the spot wallet 100 / 100", s)
	}
	// Before: the pool sat in spot at available = equity and the UM wallet
	// at availableBalance 0, 240100 free with nothing held.
	if math.Abs(bal.Available-235100) > 1e-6 {
		t.Fatalf("available = %v, want 235100 (not the 240100 of a pool read as all free)", bal.Available)
	}
}

func TestBinance_PMPro_FreeMarginFloorsAtZero(t *testing.T) {
	srv := pmProAccount()
	srv.crossNetBtc = "0.05" // 2500 in the pool
	srv.fapiAccount = pmProFapiAccount
	b := srv.connector(t)
	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if s := splitOf(t, b)[MarketSwap]; s == nil || s.Equity != 3200 || s.AvailableMargin != 0 {
		t.Fatalf("swap = %+v, want pool 3200 with nothing free (5700 held)", s)
	}
	if bal.Available != 100 || bal.Available > bal.Equity {
		t.Fatalf("available = %v of equity %v, want the spot 100 only", bal.Available, bal.Equity)
	}
}

func TestBinance_PMPro_CoinMPositionsHoldMarginToo(t *testing.T) {
	srv := pmProAccount()
	srv.fapiAccount = pmProFapiAccount
	srv.coinMBalance = `[{"asset":"BTC","balance":"0.1","crossUnPnl":"0"}]` // 5000
	// 10 contracts, notional 0.02 BTC at mark 50000 = 1000 USD, 20x: 50 held.
	srv.cmPositionRisk = `[{"symbol":"BTCUSD_PERP","positionAmt":"10","markPrice":"50000","leverage":"20","notionalValue":"0.02"}]`
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if s := splitOf(t, b)[MarketSwap]; s == nil || s.Equity != 245700 || math.Abs(s.AvailableMargin-239950) > 1e-6 {
		t.Fatalf("swap = %+v, want pool 245700 (cross + UM + COIN-M) with 239950 free", s)
	}
}

// A classic account keeps its split exactly: positions on fapi change
// nothing, the USDⓈ-M wallet keeps its own availableBalance.
func TestBinance_ClassicSplitUnchangedByPositions(t *testing.T) {
	for name, account := range map[string]string{
		"no positions": "",
		"positions":    `{"totalMarginBalance":"700","totalUnrealizedProfit":"0","availableBalance":"600","positions":[{"symbol":"BTCUSDT","positionAmt":"1","entryPrice":"50000","unrealizedProfit":"0","leverage":"10"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := &binancePMActivityServer{crossNetBtc: "2", fapiAccount: account}
			b := srv.connector(t)
			bal, err := b.GetBalance(context.Background())
			if err != nil {
				t.Fatalf("GetBalance: %v", err)
			}
			split := splitOf(t, b)
			if s := split[MarketSpot]; s == nil || s.Equity != 100100 || s.AvailableMargin != 100100 {
				t.Fatalf("spot = %+v, want 100100 / 100100", s)
			}
			if s := split[MarketSwap]; s == nil || s.Equity != 700 || s.AvailableMargin != 600 {
				t.Fatalf("swap = %+v, want 700 / 600", s)
			}
			if bal.Equity != 100800 || bal.Available != 100700 {
				t.Fatalf("balance = %v / %v, want 100800 / 100700", bal.Equity, bal.Available)
			}
		})
	}
}

// A papi Portfolio Margin account keeps its split: the pool in swap at
// actualEquity less what papi says is held.
func TestBinance_PapiPMSplitUnchanged(t *testing.T) {
	b, _ := newBinancePMConnector(t, "denied", "ok")
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if s := splitOf(t, b)[MarketSwap]; s == nil || s.Equity != 5000 || s.AvailableMargin != 3700 {
		t.Fatalf("swap = %+v, want 5000 / 3700", s)
	}
}

func TestBinanceUMPositionMargin(t *testing.T) {
	rows := []binanceFuturesPositionRow{
		{Symbol: "BTCUSDT", PositionAmt: "-0.5", EntryPrice: "50000", UnrealizedProfit: "-500", Leverage: "5"}, // short marked 51000: 25500 / 5
		{Symbol: "XRPUSDT", PositionAmt: "100", EntryPrice: "1", UnrealizedProfit: "0", Leverage: ""},          // no leverage: 0
	}
	if got := binanceUMPositionMargin(rows); math.Abs(got-5100) > 1e-9 {
		t.Fatalf("held = %v, want 5100", got)
	}
}

// /fapi/v2/account answering 200 with zero equity sends the read to
// /fapi/v2/balance, which lists no position: the degraded answer's position
// list must not feed a Pro split that would then hold nothing.
func TestBinance_PMPro_BalanceEndpointFallbackKeepsClassicSplit(t *testing.T) {
	srv := pmProAccount()
	srv.fapiAccount = `{"totalMarginBalance":"0","totalUnrealizedProfit":"0","availableBalance":"0","positions":[]}`
	srv.fapiBalance = `[{"asset":"USDT","balance":"700","crossUnPnl":"0","availableBalance":"650"}]`
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	split := splitOf(t, b)
	if s := split[MarketSwap]; s == nil || s.Equity != 700 || s.AvailableMargin != 650 {
		t.Fatalf("swap = %+v, want the classic USDⓈ-M wallet 700 / 650", s)
	}
	if s := split[MarketSpot]; s == nil || s.Equity != 240100 {
		t.Fatalf("spot = %+v, want the classic 240100", s)
	}
	if d := b.BalanceDegradations(); len(d) != 1 || !strings.Contains(d[0], "balance endpoint fallback") {
		t.Fatalf("degradations = %v, want the fallback named", d)
	}
}

// A refused COIN-M positions read keeps the classic split, says why for the
// operator, and raises no marker the user would see.
func TestBinance_PMPro_CoinMPositionsUnreadableIsLoggedNotMarked(t *testing.T) {
	srv := pmProAccount()
	srv.fapiAccount = pmProFapiAccount
	srv.coinMBalance = `[{"asset":"BTC","balance":"0.1","crossUnPnl":"0"}]`
	srv.cmPositionStatus = http.StatusTeapot
	srv.portfolioAccount = `{"accountType":"PM_1","actualEquity":"245600"}`
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if s := splitOf(t, b)[MarketSwap]; s == nil || s.Equity != 700 {
		t.Fatalf("swap = %+v, want the classic split", s)
	}
	d := b.BalanceDegradations()
	if len(d) != 1 || !strings.Contains(d[0], "COIN-M positions unreadable (HTTP 418)") {
		t.Fatalf("degradations = %v, want the COIN-M read and its cause", d)
	}
	if again := b.BalanceDegradations(); len(again) != 0 {
		t.Fatalf("degradations handed out twice: %v", again)
	}
	if w := b.CapabilityWarnings(); len(w) != 0 {
		t.Fatalf("warnings = %v, want no marker", w)
	}
}

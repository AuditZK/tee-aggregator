package connector

import (
	"context"
	"strings"
	"testing"
)

// The default pool with 3.2 more USDT in the UM wallet: priced at 5003.2
// while papi's actualEquity stays at 5000, the field case of 2026-11 (the
// pool's assets at market value sit ~0.14% above actualEquity).
var binancePMBalanceAbove = strings.Replace(binancePMBalanceBody, `"umWalletBalance":"1650.5"`, `"umWalletBalance":"1653.7"`, 1)

func bucketSum(t *testing.T, b *Binance) float64 {
	t.Helper()
	sum := 0.0
	for _, m := range mustMarket(t, b) {
		sum += m.Equity
	}
	return sum
}

func TestBinance_PortfolioMargin_EquityAtMarketValue(t *testing.T) {
	b, _ := newBinancePMConnectorWith(t, "denied", "ok", binancePMBalanceAbove)

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !near(bal.Equity, 5103.2) {
		t.Fatalf("equity = %v, want 5103.2 (spot 100 + the pool at market value 5003.2, not actualEquity 5000)", bal.Equity)
	}
	if !near(bal.Available, 3803.2) {
		t.Fatalf("available = %v, want 3803.2 (spot 100 + pool 5003.2 less 1300 held by positions and orders)", bal.Available)
	}
	m := marketMap(t, mustMarket(t, b))
	if swap := m[MarketSwap]; swap == nil || !near(swap.Equity, 5003.2) || !near(swap.AvailableMargin, 3703.2) {
		t.Fatalf("swap bucket = %+v, want equity 5003.2 / available 3703.2", swap)
	}
	if sum := bucketSum(t, b); !near(sum, bal.Equity) {
		t.Fatalf("buckets sum to %v, equity is %v", sum, bal.Equity)
	}

	note, ok := b.EquityBasisNote()
	if !ok || note.Basis != EquityBasisMarketValue || note.Fallback != "" {
		t.Fatalf("note = %+v (ok %v), want the market value basis", note, ok)
	}
	if !near(note.Residual, 3.2) || !near(note.ResidualShare, 3.2/5000) {
		t.Fatalf("residual = %v / share %v, want 3.2 / %v", note.Residual, note.ResidualShare, 3.2/5000)
	}
	if _, again := b.EquityBasisNote(); again {
		t.Fatal("the note of one read handed out twice")
	}
}

// An asset no ticker prices is not valued at 0: the whole pool keeps papi's
// actualEquity for that read, and the note says why.
func TestBinance_PortfolioMargin_UnpricedAssetKeepsActualEquity(t *testing.T) {
	withUnpriced := strings.TrimSuffix(binancePMBalanceAbove, "]") +
		`,{"asset":"XYZ","crossMarginFree":"10","crossMarginLocked":"0","crossMarginBorrowed":"0","crossMarginInterest":"0","umWalletBalance":"0","umUnrealizedPNL":"0","cmWalletBalance":"0","cmUnrealizedPNL":"0"}]`
	b, _ := newBinancePMConnectorWith(t, "denied", "ok", withUnpriced)

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !near(bal.Equity, 5100) {
		t.Fatalf("equity = %v, want 5100 (spot 100 + actualEquity 5000)", bal.Equity)
	}
	if sum := bucketSum(t, b); !near(sum, bal.Equity) {
		t.Fatalf("buckets sum to %v, equity is %v", sum, bal.Equity)
	}
	note, ok := b.EquityBasisNote()
	// The log line carries a count, never which assets the user holds.
	if !ok || note.Basis != EquityBasisVenue || note.Fallback != "1 asset without a spot price" || strings.Contains(note.Fallback, "XYZ") {
		t.Fatalf("note = %+v (ok %v), want the venue basis and the unpriced assets counted, not named", note, ok)
	}
	if note.Residual != 0 {
		t.Fatalf("residual = %v, want 0 when the market value could not be taken", note.Residual)
	}
}

// A pool whose market value misses actualEquity by more than
// max(0.5%, $50) has a sub-ledger read wrong: papi's figure is kept.
func TestBinance_PortfolioMargin_FarFromActualEquityKeepsIt(t *testing.T) {
	far := strings.Replace(binancePMBalanceBody, `"umWalletBalance":"1650.5"`, `"umWalletBalance":"1750.5"`, 1)
	b, _ := newBinancePMConnectorWith(t, "denied", "ok", far)

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !near(bal.Equity, 5100) {
		t.Fatalf("equity = %v, want 5100 (spot 100 + actualEquity 5000)", bal.Equity)
	}
	note, ok := b.EquityBasisNote()
	if !ok || note.Basis != EquityBasisVenue || note.Fallback == "" || !near(note.Residual, 100) {
		t.Fatalf("note = %+v (ok %v), want the venue basis, a reason and the 100 residual", note, ok)
	}
}

// A classic account (fapi answers) never reaches papi and carries no note.
func TestBinance_ClassicAccountCarriesNoEquityBasisNote(t *testing.T) {
	b, _ := newBinancePMConnector(t, "ok", "ok")

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 100800 {
		t.Fatalf("equity = %v, want 100800 (spot 100 + futures 700 + cross margin 2 BTC)", bal.Equity)
	}
	if note, ok := b.EquityBasisNote(); ok {
		t.Fatalf("note = %+v on a classic account", note)
	}
}

func TestSpotPriceUSD(t *testing.T) {
	prices := map[string]float64{"BTCUSDT": 50000, "ETHUSDC": 3000, "ABCBTC": 0.001}
	for _, tc := range []struct {
		asset string
		want  float64
		ok    bool
	}{
		{"USDT", 1, true},
		{"usdc", 1, true},
		{"BTC", 50000, true},
		{"ETH", 3000, true},
		{"ABC", 50, true},
		{"XYZ", 0, false},
	} {
		got, ok := SpotPriceUSD(tc.asset, prices)
		if ok != tc.ok || !near(got, tc.want) {
			t.Errorf("SpotPriceUSD(%s) = %v, %v; want %v, %v", tc.asset, got, ok, tc.want, tc.ok)
		}
	}
}

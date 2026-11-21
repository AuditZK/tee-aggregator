package connector

import (
	"context"
	"math"
	"net/http"
	"testing"
)

// Reading free margin off the USDT detail line alone reported a free margin of
// zero — and an unrealized P&L of zero — for every account settled in anything
// else, while total equity stayed correct. Observed on an OKX account whose
// equity tracked fine for nine days with realized balance pinned to equity on
// every single one of them.
func TestOKXGetBalance_NonUSDTSettlementStillReportsFreeMargin(t *testing.T) {
	body := `{"code":"0","data":[{"totalEq":"3569.14","details":[` +
		`{"ccy":"USDC","eq":"3569.14","eqUsd":"3569.14","availBal":"1204.55","upl":"-31.20"}` +
		`]}]}`
	okx := newOKXAcross(newOKXFakeRegion(t, http.StatusOK, body))

	balance, err := okx.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if balance.Available == 0 {
		t.Fatal("free margin is zero on a USDC-settled account — the dashboard shows no margin at all")
	}
	assertNear(t, "available", balance.Available, 1204.55)
	assertNear(t, "unrealized", balance.UnrealizedPnL, -31.20)
	assertNear(t, "equity", balance.Equity, 3569.14)
}

// A coin-margined line is worth its USD value, not its quantity: 0.4 BTC of
// free margin is not 0.4 dollars.
func TestOKXGetBalance_ConvertsEachCurrencyToUSD(t *testing.T) {
	body := `{"code":"0","data":[{"totalEq":"52000","details":[` +
		`{"ccy":"USDT","eq":"2000","eqUsd":"2000","availBal":"1500","upl":"10"},` +
		`{"ccy":"BTC","eq":"0.5","eqUsd":"50000","availBal":"0.4","upl":"0.01"}` +
		`]}]}`
	okx := newOKXAcross(newOKXFakeRegion(t, http.StatusOK, body))

	balance, err := okx.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// BTC prices at 50000/0.5 = 100000 per unit.
	assertNear(t, "available", balance.Available, 1500+0.4*100000)
	assertNear(t, "unrealized", balance.UnrealizedPnL, 10+0.01*100000)
}

// The USDT path is the one every existing account is on. It must come through
// the multi-currency rewrite byte for byte.
func TestOKXGetBalance_USDTAccountUnchanged(t *testing.T) {
	body := `{"code":"0","data":[{"totalEq":"42327.43","details":[` +
		`{"ccy":"USDT","eq":"42327.43","eqUsd":"42331.66","availBal":"38210.02","upl":"215.41"}` +
		`]}]}`
	okx := newOKXAcross(newOKXFakeRegion(t, http.StatusOK, body))

	balance, err := okx.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// The peg drifts by a hair; the figures must not move by more than it.
	assertWithin(t, "available", balance.Available, 38210.02, 5)
	assertWithin(t, "unrealized", balance.UnrealizedPnL, 215.41, 1)
	assertNear(t, "equity", balance.Equity, 42327.43)
}

// OKX omits eqUsd on some responses. A USD-pegged line must still count rather
// than collapse to zero.
func TestOKXGetBalance_MissingEqUsdFallsBackToParity(t *testing.T) {
	okx := newOKXAcross(newOKXFakeRegion(t, http.StatusOK, okxBodyBalanceOK))

	balance, err := okx.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	assertNear(t, "available", balance.Available, 1000)
	assertNear(t, "equity", balance.Equity, 1234.5)
}

func TestOKXGetBalance_NoDetailsIsNotAnError(t *testing.T) {
	body := `{"code":"0","data":[{"totalEq":"0","details":[]}]}`
	okx := newOKXAcross(newOKXFakeRegion(t, http.StatusOK, body))

	balance, err := okx.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if balance.Available != 0 || balance.UnrealizedPnL != 0 {
		t.Fatalf("empty details produced available=%v upl=%v, want zeros", balance.Available, balance.UnrealizedPnL)
	}
}

func assertNear(t *testing.T, name string, got, want float64) {
	t.Helper()
	assertWithin(t, name, got, want, 1e-6)
}

func assertWithin(t *testing.T, name string, got, want, tolerance float64) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %v, want %v (tolerance %v)", name, got, want, tolerance)
	}
}

package connector

import (
	"context"
	"net/http"
	"slices"
	"testing"
	"time"
)

// The classic account of binancePMActivityServer measures 800: spot 100 USDT
// and a USDⓈ-M wallet of 700.
func walletRows(extra string) string {
	return `[{"activate":true,"balance":"100","walletName":"Spot"},` +
		`{"activate":true,"balance":"700","walletName":"USDⓈ-M Futures"},` +
		`{"activate":true,"balance":"0","walletName":"Cross Margin"},` +
		`{"activate":true,"balance":"0","walletName":"Isolated Margin"},` +
		`{"activate":true,"balance":"0","walletName":"COIN-M Futures"},` +
		`{"activate":true,"balance":"0","walletName":"Earn"},` +
		`{"activate":false,"balance":"0","walletName":"Options"},` +
		`{"activate":true,"balance":"0","walletName":"Trading Bots"},` +
		`{"activate":true,"balance":"0","walletName":"Copy Trading"},` +
		extra + `]`
}

func balanceWithWallets(t *testing.T, srv *binancePMActivityServer) (*Balance, []string) {
	t.Helper()
	b := srv.connector(t)
	b.walletCheckTimeout = 50 * time.Millisecond
	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	return bal, b.CapabilityWarnings()
}

func TestBinance_WalletCoverage_FundingWalletHoldingMoney(t *testing.T) {
	bal, warnings := balanceWithWallets(t, &binancePMActivityServer{
		wallets: walletRows(`{"activate":true,"balance":"500","walletName":"Funding"}`),
	})
	if bal.Equity != 800 {
		t.Fatalf("equity = %v, want 800 unchanged by the check", bal.Equity)
	}
	if !slices.Equal(warnings, []string{binanceMarkerUnmeasuredWallet}) {
		t.Fatalf("warnings = %v, want only %s", warnings, binanceMarkerUnmeasuredWallet)
	}
}

func TestBinance_WalletCoverage_AllWalletsMeasured(t *testing.T) {
	_, warnings := balanceWithWallets(t, &binancePMActivityServer{
		// Dust in Funding, and a total 0.5% off the equity: both normal.
		wallets: `[{"balance":"104","walletName":"Spot"},{"balance":"700","walletName":"USDⓈ-M Futures"},` +
			`{"balance":"4.5","walletName":"Funding"},{"balance":"0","walletName":"Earn"}]`,
	})
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v, want none", warnings)
	}
}

func TestBinance_WalletCoverage_TotalMismatch(t *testing.T) {
	_, warnings := balanceWithWallets(t, &binancePMActivityServer{
		wallets: `[{"balance":"100","walletName":"Spot"},{"balance":"1700","walletName":"USDⓈ-M Futures"}]`,
	})
	if !slices.Equal(warnings, []string{binanceMarkerWalletMismatch}) {
		t.Fatalf("warnings = %v, want only %s", warnings, binanceMarkerWalletMismatch)
	}
}

// The PM pool is read as actualEquity, about 0.15% under Binance's own
// valuation of it: that is not a mismatch.
func TestBinance_WalletCoverage_PortfolioMarginHaircutIsNoMismatch(t *testing.T) {
	_, warnings := balanceWithWallets(t, &binancePMActivityServer{
		portfolioMargin: true,
		wallets:         `[{"balance":"100","walletName":"Spot"},{"balance":"5007.5","walletName":"Cross Margin (PM)"},{"balance":"0","walletName":"Funding"}]`,
	})
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v, want none", warnings)
	}
}

// Whatever the endpoint does, the balance read returns what it returned
// without it, with the same markers, after one attempt.
func TestBinance_WalletCoverage_FailureChangesNothing(t *testing.T) {
	cases := map[string]*binancePMActivityServer{
		"500":       {walletStatus: http.StatusInternalServerError},
		"418":       {walletStatus: http.StatusTeapot},
		"429":       {walletStatus: http.StatusTooManyRequests},
		"refused":   {walletStatus: http.StatusUnauthorized},
		"timeout":   {walletDelay: 2 * time.Second, wallets: walletRows(`{"balance":"500","walletName":"Funding"}`)},
		"not json":  {wallets: `<html>`},
		"empty":     {wallets: `[]`},
		"no answer": {},
	}
	for name, srv := range cases {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			bal, warnings := balanceWithWallets(t, srv)
			if bal.Equity != 800 || bal.Available != 700 || bal.UnrealizedPnL != 0 {
				t.Fatalf("balance = %+v, want 800 / 700 / 0", bal)
			}
			if len(warnings) != 0 {
				t.Fatalf("warnings = %v, want none", warnings)
			}
			if n := srv.count("/sapi/v1/asset/wallet/balance"); n != 1 {
				t.Fatalf("wallet endpoint called %d times, want 1 (no retry)", n)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("balance read took %v: the check slowed the sync", elapsed)
			}
		})
	}
}

func TestBinanceWalletKey(t *testing.T) {
	for name, want := range map[string]string{
		"USDⓈ-M Futures":    "usdmfutures",
		"Cross Margin (PM)": "crossmarginpm",
		"COIN-M Futures":    "coinmfutures",
		"Trading Bots":      "tradingbots",
	} {
		if got := binanceWalletKey(name); got != want {
			t.Errorf("binanceWalletKey(%q) = %q, want %q", name, got, want)
		}
	}
	if binanceWalletRead[binanceWalletKey("Some New Wallet")] {
		t.Fatal("an unknown wallet counts as measured")
	}
}

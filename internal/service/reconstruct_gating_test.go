package service

import "testing"

// TestReconstructsEverySync guards the invariant that only the in-enclave
// reconstruction-capable exchanges (IBKR, cTrader) re-run reconstruction every
// sync. Live-only exchanges must be false so they are never routed away from
// their mandatory live 24h cash-flow window. The real safety net is the
// connector.HistoricalSnapshotProvider type assertion at the call sites; this
// guards the name-level helper as defense in depth.
func TestReconstructsEverySync(t *testing.T) {
	cases := map[string]bool{
		"ibkr":        true,
		"IBKR":        true,
		"ctrader":     true,
		"cTrader":     true,
		"hyperliquid": false,
		"mexc":        false,
		"lighter":     false,
		"alpaca":      false,
		"bitget":      false,
		"":            false,
	}
	for ex, want := range cases {
		if got := reconstructsEverySync(ex); got != want {
			t.Errorf("reconstructsEverySync(%q) = %v, want %v", ex, got, want)
		}
	}
}

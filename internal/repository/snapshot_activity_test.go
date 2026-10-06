package repository

import (
	"encoding/json"
	"testing"
)

const storedLiveDay = `{"swap":{"equity":75952.49,"trades":29,"volume":192304.16,"funding_fees":0,"trading_fees":19.35,"available_margin":57116.05,"kept":"x"},` +
	`"global":{"equity":75952.49,"trades":29,"volume":192304.16,"funding_fees":0,"trading_fees":19.35,"available_margin":57116.05}}`

// Only activity moves: equity, margin and any key this struct does not know
// stay exactly as stored.
func TestPatchActivityKeepsEverythingElse(t *testing.T) {
	activity := MarketMetrics{Volume: 3000, Trades: 12, TradingFees: 1.8, FundingFees: 20.5, LongTrades: 7, ShortTrades: 5, LongVolume: 1800, ShortVolume: 1200}
	swap, global := activity, activity
	next := &MarketBreakdown{Swap: &swap, Global: &global}

	out, ok, err := patchActivity([]byte(storedLiveDay), 29, 192304.16, next)
	if err != nil || !ok {
		t.Fatalf("patch: ok=%v err=%v", ok, err)
	}

	var doc map[string]map[string]any
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"swap", "global"} {
		m := doc[key]
		if m["equity"] != 75952.49 || m["available_margin"] != 57116.05 {
			t.Fatalf("%s equity/margin changed: %v", key, m)
		}
		if m["trades"] != 12.0 || m["volume"] != 3000.0 || m["funding_fees"] != 20.5 || m["long_trades"] != 7.0 || m["short_trades"] != 5.0 {
			t.Fatalf("%s activity not patched: %v", key, m)
		}
	}
	if doc["swap"]["kept"] != "x" {
		t.Fatalf("unknown key dropped: %v", doc["swap"])
	}
}

// A row rewritten since the dry run read it is left alone.
func TestPatchActivityRefusesAMovedRow(t *testing.T) {
	next := &MarketBreakdown{Global: &MarketMetrics{Trades: 1}}
	if _, ok, err := patchActivity([]byte(storedLiveDay), 30, 192304.16, next); err != nil || ok {
		t.Fatalf("trades moved: ok=%v err=%v, want refused", ok, err)
	}
	if _, ok, err := patchActivity([]byte(storedLiveDay), 29, 1, next); err != nil || ok {
		t.Fatalf("volume moved: ok=%v err=%v, want refused", ok, err)
	}
}

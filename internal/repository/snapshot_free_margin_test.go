package repository

import (
	"encoding/json"
	"reflect"
	"testing"
)

// A rebuilt classic-path day of a Portfolio Margin Pro account: the cross
// margin pool in spot at available = equity, the USDⓈ-M wallet floored at 0.
// The numbers carry more digits than a float round trip would keep exactly
// if it re-encoded them, and an unknown key.
const storedRebuiltDay = `{"spot":{"equity":241946.123456789012,"available_margin":241946.123456789012,"volume":0,"trades":0,"trading_fees":0,"funding_fees":0,"x_unknown":{"a":[1,2]}},` +
	`"swap":{"equity":10115.5,"volume":1234.5,"trades":7,"trading_fees":0.61,"funding_fees":-3.25},` +
	`"global":{"equity":252061.623456789012,"available_margin":252061.623456789012,"volume":1234.5,"trades":7,"trading_fees":0.61,"funding_fees":-3.25,"long_trades":4,"short_trades":3}}`

func entries(t *testing.T, raw []byte) map[string]map[string]json.RawMessage {
	t.Helper()
	top := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]json.RawMessage{}
	for k, v := range top {
		e := map[string]json.RawMessage{}
		if err := json.Unmarshal(v, &e); err != nil {
			t.Fatal(err)
		}
		out[k] = e
	}
	return out
}

// Before/after: only available_margin of the named entries moves; every
// other key of every entry keeps its stored bytes, equity included.
func TestPatchAvailableMarginTouchesOnlyFreeMargin(t *testing.T) {
	expect := map[string]MarketMargin{
		"spot":   {Equity: 241946.123456789012, AvailableMargin: 241946.123456789012},
		"swap":   {Equity: 10115.5, AvailableMargin: 0},
		"global": {Equity: 252061.623456789012, AvailableMargin: 252061.623456789012},
	}
	next := map[string]float64{"spot": 231946.12, "swap": 0, "global": 231946.12}
	out, ok, err := patchAvailableMargin([]byte(storedRebuiltDay), expect, next)
	if err != nil || !ok {
		t.Fatalf("patch: ok=%v err=%v", ok, err)
	}

	before, after := entries(t, []byte(storedRebuiltDay)), entries(t, out)
	if len(before) != len(after) {
		t.Fatalf("entries %d -> %d", len(before), len(after))
	}
	for key, b := range before {
		a := after[key]
		for field, v := range b {
			if field == "available_margin" {
				continue
			}
			if string(a[field]) != string(v) {
				t.Errorf("%s.%s changed: %s -> %s", key, field, v, a[field])
			}
		}
		for field := range a {
			if _, had := b[field]; !had && field != "available_margin" {
				t.Errorf("%s.%s added", key, field)
			}
		}
	}
	for key, want := range next {
		var got float64
		if err := json.Unmarshal(after[key]["available_margin"], &got); err != nil || got != want {
			t.Errorf("%s.available_margin = %s, want %v", key, after[key]["available_margin"], want)
		}
	}
}

// A row rewritten since the dry run read it (equity or free margin moved) is
// left alone, and so is a row without the entry.
func TestPatchAvailableMarginRefusesAMovedRow(t *testing.T) {
	good := map[string]MarketMargin{"spot": {Equity: 241946.123456789012, AvailableMargin: 241946.123456789012}}
	next := map[string]float64{"spot": 1}
	for name, expect := range map[string]map[string]MarketMargin{
		"equity moved": {"spot": {Equity: 241000, AvailableMargin: 241946.123456789012}},
		"margin moved": {"spot": {Equity: 241946.123456789012, AvailableMargin: 1000}},
	} {
		if _, ok, err := patchAvailableMargin([]byte(storedRebuiltDay), expect, next); err != nil || ok {
			t.Errorf("%s: ok=%v err=%v, want refused", name, ok, err)
		}
	}
	if _, ok, err := patchAvailableMargin([]byte(storedRebuiltDay), map[string]MarketMargin{"futures": {}}, map[string]float64{"futures": 1}); err != nil || ok {
		t.Errorf("absent entry: ok=%v err=%v, want refused", ok, err)
	}
	if _, ok, err := patchAvailableMargin([]byte(storedRebuiltDay), good, map[string]float64{"spot": 1, "swap": 0}); err == nil || ok {
		t.Errorf("entry without expectation: ok=%v err=%v, want an error", ok, err)
	}
	out, ok, err := patchAvailableMargin([]byte(storedRebuiltDay), good, next)
	if err != nil || !ok {
		t.Fatalf("matching row refused: ok=%v err=%v", ok, err)
	}
	if reflect.DeepEqual(out, []byte(storedRebuiltDay)) {
		t.Fatal("nothing patched")
	}
}

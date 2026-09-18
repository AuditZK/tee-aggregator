package connector

import (
	"testing"
	"time"
)

// OKX's asset-bills endpoints ignore the begin and end they are sent. A probe
// asking for one day came back with two months, and a daily sync believed it:
// three accounts spent two nights reporting their entire lifetime capital as a
// single day's inflow, each showing a daily return near -90%.
func TestOKXFlowsSince_DropsWhatTheVenueSentOutsideTheWindow(t *testing.T) {
	since := time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)
	flows := []*Cashflow{
		{Amount: 1000, Currency: "USDT", Timestamp: time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)},
		{Amount: -500, Currency: "USDT", Timestamp: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)},
		{Amount: 250, Currency: "USDT", Timestamp: since},
		{Amount: 75, Currency: "USDT", Timestamp: time.Date(2026, 9, 17, 18, 30, 0, 0, time.UTC)},
	}

	kept := okxFlowsSince(flows, since)
	if len(kept) != 2 {
		t.Fatalf("kept %d flows, want 2: %+v", len(kept), kept)
	}
	// The boundary belongs to the window: a flow stamped exactly at `since`
	// is inside it, or the day it opens loses its own morning.
	if kept[0].Amount != 250 || kept[1].Amount != 75 {
		t.Fatalf("kept the wrong flows: %v, %v", kept[0].Amount, kept[1].Amount)
	}
}

// A caller with no window wants everything: the reconstruction path and the
// diagnostic probe both rely on that.
func TestOKXFlowsSince_ZeroWindowKeepsEverything(t *testing.T) {
	flows := []*Cashflow{
		{Amount: 10, Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Amount: 20, Timestamp: time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)},
	}
	if got := okxFlowsSince(flows, time.Time{}); len(got) != 2 {
		t.Fatalf("kept %d, want both", len(got))
	}
}

// The window is applied AFTER classification, never before it: pairing a
// trading transfer with its funding twin needs both legs, and trimming the
// twin would book a move between two wallets as a crossing that never
// happened. This pins the order by feeding a pair that straddles `since`.
func TestOKXCashflows_PairingSurvivesAWindowThatSplitsThePair(t *testing.T) {
	since := time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)
	justBefore := since.Add(-3 * time.Minute)
	justAfter := since.Add(2 * time.Minute)

	// A withdrawal out of trading at 23:57 and its arrival in funding at
	// 00:02: one internal move, no crossing, whatever the window says.
	bills := []okxBill{
		{BillID: "t1", T: justBefore, Ccy: "USDT", BalChg: -400, Type: "1"},
	}
	funding := []okxBill{
		{BillID: "f1", T: justAfter, Ccy: "USDT", BalChg: 400, Type: "130"},
	}

	flows, _ := okxClassifyCashflows(bills, funding, nil)
	kept := okxFlowsSince(flows, since)
	if len(kept) != 0 {
		t.Fatalf("an internal transfer must not cross the perimeter, got %+v", kept[0])
	}
}

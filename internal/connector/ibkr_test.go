package connector

import (
	"testing"
	"time"
)

// aggregateFlexTradesByDate must split each day's activity by direction —
// historical IBKR snapshots feed the dashboard's Long/Short Proportion
// chart from these counters (live sync gets them from aggregateTrades).
func TestAggregateFlexTradesByDate_LongShortSplit(t *testing.T) {
	day := time.Date(2026, 6, 9, 14, 30, 0, 0, time.UTC)
	trades := []*Trade{
		{Side: "buy", Price: 100, Quantity: 2, Fee: 1, Timestamp: day},
		{Side: "sell", Price: 50, Quantity: 1, Fee: 0.5, Timestamp: day.Add(time.Hour)},
		{Side: "buy", Price: 10, Quantity: 10, Fee: 0.25, Timestamp: day.Add(26 * time.Hour)}, // next day
	}

	byDate := aggregateFlexTradesByDate(trades)

	d1 := byDate["20260609"]
	if d1.count != 2 || d1.longTrades != 1 || d1.shortTrades != 1 {
		t.Fatalf("day1 split: count=%d long=%d short=%d, want 2/1/1",
			d1.count, d1.longTrades, d1.shortTrades)
	}
	if d1.longVolume != 200 || d1.shortVolume != 50 || d1.volume != 250 {
		t.Fatalf("day1 volumes: long=%v short=%v total=%v, want 200/50/250",
			d1.longVolume, d1.shortVolume, d1.volume)
	}
	if d1.fees != 1.5 {
		t.Fatalf("day1 fees: got %v, want 1.5", d1.fees)
	}

	d2 := byDate["20260610"]
	if d2.count != 1 || d2.longTrades != 1 || d2.shortTrades != 0 {
		t.Fatalf("day2 split: count=%d long=%d short=%d, want 1/1/0",
			d2.count, d2.longTrades, d2.shortTrades)
	}
}

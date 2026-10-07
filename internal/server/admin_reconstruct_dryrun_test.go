package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func TestDryRunRows_CarriesPerMarketFreeMargin(t *testing.T) {
	days := []*repository.Snapshot{{
		Timestamp:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		TotalEquity: 1000,
		Breakdown: &repository.MarketBreakdown{
			Spot: &repository.MarketMetrics{Equity: 100, AvailableMargin: 100},
			Swap: &repository.MarketMetrics{Equity: 900, AvailableMargin: 600},
		},
	}}

	raw, err := json.Marshal(dryRunRows(days))
	if err != nil {
		t.Fatalf("marshal rows: %v", err)
	}
	var got []struct {
		Day       string `json:"day"`
		Breakdown struct {
			Swap struct {
				Equity          float64 `json:"equity"`
				AvailableMargin float64 `json:"available_margin"`
			} `json:"swap"`
		} `json:"breakdown_by_market"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal rows: %v", err)
	}
	if len(got) != 1 || got[0].Day != "2026-01-02" {
		t.Fatalf("rows: got %+v", got)
	}
	if sw := got[0].Breakdown.Swap; sw.Equity != 900 || sw.AvailableMargin != 600 {
		t.Fatalf("swap bucket: got %+v, want equity 900 available 600", sw)
	}
}

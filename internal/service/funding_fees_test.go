package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

// Connectors sign funding negative when charged; the stored breakdown carries
// it as a cost, like trading_fees, and on global too, which is the only entry
// the dashboard reads its fees from.
func TestApplyFundingFeesStoresACostOnGlobal(t *testing.T) {
	svc := &SyncService{}
	now := time.Now()
	agg := svc.aggregateTrades([]*connector.Trade{
		{ID: "1", Price: 10, Quantity: 1, Fee: 0.5, Side: "buy", MarketType: connector.MarketSwap, Timestamp: now},
	})
	charges := applyFundingFees(agg, "hyperliquid", []*connector.FundingFee{
		{Amount: -3, Symbol: "BTC", Timestamp: now},
		{Amount: 1, Symbol: "ETH", Timestamp: now},
	})
	if charges != -2 {
		t.Fatalf("charges = %v, want -2 as reported", charges)
	}

	bd := agg.toRepo(1000, 0, 1)
	if bd.Swap == nil || bd.Swap.FundingFees != 2 {
		t.Fatalf("swap funding = %+v, want 2 (paid, as a cost)", bd.Swap)
	}
	if bd.Global == nil || bd.Global.FundingFees != 2 || bd.Global.TradingFees != 0.5 {
		t.Fatalf("global = %+v, want funding 2 and trading 0.5", bd.Global)
	}
}

func TestApplyFundingFeesLeavesNoNegativeZero(t *testing.T) {
	agg := (&SyncService{}).aggregateTrades(nil)
	applyFundingFees(agg, "bybit", nil)
	if f := agg.swap.fundingFees; f != 0 || 1/f < 0 {
		t.Fatalf("funding = %v, want +0", f)
	}
}

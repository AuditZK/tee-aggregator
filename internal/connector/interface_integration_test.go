package connector

import (
	"testing"
)

// TestAllConnectorsImplementInterface verifies every connector type
// satisfies the Connector interface at compile time.
func TestAllConnectorsImplementInterface(t *testing.T) {
	var _ Connector = (*MEXC)(nil)
	var _ Connector = (*Binance)(nil)
	var _ Connector = (*Bybit)(nil)
	var _ Connector = (*OKX)(nil)
	var _ Connector = (*Kraken)(nil)
	var _ Connector = (*Deribit)(nil)
	var _ Connector = (*IBKR)(nil)
	var _ Connector = (*Alpaca)(nil)
	var _ Connector = (*TradeStation)(nil)
	var _ Connector = (*Hyperliquid)(nil)
	var _ Connector = (*Lighter)(nil)
	var _ Connector = (*CTrader)(nil)
	var _ Connector = (*MetaTrader)(nil)
	var _ Connector = (*MockConnector)(nil)
	var _ Connector = (*Bitget)(nil)
	var _ Connector = (*KuCoin)(nil)
	var _ Connector = (*Coinbase)(nil)
	var _ Connector = (*Gate)(nil)
	var _ Connector = (*BingX)(nil)
	var _ Connector = (*Huobi)(nil)
}

// TestOptionalInterfaces verifies which connectors implement optional interfaces
func TestOptionalInterfaces(t *testing.T) {
	tests := []struct {
		name     string
		conn     Connector
		cashflow bool
		balMkt   bool
		funding  bool
		earn     bool
		mktDet   bool
		paper    bool
	}{
		{"Deribit", NewDeribit(&Credentials{APIKey: "k", APISecret: "s"}), true, true, true, false, false, true},
		{"IBKR", NewIBKR(&Credentials{APIKey: "t", APISecret: "q"}), true, true, false, false, false, true},
		{"Alpaca", NewAlpaca(&Credentials{APIKey: "k", APISecret: "s"}), true, false, false, false, false, true},
		{"Hyperliquid", NewHyperliquid(&Credentials{WalletAddress: "0x1"}), true, true, true, false, false, true},
		{"Lighter", NewLighter(&Credentials{WalletAddress: "0x1"}), false, true, false, false, false, true},
		{"MEXC", NewMEXC(&Credentials{APIKey: "k", APISecret: "s"}), true, true, true, false, false, false},
		{"Mock", NewMock(), false, false, false, false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, hasCF := tt.conn.(CashflowFetcher)
			if hasCF != tt.cashflow {
				t.Errorf("CashflowFetcher: got %v, want %v", hasCF, tt.cashflow)
			}

			_, hasBM := tt.conn.(BalanceByMarketFetcher)
			if hasBM != tt.balMkt {
				t.Errorf("BalanceByMarketFetcher: got %v, want %v", hasBM, tt.balMkt)
			}

			_, hasFF := tt.conn.(FundingFeesFetcher)
			if hasFF != tt.funding {
				t.Errorf("FundingFeesFetcher: got %v, want %v", hasFF, tt.funding)
			}

			_, hasEB := tt.conn.(EarnBalanceFetcher)
			if hasEB != tt.earn {
				t.Errorf("EarnBalanceFetcher: got %v, want %v", hasEB, tt.earn)
			}

			_, hasMD := tt.conn.(MarketTypeDetector)
			if hasMD != tt.mktDet {
				t.Errorf("MarketTypeDetector: got %v, want %v", hasMD, tt.mktDet)
			}

			_, hasPD := tt.conn.(PaperAccountDetector)
			if hasPD != tt.paper {
				t.Errorf("PaperAccountDetector: got %v, want %v", hasPD, tt.paper)
			}
		})
	}
}

// TestMarketTypeConstants verifies all market type constants are defined
func TestAllMarketTypeConstants(t *testing.T) {
	types := []string{
		MarketSpot, MarketStocks, MarketSwap, MarketFutures, MarketOptions,
		MarketMargin, MarketEarn, MarketCFD, MarketForex, MarketCommodities,
	}

	seen := make(map[string]bool)
	for _, mt := range types {
		if mt == "" {
			t.Error("empty market type constant")
		}
		if seen[mt] {
			t.Errorf("duplicate market type: %s", mt)
		}
		seen[mt] = true
	}

	if len(types) != 10 {
		t.Fatalf("expected 10 market types, got %d", len(types))
	}
}

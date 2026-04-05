package connector

import (
	"testing"
)

// TestAllConnectorsImplementInterface verifies every connector type
// satisfies the Connector interface at compile time.
func TestAllConnectorsImplementInterface(t *testing.T) {
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
	var _ Connector = (*CCXTConnector)(nil)
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

// TestCCXTOptionalInterfaces verifies CCXT connector implements all optional interfaces
func TestCCXTOptionalInterfaces(t *testing.T) {
	conn, err := NewCCXT("mexc", &Credentials{Exchange: "mexc", APIKey: "k", APISecret: "s"})
	if err != nil {
		t.Fatalf("NewCCXT failed: %v", err)
	}

	if _, ok := conn.(CashflowFetcher); !ok {
		t.Error("CCXT should implement CashflowFetcher")
	}
	if _, ok := conn.(BalanceByMarketFetcher); !ok {
		t.Error("CCXT should implement BalanceByMarketFetcher")
	}
	if _, ok := conn.(FundingFeesFetcher); !ok {
		t.Error("CCXT should implement FundingFeesFetcher")
	}
	if _, ok := conn.(EarnBalanceFetcher); !ok {
		t.Error("CCXT should implement EarnBalanceFetcher")
	}
	if _, ok := conn.(MarketTypeDetector); !ok {
		t.Error("CCXT should implement MarketTypeDetector")
	}
	if _, ok := conn.(PerMarketTradeFetcher); !ok {
		t.Error("CCXT should implement PerMarketTradeFetcher")
	}
	if _, ok := conn.(PaperAccountDetector); !ok {
		t.Error("CCXT should implement PaperAccountDetector")
	}
	if _, ok := conn.(KYCLevelFetcher); !ok {
		t.Error("CCXT should implement KYCLevelFetcher")
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

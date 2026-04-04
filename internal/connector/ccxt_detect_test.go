package connector

import "testing"

func TestDefaultMarketTypes_Binance(t *testing.T) {
	c := &CCXTConnector{exchange: "binance"}
	types := c.defaultMarketTypes()
	if len(types) != 3 {
		t.Fatalf("binance should have 3 market types, got %d", len(types))
	}
}

func TestDefaultMarketTypes_Unknown(t *testing.T) {
	c := &CCXTConnector{exchange: "unknownexchange"}
	types := c.defaultMarketTypes()
	if len(types) != 1 || types[0] != MarketSpot {
		t.Fatalf("unknown exchange should default to [spot], got %v", types)
	}
}

func TestDefaultMarketTypes_Bybit(t *testing.T) {
	c := &CCXTConnector{exchange: "bybit"}
	types := c.defaultMarketTypes()
	if len(types) != 2 {
		t.Fatalf("bybit should have 2 market types, got %d", len(types))
	}
}

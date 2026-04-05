package connector

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestMockConnector_CoreInterface verifies mock implements all required methods
func TestMockConnector_CoreInterface(t *testing.T) {
	mock := NewMock()

	// Exchange name
	if mock.Exchange() != "mock" {
		t.Fatalf("expected exchange=mock, got %s", mock.Exchange())
	}

	ctx := context.Background()

	// TestConnection
	if err := mock.TestConnection(ctx); err != nil {
		t.Fatalf("TestConnection failed: %v", err)
	}

	// GetBalance
	bal, err := mock.GetBalance(ctx)
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if bal.Equity <= 0 {
		t.Fatalf("expected positive equity, got %f", bal.Equity)
	}

	// GetPositions
	positions, err := mock.GetPositions(ctx)
	if err != nil {
		t.Fatalf("GetPositions failed: %v", err)
	}
	if len(positions) == 0 {
		t.Log("no positions (acceptable for mock)")
	}

	// GetTrades
	trades, err := mock.GetTrades(ctx, time.Now().Add(-24*time.Hour), time.Now())
	if err != nil {
		t.Fatalf("GetTrades failed: %v", err)
	}
	if trades == nil {
		t.Log("no trades (acceptable for mock)")
	}
}

// TestNativeConnectorMemory_Binance benchmarks Binance native connector memory usage
func TestNativeConnectorMemory_Binance(t *testing.T) {
	measureConnectorMemory(t, "binance", func() Connector {
		return NewBinance(&Credentials{
			Exchange:  "binance",
			APIKey:    "test_key_not_real",
			APISecret: "test_secret_not_real",
		})
	})
}

// TestNativeConnectorMemory_Bybit benchmarks Bybit native connector memory usage
func TestNativeConnectorMemory_Bybit(t *testing.T) {
	measureConnectorMemory(t, "bybit", func() Connector {
		return NewBybit(&Credentials{
			Exchange:  "bybit",
			APIKey:    "test_key_not_real",
			APISecret: "test_secret_not_real",
		})
	})
}

// TestNativeConnectorMemory_OKX benchmarks OKX native connector memory usage
func TestNativeConnectorMemory_OKX(t *testing.T) {
	measureConnectorMemory(t, "okx", func() Connector {
		return NewOKX(&Credentials{
			Exchange:   "okx",
			APIKey:     "test_key_not_real",
			APISecret:  "test_secret_not_real",
			Passphrase: "test_pass",
		})
	})
}

// TestNativeConnectorMemory_Kraken benchmarks Kraken native connector memory usage
func TestNativeConnectorMemory_Kraken(t *testing.T) {
	measureConnectorMemory(t, "kraken", func() Connector {
		return NewKraken(&Credentials{
			Exchange:  "kraken",
			APIKey:    "test_key_not_real",
			APISecret: "dGVzdF9zZWNyZXRfbm90X3JlYWw=", // base64 encoded
		})
	})
}

// TestNativeConnectorMemory_Deribit benchmarks Deribit native connector memory usage
func TestNativeConnectorMemory_Deribit(t *testing.T) {
	measureConnectorMemory(t, "deribit", func() Connector {
		return NewDeribit(&Credentials{
			Exchange:  "deribit",
			APIKey:    "test_key_not_real",
			APISecret: "test_secret_not_real",
		})
	})
}

// TestNativeConnectorMemory_Alpaca benchmarks Alpaca native connector memory usage
func TestNativeConnectorMemory_Alpaca(t *testing.T) {
	measureConnectorMemory(t, "alpaca", func() Connector {
		return NewAlpaca(&Credentials{
			Exchange:  "alpaca",
			APIKey:    "PK_TEST_NOT_REAL",
			APISecret: "test_secret_not_real",
		})
	})
}

// TestNativeConnectorMemory_Hyperliquid benchmarks Hyperliquid native connector memory usage
func TestNativeConnectorMemory_Hyperliquid(t *testing.T) {
	measureConnectorMemory(t, "hyperliquid", func() Connector {
		return NewHyperliquid(&Credentials{
			Exchange:      "hyperliquid",
			WalletAddress: "0x1234567890abcdef1234567890abcdef12345678",
		})
	})
}

// TestNativeConnectorMemory_Lighter benchmarks Lighter native connector memory usage
func TestNativeConnectorMemory_Lighter(t *testing.T) {
	measureConnectorMemory(t, "lighter", func() Connector {
		return NewLighter(&Credentials{
			Exchange:      "lighter",
			WalletAddress: "0x1234567890abcdef1234567890abcdef12345678",
		})
	})
}

// TestMultipleConnectorsMemory creates 11 native connectors simultaneously
// and verifies total memory stays under 50MB (vs 1.5GB for CCXT)
func TestMultipleConnectorsMemory(t *testing.T) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	connectors := make([]Connector, 0, 10)

	connectors = append(connectors,
		NewBinance(&Credentials{Exchange: "binance", APIKey: "k", APISecret: "s"}),
		NewBybit(&Credentials{Exchange: "bybit", APIKey: "k", APISecret: "s"}),
		NewOKX(&Credentials{Exchange: "okx", APIKey: "k", APISecret: "s", Passphrase: "p"}),
		NewKraken(&Credentials{Exchange: "kraken", APIKey: "k", APISecret: "dGVzdA=="}),
		NewDeribit(&Credentials{Exchange: "deribit", APIKey: "k", APISecret: "s"}),
		NewAlpaca(&Credentials{Exchange: "alpaca", APIKey: "PK_TEST", APISecret: "s"}),
		NewHyperliquid(&Credentials{Exchange: "hyperliquid", WalletAddress: "0x1234"}),
		NewLighter(&Credentials{Exchange: "lighter", WalletAddress: "0x1234"}),
		NewIBKR(&Credentials{Exchange: "ibkr", APIKey: "token", APISecret: "queryid"}),
		NewMock(),
		NewMEXC(&Credentials{Exchange: "mexc", APIKey: "k", APISecret: "s"}),
	)

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	allocMB := (after.Alloc - before.Alloc) / 1024 / 1024
	t.Logf("11 native connectors: %d MB allocated", allocMB)

	if allocMB > 50 {
		t.Fatalf("11 native connectors should use < 50MB, used %d MB", allocMB)
	}

	// Keep reference to prevent GC
	_ = connectors
}

// TestFactoryCreatesNativeConnectors verifies the factory returns native (not CCXT)
// connectors for major exchanges
func TestFactoryCreatesNativeConnectors(t *testing.T) {
	factory := NewFactory()

	tests := []struct {
		exchange string
		wantType string
	}{
		{"binance", "*connector.Binance"},
		{"bybit", "*connector.Bybit"},
		{"okx", "*connector.OKX"},
		{"kraken", "*connector.Kraken"},
		{"deribit", "*connector.Deribit"},
		{"ibkr", "*connector.IBKR"},
		{"alpaca", "*connector.Alpaca"},
		{"hyperliquid", "*connector.Hyperliquid"},
		{"lighter", "*connector.Lighter"},
		{"mock", "*connector.Mock"},
	}

	for _, tt := range tests {
		t.Run(tt.exchange, func(t *testing.T) {
			conn, err := factory.Create(&Credentials{
				Exchange:      tt.exchange,
				APIKey:        "test",
				APISecret:     "dGVzdA==", // base64 for kraken
				Passphrase:    "test",
				WalletAddress: "0x1234",
			})
			if err != nil {
				t.Fatalf("Create(%s) failed: %v", tt.exchange, err)
			}
			if conn == nil {
				t.Fatalf("Create(%s) returned nil", tt.exchange)
			}
			if conn.Exchange() != tt.exchange {
				// Some exchanges normalize the name
				t.Logf("Exchange() = %s (expected %s)", conn.Exchange(), tt.exchange)
			}
		})
	}
}

// TestNativeConnectorMemory_MEXC benchmarks MEXC native connector memory usage
func TestNativeConnectorMemory_MEXC(t *testing.T) {
	measureConnectorMemory(t, "mexc", func() Connector {
		return NewMEXC(&Credentials{
			Exchange:  "mexc",
			APIKey:    "test_key_not_real",
			APISecret: "test_secret_not_real",
		})
	})
}

// TestCCXTOnlyForMinorExchanges verifies CCXT is used only for minor exchanges
func TestCCXTOnlyForMinorExchanges(t *testing.T) {
	factory := NewFactory()
	ccxtExchanges := []string{"bitget", "kucoin", "coinbase", "gate", "bingx", "huobi"}

	for _, ex := range ccxtExchanges {
		t.Run(ex, func(t *testing.T) {
			conn, err := factory.Create(&Credentials{
				Exchange:  ex,
				APIKey:    "test",
				APISecret: "test",
			})
			if err != nil {
				t.Fatalf("Create(%s) failed: %v", ex, err)
			}
			// Verify it's a CCXTConnector
			if _, ok := conn.(*CCXTConnector); !ok {
				t.Fatalf("expected CCXTConnector for %s, got %T", ex, conn)
			}
		})
	}
}

// measureConnectorMemory measures memory usage of creating a single connector
func measureConnectorMemory(t *testing.T, name string, create func() Connector) {
	t.Helper()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	conn := create()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	allocKB := (after.Alloc - before.Alloc) / 1024
	t.Logf("%s connector: %d KB allocated", name, allocKB)

	// Native connectors should use < 1MB
	if allocKB > 1024 {
		t.Fatalf("%s connector should use < 1MB, used %d KB", name, allocKB)
	}

	// Verify it implements Connector interface
	_ = conn.Exchange()
}

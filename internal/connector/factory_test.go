package connector

import (
	"errors"
	"strings"
	"testing"
)

func TestFactoryCreateSupportsDeribitAndMetaTrader(t *testing.T) {
	f := NewFactory()

	tests := []struct {
		name     string
		exchange string
		wantConn string
	}{
		{name: "deribit", exchange: "deribit", wantConn: "deribit"},
		{name: "mt4", exchange: "mt4", wantConn: "mt4"},
		{name: "mt5", exchange: "mt5", wantConn: "mt5"},
		{name: "binance_futures", exchange: "binance_futures", wantConn: "binance"},
		{name: "binanceusdm", exchange: "binanceusdm", wantConn: "binance"},
		{name: "bitget", exchange: "bitget", wantConn: "bitget"},
		{name: "mexc", exchange: "mexc", wantConn: "mexc"},
		{name: "kucoin", exchange: "kucoin", wantConn: "kucoin"},
		{name: "coinbase", exchange: "coinbase", wantConn: "coinbase"},
		{name: "gate", exchange: "gate", wantConn: "gate"},
		{name: "bingx", exchange: "bingx", wantConn: "bingx"},
		{name: "huobi", exchange: "huobi", wantConn: "huobi"},
		{name: "ig", exchange: "ig", wantConn: "ig"},
		{name: "ig_demo", exchange: "ig_demo", wantConn: "ig_demo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, err := f.Create(&Credentials{
				Exchange:   tt.exchange,
				APIKey:     "123456",
				APISecret:  "secret",
				Passphrase: "broker:443",
			})
			if err != nil {
				t.Fatalf("Create returned error: %v", err)
			}
			if conn == nil {
				t.Fatal("Create returned nil connector")
			}
			if got := conn.Exchange(); got != tt.wantConn {
				t.Fatalf("Exchange() = %q, want %q", got, tt.wantConn)
			}
		})
	}
}

func TestFactorySupportedExchangesIncludesDeribitAndMetaTrader(t *testing.T) {
	f := NewFactory()
	got := make(map[string]struct{})
	for _, ex := range f.SupportedExchanges() {
		got[ex] = struct{}{}
	}

	for _, ex := range []string{"deribit", "mt4", "mt5", "binance_futures", "binanceusdm"} {
		if _, ok := got[ex]; !ok {
			t.Fatalf("supported exchanges missing %q", ex)
		}
	}

	for _, ex := range []string{"bitget", "mexc", "kucoin", "coinbase", "gate", "bingx", "huobi"} {
		if _, ok := got[ex]; !ok {
			t.Fatalf("supported exchanges missing %q", ex)
		}
	}

	for _, ex := range []string{"ig", "ig_demo"} {
		if _, ok := got[ex]; !ok {
			t.Fatalf("supported exchanges missing %q", ex)
		}
	}
}

// CONN-01: the mock connector fabricates balances/trades and must never be
// reachable through the production factory.
func TestFactoryMockRejectedInProduction(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("NODE_ENV", "")
	f := NewFactory()

	if _, err := f.Create(&Credentials{Exchange: "mock"}); err == nil {
		t.Fatal("Create(mock) must fail when ENV=production")
	} else if !errors.Is(err, ErrUnsupportedExchange) {
		t.Fatalf("expected ErrUnsupportedExchange, got %v", err)
	}

	for _, ex := range f.SupportedExchanges() {
		if ex == "mock" {
			t.Fatal("SupportedExchanges() must not list mock when ENV=production")
		}
	}
}

// CONN-01: the mock connector stays available outside production for the dev
// harness and integration tests.
func TestFactoryMockAvailableOutsideProduction(t *testing.T) {
	t.Setenv("ENV", "development")
	t.Setenv("NODE_ENV", "")
	f := NewFactory()

	conn, err := f.Create(&Credentials{Exchange: "mock"})
	if err != nil {
		t.Fatalf("Create(mock) should succeed outside production: %v", err)
	}
	if conn == nil {
		t.Fatal("Create(mock) returned nil connector")
	}

	var listed bool
	for _, ex := range f.SupportedExchanges() {
		if ex == "mock" {
			listed = true
		}
	}
	if !listed {
		t.Fatal("SupportedExchanges() should list mock outside production")
	}
}

func TestFactoryCreateUnsupportedExchangeIncludesSupportedList(t *testing.T) {
	f := NewFactory()
	_, err := f.Create(&Credentials{Exchange: "unknown_ex"})
	if err == nil {
		t.Fatal("expected error for unsupported exchange")
	}
	if !errors.Is(err, ErrUnsupportedExchange) {
		t.Fatalf("expected ErrUnsupportedExchange, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "Supported:") {
		t.Fatalf("expected supported list in error message, got %q", msg)
	}
	if !strings.Contains(msg, "binance") {
		t.Fatalf("expected at least one supported exchange in error message, got %q", msg)
	}
}

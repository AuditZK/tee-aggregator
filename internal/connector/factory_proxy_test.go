package connector

import (
	"net/http"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/proxy"
)

// TestNewFactoryWithProxy_BinanceGetsProxyTransport verifies the factory wires
// the proxy-configured client into the Binance connector when Binance is in
// the proxied set — the path used in EU datacenters where Binance geo-blocks.
func TestNewFactoryWithProxy_BinanceGetsProxyTransport(t *testing.T) {
	cfg := proxy.ParseConfig("http://proxy.example:8080", "binance")

	conn, err := NewFactoryWithProxy(cfg).Create(&Credentials{
		Exchange:  "binance",
		APIKey:    "k",
		APISecret: "s",
	})
	if err != nil {
		t.Fatalf("create binance: %v", err)
	}

	b, ok := conn.(*Binance)
	if !ok {
		t.Fatalf("connector is %T, want *Binance", conn)
	}
	if b.client.Transport == nil {
		t.Fatal("binance built via proxy factory has no transport — proxy not wired")
	}
	if _, ok := b.client.Transport.(*http.Transport); !ok {
		t.Fatalf("transport is %T, want *http.Transport", b.client.Transport)
	}
	// proxy.NewClient yields Timeout==0; NewBinanceWithClient must patch it to
	// 30s, or a proxied sync could hang forever (audit C-03).
	if b.client.Timeout != 30*time.Second {
		t.Errorf("client timeout = %v, want 30s", b.client.Timeout)
	}
}

// TestNewFactoryWithProxy_BinanceWithoutProxyURLIsDirect verifies the factory
// falls back to the direct client when no proxy URL is configured — a proxy
// factory must not force a (nil) proxy onto every connector.
func TestNewFactoryWithProxy_BinanceWithoutProxyURLIsDirect(t *testing.T) {
	cfg := proxy.ParseConfig("", "binance")

	conn, err := NewFactoryWithProxy(cfg).Create(&Credentials{
		Exchange:  "binance",
		APIKey:    "k",
		APISecret: "s",
	})
	if err != nil {
		t.Fatalf("create binance: %v", err)
	}

	b, ok := conn.(*Binance)
	if !ok {
		t.Fatalf("connector is %T, want *Binance", conn)
	}
	if b.client.Transport != nil {
		t.Errorf("transport = %v, want nil when no proxy URL is configured", b.client.Transport)
	}
}

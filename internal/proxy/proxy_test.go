package proxy

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	t.Run("empty input: no proxy URL, binance default", func(t *testing.T) {
		cfg := ParseConfig("", "")
		if cfg.ProxyURL != nil {
			t.Errorf("ProxyURL = %v, want nil", cfg.ProxyURL)
		}
		if !cfg.Exchanges["binance"] {
			t.Error("empty exchange list must default to binance")
		}
	})

	t.Run("valid proxy URL + explicit exchange list", func(t *testing.T) {
		cfg := ParseConfig("http://proxy.example:8080", "binance,kraken")
		if cfg.ProxyURL == nil || cfg.ProxyURL.Host != "proxy.example:8080" {
			t.Fatalf("ProxyURL = %v, want host proxy.example:8080", cfg.ProxyURL)
		}
		if !cfg.Exchanges["binance"] || !cfg.Exchanges["kraken"] {
			t.Errorf("exchanges = %v, want binance+kraken", cfg.Exchanges)
		}
	})

	t.Run("exchange list trimmed, lowercased, empty parts dropped", func(t *testing.T) {
		cfg := ParseConfig("http://p:1/", "  Binance , KRAKEN ,, ")
		if !cfg.Exchanges["binance"] || !cfg.Exchanges["kraken"] {
			t.Errorf("exchanges = %v, want normalized binance+kraken", cfg.Exchanges)
		}
		if len(cfg.Exchanges) != 2 {
			t.Errorf("exchanges = %v, want exactly 2 entries", cfg.Exchanges)
		}
	})

	t.Run("malformed proxy URL leaves ProxyURL nil", func(t *testing.T) {
		cfg := ParseConfig("http://%zz", "binance")
		if cfg.ProxyURL != nil {
			t.Errorf("ProxyURL = %v, want nil for malformed input", cfg.ProxyURL)
		}
	})
}

func TestShouldProxy(t *testing.T) {
	proxyURL, _ := url.Parse("http://proxy.example:8080")

	t.Run("nil config never proxies", func(t *testing.T) {
		var c *Config
		if c.ShouldProxy("binance") {
			t.Error("nil config must not proxy")
		}
	})

	t.Run("config without proxy URL never proxies", func(t *testing.T) {
		c := &Config{Exchanges: map[string]bool{"binance": true}}
		if c.ShouldProxy("binance") {
			t.Error("config with no ProxyURL must not proxy")
		}
	})

	t.Run("listed exchange proxies, case-insensitive", func(t *testing.T) {
		c := &Config{ProxyURL: proxyURL, Exchanges: map[string]bool{"binance": true}}
		if !c.ShouldProxy("binance") || !c.ShouldProxy("BINANCE") {
			t.Error("listed exchange must proxy regardless of case")
		}
	})

	t.Run("unlisted exchange does not proxy", func(t *testing.T) {
		c := &Config{ProxyURL: proxyURL, Exchanges: map[string]bool{"binance": true}}
		if c.ShouldProxy("kraken") {
			t.Error("unlisted exchange must not proxy")
		}
	})
}

func TestConfigureTransport(t *testing.T) {
	t.Run("unproxied exchange yields nil transport", func(t *testing.T) {
		cfg := ParseConfig("http://proxy.example:8080", "binance")
		if tr := cfg.ConfigureTransport("kraken"); tr != nil {
			t.Errorf("ConfigureTransport(kraken) = %v, want nil", tr)
		}
	})

	t.Run("proxied exchange routes to the configured proxy URL", func(t *testing.T) {
		cfg := ParseConfig("http://proxy.example:8080", "binance")
		tr := cfg.ConfigureTransport("binance")
		if tr == nil || tr.Proxy == nil {
			t.Fatal("ConfigureTransport(binance) must return a transport with Proxy set")
		}
		got, err := tr.Proxy(httptest.NewRequest(http.MethodGet, "http://api.binance.com/x", nil))
		if err != nil {
			t.Fatalf("Proxy func: %v", err)
		}
		if got == nil || got.Host != "proxy.example:8080" {
			t.Errorf("Proxy func returned %v, want host proxy.example:8080", got)
		}
		if tr.ProxyConnectHeader != nil {
			t.Errorf("ProxyConnectHeader = %v, want nil when proxy URL carries no userinfo", tr.ProxyConnectHeader)
		}
	})

	t.Run("authenticated proxy presets Proxy-Authorization for CONNECT", func(t *testing.T) {
		cfg := ParseConfig("http://puser:ppass@proxy.example:8080", "binance")
		tr := cfg.ConfigureTransport("binance")
		if tr == nil || tr.ProxyConnectHeader == nil {
			t.Fatal("authenticated proxy must set ProxyConnectHeader")
		}
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("puser:ppass"))
		if got := tr.ProxyConnectHeader.Get("Proxy-Authorization"); got != want {
			t.Errorf("Proxy-Authorization = %q, want %q", got, want)
		}
	})
}

func TestNewClient(t *testing.T) {
	t.Run("unproxied exchange gets a plain client", func(t *testing.T) {
		cfg := ParseConfig("http://proxy.example:8080", "binance")
		if c := cfg.NewClient("kraken"); c.Transport != nil {
			t.Errorf("Transport = %v, want nil for unproxied exchange", c.Transport)
		}
	})

	t.Run("proxied exchange gets a proxy transport", func(t *testing.T) {
		cfg := ParseConfig("http://proxy.example:8080", "binance")
		if c := cfg.NewClient("binance"); c.Transport == nil {
			t.Fatal("proxied client must carry a transport")
		}
	})

	t.Run("NewClient leaves Timeout unset (audit C-03)", func(t *testing.T) {
		// proxy.NewClient returns an http.Client with no Timeout. Today that is
		// masked only because NewBinanceWithClient patches Timeout==0 -> 30s.
		// Pinned here as a tripwire: any future connector that consumes a
		// proxied client WITHOUT patching the timeout would hang indefinitely.
		cfg := ParseConfig("http://proxy.example:8080", "binance")
		if to := cfg.NewClient("binance").Timeout; to != 0 {
			t.Errorf("Timeout = %v; finding C-03 said NewClient sets none — update the audit if this was fixed", to)
		}
	})
}

// TestProxyRoutesHTTPRequestThroughConfiguredProxy is the end-to-end check:
// a client built by NewClient must send its requests to the configured proxy,
// not to the target host directly. The target uses the reserved .invalid TLD
// so it can never resolve — reaching it at all would fail the request.
func TestProxyRoutesHTTPRequestThroughConfiguredProxy(t *testing.T) {
	// The fake proxy echoes what it observed back through response headers, so
	// the test reads everything from resp (synchronised with client.Do) and
	// shares no mutable state with the handler goroutine — race-clean.
	fakeProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Fake-Proxy", "1")
		w.Header().Set("X-Seen-Host", r.Host)
		w.Header().Set("X-Seen-Proxy-Auth", r.Header.Get("Proxy-Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer fakeProxy.Close()

	// Userinfo on the proxy URL exercises the auth path too: for a plain-HTTP
	// target the stdlib derives Proxy-Authorization from the proxy userinfo
	// automatically (ProxyConnectHeader is the CONNECT-only counterpart).
	proxyURL := "http://puser:ppass@" + strings.TrimPrefix(fakeProxy.URL, "http://")
	cfg := ParseConfig(proxyURL, "binance")

	client := cfg.NewClient("binance")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://api.binance.test.invalid/api/v3/ping", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request through proxy failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.Header.Get("X-Fake-Proxy") != "1" {
		t.Fatal("request did not reach the configured proxy")
	}
	if got := resp.Header.Get("X-Seen-Host"); got != "api.binance.test.invalid" {
		t.Errorf("proxy saw Host %q, want api.binance.test.invalid", got)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("puser:ppass"))
	if got := resp.Header.Get("X-Seen-Proxy-Auth"); got != wantAuth {
		t.Errorf("proxy saw Proxy-Authorization %q, want %q", got, wantAuth)
	}
}

package proxy

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// clientTimeout bounds a proxied exchange request end-to-end so a slow or
// silent proxy cannot hang a sync goroutine indefinitely. Matches the 30 s
// timeout the non-proxied connector HTTP clients use.
const clientTimeout = 30 * time.Second

// Config holds HTTP proxy configuration for exchange connectors.
type Config struct {
	ProxyURL  *url.URL
	Exchanges map[string]bool
}

// ParseConfig parses proxy configuration from environment variables.
func ParseConfig(proxyURL, exchanges string) *Config {
	cfg := &Config{
		Exchanges: make(map[string]bool),
	}

	if proxyURL != "" {
		if u, err := url.Parse(proxyURL); err == nil {
			cfg.ProxyURL = u
		}
	}

	if exchanges == "" {
		exchanges = "binance"
	}
	for _, e := range strings.Split(exchanges, ",") {
		e = strings.TrimSpace(strings.ToLower(e))
		if e != "" {
			cfg.Exchanges[e] = true
		}
	}

	return cfg
}

// ShouldProxy returns true if the exchange should use the proxy.
func (c *Config) ShouldProxy(exchange string) bool {
	if c == nil || c.ProxyURL == nil {
		return false
	}
	return c.Exchanges[strings.ToLower(exchange)]
}

// ConfigureTransport returns an http.Transport with proxy configured, or nil if no proxy needed.
func (c *Config) ConfigureTransport(exchange string) *http.Transport {
	if !c.ShouldProxy(exchange) {
		return nil
	}
	t := &http.Transport{
		Proxy: http.ProxyURL(c.ProxyURL),
	}
	// For HTTPS targets Go uses a CONNECT tunnel. The standard library does
	// not automatically derive Proxy-Authorization from the proxy URL userinfo
	// for CONNECT requests — set it explicitly so authenticated proxies work.
	if c.ProxyURL.User != nil {
		pass, _ := c.ProxyURL.User.Password()
		creds := c.ProxyURL.User.Username() + ":" + pass
		t.ProxyConnectHeader = http.Header{
			"Proxy-Authorization": []string{"Basic " + base64.StdEncoding.EncodeToString([]byte(creds))},
		}
	}
	return t
}

// NewClient creates an http.Client with optional proxy for the given exchange.
// The client always carries a request timeout (clientTimeout) — a connector
// that swaps in this client must not lose the deadline the default clients have.
func (c *Config) NewClient(exchange string) *http.Client {
	transport := c.ConfigureTransport(exchange)
	if transport == nil {
		return &http.Client{Timeout: clientTimeout}
	}
	return &http.Client{Transport: transport, Timeout: clientTimeout}
}

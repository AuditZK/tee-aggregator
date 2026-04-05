// Package connector provides exchange-specific implementations.
//
// Architecture:
//   - CryptoBase: shared HTTP client + helpers for all crypto exchange connectors
//   - Native connectors (Binance, Bybit, OKX, Kraken, MEXC): direct HTTP with HMAC signing (~1MB each)
//   - CCXT connector: dynamic wrapper for minor exchanges (~67-150MB per LoadMarkets)
//   - Specialized connectors: IBKR (Flex XML), cTrader (WebSocket), MetaTrader (mt-bridge), etc.
//
// Use native connectors for major exchanges to minimize memory usage.
// Use CCXT only for exchanges without a native connector.
package connector

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// CryptoBase provides shared functionality for native crypto exchange connectors.
// All native crypto connectors embed this struct for HTTP requests and JSON parsing.
type CryptoBase struct {
	APIKey    string
	APISecret string
	Client    *http.Client
	BaseURL   string
}

// NewCryptoBase creates a base with standard timeout.
func NewCryptoBase(apiKey, apiSecret, baseURL string) CryptoBase {
	return CryptoBase{
		APIKey:    apiKey,
		APISecret: apiSecret,
		Client:    &http.Client{Timeout: 30 * time.Second},
		BaseURL:   baseURL,
	}
}

// DoRequest executes an HTTP request and returns the raw body.
func (b *CryptoBase) DoRequest(req *http.Request) ([]byte, error) {
	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}

// DoJSON executes an HTTP request and unmarshals the JSON response.
func (b *CryptoBase) DoJSON(req *http.Request, out interface{}) error {
	body, err := b.DoRequest(req)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

// GET is a convenience method for authenticated GET requests.
// Subclasses should add their own signing logic to the request before calling DoRequest.
func (b *CryptoBase) GET(url string) (*http.Request, error) {
	return http.NewRequest("GET", url, nil)
}

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
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/logredact"
)

// Retry policy for CONN-004: max 3 attempts with exponential backoff and
// Retry-After support for 429 / 5xx responses. DoRequest is single-shot;
// retryHTTP rebuilds (and therefore re-signs) the request on every attempt,
// so an HMAC-signed read survives a transient failure without replaying a
// stale, time-windowed signature.
const (
	maxRetryAttempts = 3
	baseBackoff      = 500 * time.Millisecond
	maxBackoff       = 5 * time.Second

	// errorBodyMaxLen caps how much of an HTTP error body is embedded in
	// a Go error (LOG-001). 512 B is enough for diagnostics (status,
	// vendor error code, short message) without giving an echoed signed
	// URL or OAuth form body room to spill entire credentials. The
	// logredact core is the primary defense; this is belt-and-suspenders.
	errorBodyMaxLen = 512

	// DefaultMaxResponseBytes caps how many bytes a connector reads from a
	// single HTTP response (CONN-AUDIT-001). 8 MiB is well above any
	// legitimate balance/trade page from the major exchanges and small
	// enough that an OOM-grade response from a hostile or compromised
	// upstream cannot exhaust the enclave's 512 MiB memory budget. Use
	// IBKRFlexMaxResponseBytes for Flex XML statements which can be
	// genuinely larger for long-history accounts.
	DefaultMaxResponseBytes = 8 << 20

	// IBKRFlexMaxResponseBytes is the per-response cap for IBKR Flex XML
	// statements, which can be larger than crypto exchange responses
	// because they bundle years of activity into a single download.
	IBKRFlexMaxResponseBytes = 32 << 20
)

// ErrResponseTooLarge is returned by ReadCappedBody when the upstream sent
// more than the requested cap. Callers can errors.Is this to distinguish
// a hostile/large response from a benign read error.
var ErrResponseTooLarge = errorString("connector response exceeds cap")

type errorString string

func (e errorString) Error() string { return string(e) }

// TruncatedBody returns the first errorBodyMaxLen bytes of body as a
// string, with a marker appended when truncation occurred. Callers
// embed the result in fmt.Errorf so failure messages stay useful while
// limiting the window an attacker-echoed response can leak into logs.
func TruncatedBody(body []byte) string {
	if len(body) <= errorBodyMaxLen {
		return string(body)
	}
	return string(body[:errorBodyMaxLen]) + "...[truncated]"
}

// vendorErrorDetail bounds and scrubs an upstream vendor error string before
// it is interpolated into a Go error (CONN-04). TruncatedBody caps the window
// a verbose or hostile upstream can occupy in logs/errors; ScrubMessage is
// defense-in-depth over the logredact core for the case where a vendor echoes
// a credential fragment back in its own error message.
func vendorErrorDetail(msg string) string {
	return logredact.ScrubMessage(TruncatedBody([]byte(msg)))
}

// ReadCappedBody reads up to max bytes from r and closes it. Returns
// (body, ErrResponseTooLarge) if the upstream sent more than max bytes —
// the partial body is still returned so callers can include vendor error
// info from the prefix. CONN-AUDIT-001: this is the centralised entry
// point that every connector must use instead of io.ReadAll(resp.Body),
// otherwise a hostile or compromised upstream can exhaust the enclave's
// memory budget.
func ReadCappedBody(r io.ReadCloser, max int64) ([]byte, error) {
	defer r.Close()
	// LimitReader returns EOF at exactly max bytes. We read one extra byte
	// to detect "would have been larger" without fully buffering.
	body, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return body, err
	}
	if int64(len(body)) > max {
		return body[:max], ErrResponseTooLarge
	}
	return body, nil
}

// isRetryableStatus reports true for HTTP status codes worth retrying.
// 429 Too Many Requests + 5xx transient failures; everything else is a
// permanent failure (auth, bad input, etc.) and retrying is pointless.
func isRetryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}
	return false
}

// parseRetryAfter extracts a retry delay from the Retry-After header, capped
// at maxBackoff to prevent an adversarial server from pinning us. Accepts
// either seconds (integer) or an HTTP-date.
func parseRetryAfter(h http.Header, fallback time.Duration) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return fallback
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		d := time.Duration(secs) * time.Second
		if d > maxBackoff {
			return maxBackoff
		}
		return d
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			return fallback
		}
		if d > maxBackoff {
			return maxBackoff
		}
		return d
	}
	return fallback
}

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

// DoRequest executes an HTTP request and returns the raw body (single-shot).
// Use retryHTTP for signed reads that should survive a transient 429 / 5xx
// (CONN-004) — it re-signs the request on each attempt.
func (b *CryptoBase) DoRequest(req *http.Request) ([]byte, error) {
	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, err
	}

	// CONN-AUDIT-001: bound the response so a hostile upstream cannot OOM the
	// enclave. ReadCappedBody closes resp.Body on return.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return body, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("HTTP %d: %s", resp.StatusCode, TruncatedBody(body))
	}

	return body, nil
}

// retryHTTP sends the request produced by buildReq and returns the response
// body. On a transient failure — a network error, HTTP 429, or HTTP 5xx — it
// calls buildReq AGAIN (rebuilding, and therefore re-signing, the request with
// a fresh timestamp) and retries, up to maxRetryAttempts, with exponential
// backoff that honours a Retry-After header. A non-transient status (4xx
// auth/argument errors) returns immediately; context cancellation aborts the
// loop. CONN-004 — taking a builder rather than a prebuilt *http.Request is
// precisely what makes retry safe for HMAC-signed requests whose signature is
// bound to a short receive window.
func retryHTTP(client *http.Client, buildReq func() (*http.Request, error)) ([]byte, error) {
	var lastBody []byte
	var lastErr error
	backoff := baseBackoff

	for attempt := 1; attempt <= maxRetryAttempts; attempt++ {
		req, err := buildReq()
		if err != nil {
			return nil, err
		}
		ctx := req.Context()

		resp, err := client.Do(req)
		if err != nil {
			// Network error — retry unless the context is already done.
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		} else {
			// CONN-AUDIT-001: bounded read; ReadCappedBody closes resp.Body.
			body, readErr := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
			if readErr != nil {
				lastErr = readErr
			} else {
				lastBody = body
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					return body, nil
				}
				lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, TruncatedBody(body))
				if !isRetryableStatus(resp.StatusCode) {
					// Permanent failure — do not retry.
					return body, lastErr
				}
				// Honour server-supplied Retry-After if any.
				backoff = parseRetryAfter(resp.Header, backoff)
			}
		}

		if attempt == maxRetryAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return lastBody, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return lastBody, lastErr
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

// signHMACHex returns the hex-encoded HMAC-SHA256 of msg under secret — the
// request-signing primitive shared by the Binance-family connectors (Binance,
// BingX, MEXC, Bybit, Coinbase). Each connector still builds its own signing
// payload; only the keyed-hash itself is centralised here.
func signHMACHex(secret, msg string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(msg))
	return hex.EncodeToString(mac.Sum(nil))
}

// signHMACBase64 returns the base64-encoded HMAC-SHA256 of msg under secret —
// the signing primitive shared by the OKX-family connectors (OKX, Bitget,
// KuCoin, Huobi).
func signHMACBase64(secret, msg string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(msg))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// signedQueryGET performs the Binance-family query-signed GET: it appends
// `timestamp=<unixMillis>` to params, HMAC-signs the resulting query string,
// appends `&signature=<hex>`, and sends the API key under apiKeyHeader. BingX
// and MEXC (spot) share this exact scheme bar the header name.
func (b *CryptoBase) signedQueryGET(ctx context.Context, apiKeyHeader, path, params string) ([]byte, error) {
	return retryHTTP(b.Client, func() (*http.Request, error) {
		query := params
		if query != "" {
			query += "&"
		}
		query += "timestamp=" + strconv.FormatInt(time.Now().UnixMilli(), 10)

		reqURL := b.BaseURL + path + "?" + query + "&signature=" + signHMACHex(b.APISecret, query)
		req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set(apiKeyHeader, b.APIKey)
		return req, nil
	})
}

// --- Spot asset valuation (CONN-VALUE-001) -------------------------------
//
// Several native connectors (Binance, BingX, Bitget, Gate, Coinbase, Huobi,
// KuCoin, Kraken) historically summed ONLY stablecoin spot balances and
// silently dropped every BTC/ETH/altcoin holding, so any account holding
// crypto rather than USDT reported an equity of ~0. MEXC is the reference that
// got it right: value every non-zero holding at its market price. These helpers
// centralise that valuation so each connector only has to supply its own price
// map.

// stablecoinsUSD lists the assets valued 1:1 with USD. Kept deliberately small
// and well-known — pricing an unknown "stable" through the ticker map is safer
// than mis-pegging it to exactly 1.
var stablecoinsUSD = map[string]struct{}{
	"USDT": {}, "USDC": {}, "USD": {}, "BUSD": {},
	"DAI": {}, "FDUSD": {}, "TUSD": {},
}

// IsStablecoinUSD reports whether asset is a USD-pegged stablecoin valued 1:1.
func IsStablecoinUSD(asset string) bool {
	_, ok := stablecoinsUSD[strings.ToUpper(strings.TrimSpace(asset))]
	return ok
}

// SpotHolding is one non-zero spot balance line: an asset and the total amount
// held (free + locked).
type SpotHolding struct {
	Asset  string
	Amount float64
}

// ValueSpotHoldingsUSD converts spot holdings to a total USD value. Stablecoins
// are valued 1:1; every other asset is priced from priceMap, a Binance-style
// symbol→price map (e.g. "BTCUSDT"→65000). Lookup order per asset:
//
//	1. <ASSET>USDT   2. <ASSET>USDC   3. bridge via <ASSET>BTC × BTCUSDT
//
// Assets with no resolvable price (dust, delisted, exotic) contribute 0 rather
// than failing the whole balance — pricing must never crash a sync. Callers
// pass a single wallet's holdings, so nothing is double-counted here.
func ValueSpotHoldingsUSD(holdings []SpotHolding, priceMap map[string]float64) float64 {
	btcUSD := priceMap["BTCUSDT"]
	var total float64
	for _, h := range holdings {
		if h.Amount <= 0 {
			continue
		}
		asset := strings.ToUpper(strings.TrimSpace(h.Asset))
		if IsStablecoinUSD(asset) {
			total += h.Amount
			continue
		}
		if p := priceMap[asset+"USDT"]; p > 0 {
			total += h.Amount * p
			continue
		}
		if p := priceMap[asset+"USDC"]; p > 0 {
			total += h.Amount * p
			continue
		}
		if btcUSD > 0 {
			if p := priceMap[asset+"BTC"]; p > 0 {
				total += h.Amount * p * btcUSD
			}
		}
	}
	return total
}

// FetchBinanceStylePriceMap fetches every symbol price from a Binance-compatible
// public spot ticker: GET {baseURL}/api/v3/ticker/price with no symbol returns
// [{"symbol","price"}] for all pairs. Binance and MEXC share this exact path.
// The call is public (unsigned) and costs one request regardless of asset count
// (weight 4 on Binance). client is threaded through so a region-proxied
// connector reuses its configured transport.
func FetchBinanceStylePriceMap(ctx context.Context, client *http.Client, baseURL string) (map[string]float64, error) {
	body, err := retryHTTP(client, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "GET", baseURL+"/api/v3/ticker/price", nil)
	})
	if err != nil {
		return nil, err
	}

	var tickers []struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}
	if err := json.Unmarshal(body, &tickers); err != nil {
		return nil, err
	}

	prices := make(map[string]float64, len(tickers))
	for _, t := range tickers {
		if p, perr := strconv.ParseFloat(t.Price, 64); perr == nil && p > 0 {
			prices[strings.ToUpper(t.Symbol)] = p
		}
	}
	return prices, nil
}

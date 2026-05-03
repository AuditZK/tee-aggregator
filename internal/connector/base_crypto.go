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
	"strconv"
	"time"
)

// Retry policy for CONN-004: max 3 attempts with exponential backoff and
// Retry-After support for 429 / 5xx responses. The primary DoRequest is
// single-shot (preserves the old behaviour for HMAC-signed callers where
// the request timestamp would drift on replay); callers that know their
// request is idempotent should invoke DoRequestWithRetry instead.
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
// Use DoRequestWithRetry for idempotent requests that should survive
// transient 429 / 5xx responses (CONN-004).
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

// DoRequestWithRetry is like DoRequest but retries 429 / 5xx responses up to
// maxRetryAttempts times with exponential backoff (CONN-004). Honours the
// Retry-After header when present (capped at maxBackoff). Respects req.Context
// cancellation: a context deadline aborts the retry loop immediately.
//
// NOTE: callers must ensure the request is idempotent AND does not embed a
// time-sensitive signature (e.g. Binance's 5 s receive-window HMAC timestamp).
// Read endpoints such as /balance and /trades are safe; signed write endpoints
// should continue to use DoRequest and handle retry at a higher level.
func (b *CryptoBase) DoRequestWithRetry(req *http.Request) ([]byte, error) {
	ctx := req.Context()
	var lastBody []byte
	var lastErr error
	backoff := baseBackoff

	for attempt := 1; attempt <= maxRetryAttempts; attempt++ {
		resp, err := b.Client.Do(req)
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

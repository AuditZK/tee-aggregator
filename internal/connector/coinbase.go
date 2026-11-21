package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const coinbaseAPI = "https://api.coinbase.com"

// Coinbase implements Connector for Coinbase exchange using native HTTP.
// Uses HMAC-SHA256 signing (hex-encoded, no passphrase).
type Coinbase struct {
	base CryptoBase
}

// NewCoinbase creates a new Coinbase connector.
func NewCoinbase(creds *Credentials) *Coinbase {
	return &Coinbase{
		base: NewCryptoBase(creds.APIKey, creds.APISecret, coinbaseAPI),
	}
}

func (c *Coinbase) Exchange() string { return "coinbase" }

func (c *Coinbase) sign(timestamp, method, path, body string) string {
	return signHMACHex(c.base.APISecret, timestamp+method+path+body)
}

func (c *Coinbase) doRequest(ctx context.Context, method, path string) ([]byte, error) {
	return retryHTTP(c.base.Client, func() (*http.Request, error) {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		signature := c.sign(timestamp, method, path, "")

		req, err := http.NewRequestWithContext(ctx, method, c.base.BaseURL+path, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("CB-ACCESS-KEY", c.base.APIKey)
		req.Header.Set("CB-ACCESS-SIGN", signature)
		req.Header.Set("CB-ACCESS-TIMESTAMP", timestamp)
		req.Header.Set("CB-VERSION", "2023-01-01")
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
}

func (c *Coinbase) TestConnection(ctx context.Context) error {
	_, err := c.doRequest(ctx, "GET", "/v2/accounts")
	return err
}

type coinbaseAccount struct {
	Balance struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"balance"`
	Currency struct {
		Code string `json:"code"`
	} `json:"currency"`
}

// coinbaseMaxAccountPages bounds the next_uri walk. Reaching it is an error,
// not a truncation: past 100 wallets the old single-page read under-reported
// the equity in silence. Same contract as igMaxTransactionPages.
const coinbaseMaxAccountPages = 25

func (c *Coinbase) fetchAccounts(ctx context.Context) ([]coinbaseAccount, error) {
	path := "/v2/accounts?limit=100"
	var accounts []coinbaseAccount

	for page := 0; ; page++ {
		if page >= coinbaseMaxAccountPages {
			return nil, fmt.Errorf("accounts exceeded %d pages", coinbaseMaxAccountPages)
		}

		body, err := c.doRequest(ctx, "GET", path)
		if err != nil {
			return nil, fmt.Errorf("accounts: %w", err)
		}

		var resp struct {
			Data       []coinbaseAccount `json:"data"`
			Pagination struct {
				NextURI string `json:"next_uri"`
			} `json:"pagination"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("parse accounts: %w", err)
		}
		accounts = append(accounts, resp.Data...)

		next := strings.TrimSpace(resp.Pagination.NextURI)
		if next == "" {
			return accounts, nil
		}
		// next_uri is upstream-supplied and gets appended to the API base:
		// only a same-origin absolute path may steer the next request.
		if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") {
			return nil, fmt.Errorf("accounts: next_uri is not a path on this API")
		}
		path = next
	}
}

func (c *Coinbase) GetBalance(ctx context.Context) (*Balance, error) {
	accounts, err := c.fetchAccounts(ctx)
	if err != nil {
		return nil, err
	}

	// CONN-12, worst case: Coinbase has no futures leg (GetPositions returns
	// nil), so spot IS the account. Summing only stablecoins meant an account
	// holding nothing but BTC reported an equity of ZERO — read by the user as
	// a broken connection rather than a wrong number.
	var holdings []SpotHolding
	hasNonStable := false
	for _, a := range accounts {
		asset := a.Balance.Currency
		if asset == "" {
			asset = a.Currency.Code
		}
		amount, _ := strconv.ParseFloat(a.Balance.Amount, 64)
		if amount <= 0 {
			continue
		}
		holdings = append(holdings, SpotHolding{Asset: asset, Amount: amount})
		if !IsStablecoinUSD(asset) {
			hasNonStable = true
		}
	}
	priceMap := map[string]float64{}
	if hasNonStable {
		pm, perr := c.fetchPriceMap(ctx)
		if perr != nil {
			return nil, fmt.Errorf("%w: coinbase exchange rates: %v", ErrSpotPricingUnavailable, perr)
		}
		priceMap = pm
	}
	totalEquity := ValueSpotHoldingsUSD(holdings, priceMap)

	return &Balance{
		Equity:    totalEquity,
		Available: totalEquity,
		Currency:  "USD",
	}, nil
}

func (c *Coinbase) GetPositions(_ context.Context) ([]*Position, error) {
	// Coinbase basic API does not support margin/futures positions
	return nil, nil
}

func (c *Coinbase) GetTrades(_ context.Context, _, _ time.Time) ([]*Trade, error) {
	// Coinbase basic API doesn't expose trade history easily
	return nil, nil
}

// GetCashflows returns nil — not reliably available on Coinbase basic API.
func (c *Coinbase) GetCashflows(_ context.Context, _ time.Time) ([]*Cashflow, error) {
	return nil, nil
}

// fetchPriceMap builds a Binance-style price map from Coinbase's public
// exchange-rates endpoint. Those rates are INVERTED (they answer "how much of
// asset X is one USD worth"), so the USD price of an asset is 1/rate. Rates
// at or below zero are skipped rather than inverted into an absurd price.
func (c *Coinbase) fetchPriceMap(ctx context.Context) (map[string]float64, error) {
	body, err := retryHTTP(c.base.Client, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "GET", c.base.BaseURL+"/v2/exchange-rates?currency=USD", nil)
	})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Data struct {
			Rates map[string]string `json:"rates"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	prices := make(map[string]float64, len(resp.Data.Rates))
	for asset, rateStr := range resp.Data.Rates {
		rate, perr := strconv.ParseFloat(rateStr, 64)
		if perr != nil || rate <= 0 {
			continue
		}
		prices[strings.ToUpper(asset)+"USDT"] = 1 / rate
	}
	return prices, nil
}

package connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	message := timestamp + method + path + body
	mac := hmac.New(sha256.New, []byte(c.base.APISecret))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *Coinbase) doRequest(ctx context.Context, method, path string) ([]byte, error) {
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

	return c.base.DoRequest(req)
}

func (c *Coinbase) TestConnection(ctx context.Context) error {
	_, err := c.doRequest(ctx, "GET", "/v2/accounts")
	return err
}

func (c *Coinbase) GetBalance(ctx context.Context) (*Balance, error) {
	body, err := c.doRequest(ctx, "GET", "/v2/accounts?limit=100")
	if err != nil {
		return nil, fmt.Errorf("accounts: %w", err)
	}

	var resp struct {
		Data []struct {
			Balance struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			} `json:"balance"`
			Currency struct {
				Code string `json:"code"`
			} `json:"currency"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse accounts: %w", err)
	}

	stablecoins := []string{"USDT", "USDC", "USD"}
	totalEquity := 0.0
	for _, a := range resp.Data {
		for _, sc := range stablecoins {
			if strings.EqualFold(a.Balance.Currency, sc) || strings.EqualFold(a.Currency.Code, sc) {
				amount, _ := strconv.ParseFloat(a.Balance.Amount, 64)
				totalEquity += amount
			}
		}
	}

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

package connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const huobiAPI = "https://api.huobi.pro"

// Huobi implements Connector for Huobi (HTX) exchange using native HTTP.
// Uses HMAC-SHA256 query-param signing.
type Huobi struct {
	base CryptoBase
}

// NewHuobi creates a new Huobi connector.
func NewHuobi(creds *Credentials) *Huobi {
	return &Huobi{
		base: NewCryptoBase(creds.APIKey, creds.APISecret, huobiAPI),
	}
}

func (h *Huobi) Exchange() string { return "huobi" }

func (h *Huobi) sign(method, host, path string, params url.Values) string {
	payload := method + "\n" + host + "\n" + path + "\n" + params.Encode()
	mac := hmac.New(sha256.New, []byte(h.base.APISecret))
	mac.Write([]byte(payload))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (h *Huobi) signedGET(ctx context.Context, path string, extra url.Values) ([]byte, error) {
	params := url.Values{}
	params.Set("AccessKeyId", h.base.APIKey)
	params.Set("SignatureMethod", "HmacSHA256")
	params.Set("SignatureVersion", "2")
	params.Set("Timestamp", time.Now().UTC().Format("2006-01-02T15:04:05"))

	for k, vs := range extra {
		for _, v := range vs {
			params.Set(k, v)
		}
	}

	signature := h.sign("GET", "api.huobi.pro", path, params)
	params.Set("Signature", signature)

	reqURL := h.base.BaseURL + path + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	body, err := h.base.DoRequest(req)
	if err != nil {
		return nil, err
	}

	var result struct {
		Status  string `json:"status"`
		ErrCode string `json:"err-code"`
		ErrMsg  string `json:"err-msg"`
	}
	json.Unmarshal(body, &result)
	if result.Status == "error" {
		return nil, fmt.Errorf("huobi API error: %s (%s)", result.ErrMsg, result.ErrCode)
	}

	return body, nil
}

func (h *Huobi) TestConnection(ctx context.Context) error {
	_, err := h.signedGET(ctx, "/v1/account/accounts", nil)
	return err
}

func (h *Huobi) getAccountID(ctx context.Context) (string, error) {
	body, err := h.signedGET(ctx, "/v1/account/accounts", nil)
	if err != nil {
		return "", err
	}

	var resp struct {
		Data []struct {
			ID    int64  `json:"id"`
			Type  string `json:"type"`
			State string `json:"state"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", err
	}

	// Prefer "spot" account type
	for _, a := range resp.Data {
		if a.Type == "spot" && a.State == "working" {
			return strconv.FormatInt(a.ID, 10), nil
		}
	}
	// Fallback to first working account
	for _, a := range resp.Data {
		if a.State == "working" {
			return strconv.FormatInt(a.ID, 10), nil
		}
	}

	return "", fmt.Errorf("no working account found")
}

func (h *Huobi) GetBalance(ctx context.Context) (*Balance, error) {
	accountID, err := h.getAccountID(ctx)
	if err != nil {
		return nil, fmt.Errorf("get account id: %w", err)
	}

	path := fmt.Sprintf("/v1/account/accounts/%s/balance", accountID)
	body, err := h.signedGET(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("balance: %w", err)
	}

	var resp struct {
		Data struct {
			List []struct {
				Currency string `json:"currency"`
				Type     string `json:"type"` // "trade" or "frozen"
				Balance  string `json:"balance"`
			} `json:"list"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse balance: %w", err)
	}

	stablecoins := []string{"usdt", "usdc", "usd"}
	totalEquity := 0.0
	totalAvailable := 0.0
	for _, b := range resp.Data.List {
		for _, sc := range stablecoins {
			if strings.EqualFold(b.Currency, sc) {
				amount, _ := strconv.ParseFloat(b.Balance, 64)
				totalEquity += amount
				if b.Type == "trade" {
					totalAvailable += amount
				}
			}
		}
	}

	return &Balance{
		Equity:    totalEquity,
		Available: totalAvailable,
		Currency:  "USDT",
	}, nil
}

func (h *Huobi) GetPositions(_ context.Context) ([]*Position, error) {
	// Huobi spot does not have positions; futures requires a separate API domain
	return nil, nil
}

func (h *Huobi) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	params := url.Values{}
	params.Set("start-time", strconv.FormatInt(start.UnixMilli(), 10))
	params.Set("end-time", strconv.FormatInt(end.UnixMilli(), 10))
	params.Set("size", "100")

	body, err := h.signedGET(ctx, "/v1/order/matchresults", params)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			ID           int64  `json:"id"`
			Symbol       string `json:"symbol"`
			Type         string `json:"type"` // "buy-market", "sell-limit", etc.
			FilledAmount string `json:"filled-amount"`
			Price        string `json:"price"`
			FilledFees   string `json:"filled-fees"`
			FeeCurrency  string `json:"fee-currency"`
			CreatedAt    int64  `json:"created-at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(resp.Data))
	for _, t := range resp.Data {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.FilledAmount, 64)
		fee, _ := strconv.ParseFloat(t.FilledFees, 64)

		side := "buy"
		if strings.Contains(t.Type, "sell") {
			side = "sell"
		}

		trades = append(trades, &Trade{
			ID:          strconv.FormatInt(t.ID, 10),
			Symbol:      t.Symbol,
			Side:        side,
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.FeeCurrency,
			Timestamp:   time.UnixMilli(t.CreatedAt),
			MarketType:  MarketSpot,
		})
	}

	return trades, nil
}

// GetCashflows returns nil — not reliably available on Huobi.
func (h *Huobi) GetCashflows(_ context.Context, _ time.Time) ([]*Cashflow, error) {
	return nil, nil
}

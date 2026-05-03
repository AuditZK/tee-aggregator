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
	"time"
)

const bybitAPI = "https://api.bybit.com"

// Bybit implements Connector for Bybit exchange
type Bybit struct {
	apiKey    string
	apiSecret string
	client    *http.Client
}

// NewBybit creates a new Bybit connector
func NewBybit(creds *Credentials) *Bybit {
	return &Bybit{
		apiKey:    creds.APIKey,
		apiSecret: creds.APISecret,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (b *Bybit) Exchange() string {
	return "bybit"
}

func (b *Bybit) sign(timestamp, params string) string {
	payload := timestamp + b.apiKey + "5000" + params
	h := hmac.New(sha256.New, []byte(b.apiSecret))
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
}

func (b *Bybit) doRequest(ctx context.Context, method, path, params string) ([]byte, error) {
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	signature := b.sign(timestamp, params)

	url := bybitAPI + path
	if params != "" {
		url += "?" + params
	}

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-BAPI-API-KEY", b.apiKey)
	req.Header.Set("X-BAPI-TIMESTAMP", timestamp)
	req.Header.Set("X-BAPI-SIGN", signature)
	req.Header.Set("X-BAPI-RECV-WINDOW", "5000")

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	// CONN-AUDIT-001: bounded read.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return nil, err
	}

	var result struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
	}
	json.Unmarshal(body, &result)
	if result.RetCode != 0 {
		return nil, fmt.Errorf("bybit API error: %s", result.RetMsg)
	}

	return body, nil
}

func (b *Bybit) TestConnection(ctx context.Context) error {
	_, err := b.doRequest(ctx, "GET", "/v5/account/wallet-balance", "accountType=UNIFIED")
	return err
}

func (b *Bybit) GetBalance(ctx context.Context) (*Balance, error) {
	body, err := b.doRequest(ctx, "GET", "/v5/account/wallet-balance", "accountType=UNIFIED")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result struct {
			List []struct {
				TotalEquity           string `json:"totalEquity"`
				TotalWalletBalance    string `json:"totalWalletBalance"`
				TotalPerpUPL          string `json:"totalPerpUPL"`
				TotalAvailableBalance string `json:"totalAvailableBalance"`
			} `json:"list"`
		} `json:"result"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	if len(resp.Result.List) == 0 {
		return &Balance{Currency: "USDT"}, nil
	}

	account := resp.Result.List[0]
	equity, _ := strconv.ParseFloat(account.TotalEquity, 64)
	available, _ := strconv.ParseFloat(account.TotalAvailableBalance, 64)
	unrealized, _ := strconv.ParseFloat(account.TotalPerpUPL, 64)

	return &Balance{
		Available:     available,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USDT",
	}, nil
}

func (b *Bybit) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := b.doRequest(ctx, "GET", "/v5/position/list", "category=linear&settleCoin=USDT")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result struct {
			List []struct {
				Symbol        string `json:"symbol"`
				Side          string `json:"side"`
				Size          string `json:"size"`
				AvgPrice      string `json:"avgPrice"`
				MarkPrice     string `json:"markPrice"`
				UnrealisedPnl string `json:"unrealisedPnl"`
			} `json:"list"`
		} `json:"result"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var positions []*Position
	for _, p := range resp.Result.List {
		size, _ := strconv.ParseFloat(p.Size, 64)
		if size == 0 {
			continue
		}

		entry, _ := strconv.ParseFloat(p.AvgPrice, 64)
		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		unrealized, _ := strconv.ParseFloat(p.UnrealisedPnl, 64)

		side := "long"
		if p.Side == "Sell" {
			side = "short"
		}

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          size,
			EntryPrice:    entry,
			MarkPrice:     mark,
			UnrealizedPnL: unrealized,
			MarketType:    "swap",
		})
	}

	return positions, nil
}

func (b *Bybit) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	params := fmt.Sprintf("category=linear&startTime=%d&endTime=%d&limit=100",
		start.UnixMilli(), end.UnixMilli())

	body, err := b.doRequest(ctx, "GET", "/v5/execution/list", params)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result struct {
			List []struct {
				ExecId    string `json:"execId"`
				Symbol    string `json:"symbol"`
				Side      string `json:"side"`
				ExecPrice string `json:"execPrice"`
				ExecQty   string `json:"execQty"`
				ExecFee   string `json:"execFee"`
				ExecTime  string `json:"execTime"`
				ClosedPnl string `json:"closedPnl"`
			} `json:"list"`
		} `json:"result"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, t := range resp.Result.List {
		price, _ := strconv.ParseFloat(t.ExecPrice, 64)
		qty, _ := strconv.ParseFloat(t.ExecQty, 64)
		fee, _ := strconv.ParseFloat(t.ExecFee, 64)
		pnl, _ := strconv.ParseFloat(t.ClosedPnl, 64)
		execTime, _ := strconv.ParseInt(t.ExecTime, 10, 64)

		trades = append(trades, &Trade{
			ID:          t.ExecId,
			Symbol:      t.Symbol,
			Side:        t.Side,
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: "USDT",
			RealizedPnL: pnl,
			Timestamp:   time.UnixMilli(execTime),
			MarketType:  "swap",
		})
	}

	return trades, nil
}

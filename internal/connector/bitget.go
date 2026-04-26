package connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const bitgetAPI = "https://api.bitget.com"

// Bitget implements Connector for Bitget exchange using native HTTP.
// Uses HMAC-SHA256 + passphrase signing (similar to OKX).
type Bitget struct {
	base       CryptoBase
	passphrase string
}

// NewBitget creates a new Bitget connector.
func NewBitget(creds *Credentials) *Bitget {
	return &Bitget{
		base:       NewCryptoBase(creds.APIKey, creds.APISecret, bitgetAPI),
		passphrase: creds.Passphrase,
	}
}

func (b *Bitget) Exchange() string { return "bitget" }

func (b *Bitget) sign(timestamp, method, path, body string) string {
	message := timestamp + method + path + body
	mac := hmac.New(sha256.New, []byte(b.base.APISecret))
	mac.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (b *Bitget) doRequest(ctx context.Context, method, path string) ([]byte, error) {
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	signature := b.sign(timestamp, method, path, "")

	req, err := http.NewRequestWithContext(ctx, method, b.base.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("ACCESS-KEY", b.base.APIKey)
	req.Header.Set("ACCESS-SIGN", signature)
	req.Header.Set("ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("ACCESS-PASSPHRASE", b.passphrase)
	req.Header.Set("Content-Type", "application/json")

	body, err := b.base.DoRequest(req)
	if err != nil {
		return nil, err
	}

	var result struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
	}
	json.Unmarshal(body, &result)
	if result.Code != "00000" {
		return nil, fmt.Errorf("bitget API error: %s (code %s)", result.Msg, result.Code)
	}

	return body, nil
}

func (b *Bitget) TestConnection(ctx context.Context) error {
	_, err := b.doRequest(ctx, "GET", "/api/v2/spot/account/assets")
	return err
}

func (b *Bitget) GetBalance(ctx context.Context) (*Balance, error) {
	// Spot balance
	spotBody, err := b.doRequest(ctx, "GET", "/api/v2/spot/account/assets")
	if err != nil {
		return nil, fmt.Errorf("spot balance: %w", err)
	}

	var spotResp struct {
		Data []struct {
			Coin      string `json:"coin"`
			Available string `json:"available"`
			Frozen    string `json:"frozen"`
		} `json:"data"`
	}
	if err := json.Unmarshal(spotBody, &spotResp); err != nil {
		return nil, fmt.Errorf("parse spot balance: %w", err)
	}

	stablecoins := []string{"USDT", "USDC", "USD"}
	spotEquity := 0.0
	spotAvailable := 0.0
	for _, a := range spotResp.Data {
		for _, sc := range stablecoins {
			if strings.EqualFold(a.Coin, sc) {
				avail, _ := strconv.ParseFloat(a.Available, 64)
				frozen, _ := strconv.ParseFloat(a.Frozen, 64)
				spotEquity += avail + frozen
				spotAvailable += avail
			}
		}
	}

	// Futures balance (ignore error — account may not have futures enabled)
	futuresEquity := 0.0
	futuresUnrealized := 0.0
	futBody, err := b.doRequest(ctx, "GET", "/api/v2/mix/account/accounts?productType=USDT-FUTURES")
	if err == nil {
		var futResp struct {
			Data []struct {
				MarginCoin    string `json:"marginCoin"`
				AccountEquity string `json:"accountEquity"`
				UnrealizedPL  string `json:"unrealizedPL"`
				Available     string `json:"available"`
			} `json:"data"`
		}
		if json.Unmarshal(futBody, &futResp) == nil {
			for _, a := range futResp.Data {
				for _, sc := range stablecoins {
					if strings.EqualFold(a.MarginCoin, sc) {
						eq, _ := strconv.ParseFloat(a.AccountEquity, 64)
						upl, _ := strconv.ParseFloat(a.UnrealizedPL, 64)
						futuresEquity += eq
						futuresUnrealized += upl
					}
				}
			}
		}
	}

	return &Balance{
		Equity:        spotEquity + futuresEquity,
		Available:     spotAvailable,
		UnrealizedPnL: futuresUnrealized,
		Currency:      "USDT",
	}, nil
}

func (b *Bitget) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := b.doRequest(ctx, "GET", "/api/v2/mix/position/all-position?productType=USDT-FUTURES")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			Symbol       string `json:"symbol"`
			HoldSide     string `json:"holdSide"` // "long" or "short"
			Total        string `json:"total"`
			OpenPriceAvg string `json:"openPriceAvg"`
			MarkPrice    string `json:"markPrice"`
			UnrealizedPL string `json:"unrealizedPL"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	positions := make([]*Position, 0, len(resp.Data))
	for _, p := range resp.Data {
		size, _ := strconv.ParseFloat(p.Total, 64)
		if size == 0 {
			continue
		}
		entry, _ := strconv.ParseFloat(p.OpenPriceAvg, 64)
		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		upl, _ := strconv.ParseFloat(p.UnrealizedPL, 64)

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          p.HoldSide,
			Size:          size,
			EntryPrice:    entry,
			MarkPrice:     mark,
			UnrealizedPnL: upl,
			MarketType:    MarketSwap,
		})
	}

	return positions, nil
}

func (b *Bitget) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	path := fmt.Sprintf("/api/v2/spot/trade/fills?startTime=%d&endTime=%d&limit=100",
		start.UnixMilli(), end.UnixMilli())

	body, err := b.doRequest(ctx, "GET", path)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			TradeID   string `json:"tradeId"`
			Symbol    string `json:"symbol"`
			Side      string `json:"side"`
			Price     string `json:"priceAvg"`
			Size      string `json:"size"`
			Fee       string `json:"fees"`
			FeeCcy    string `json:"feeCurrency"`
			Timestamp string `json:"cTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(resp.Data))
	for _, t := range resp.Data {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Size, 64)
		fee, _ := strconv.ParseFloat(t.Fee, 64)
		ts, _ := strconv.ParseInt(t.Timestamp, 10, 64)

		trades = append(trades, &Trade{
			ID:          t.TradeID,
			Symbol:      t.Symbol,
			Side:        strings.ToLower(t.Side),
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.FeeCcy,
			Timestamp:   time.UnixMilli(ts),
			MarketType:  MarketSpot,
		})
	}

	return trades, nil
}

// GetCashflows returns nil — not reliably available on Bitget.
func (b *Bitget) GetCashflows(_ context.Context, _ time.Time) ([]*Cashflow, error) {
	return nil, nil
}

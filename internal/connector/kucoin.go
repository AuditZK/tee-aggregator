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

const kucoinAPI = "https://api.kucoin.com"

// KuCoin implements Connector for KuCoin exchange using native HTTP.
// Uses HMAC-SHA256 + HMAC-signed passphrase.
type KuCoin struct {
	base       CryptoBase
	passphrase string
}

// NewKuCoin creates a new KuCoin connector.
func NewKuCoin(creds *Credentials) *KuCoin {
	return &KuCoin{
		base:       NewCryptoBase(creds.APIKey, creds.APISecret, kucoinAPI),
		passphrase: creds.Passphrase,
	}
}

func (k *KuCoin) Exchange() string { return "kucoin" }

func (k *KuCoin) hmacSign(message string) string {
	mac := hmac.New(sha256.New, []byte(k.base.APISecret))
	mac.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (k *KuCoin) signedPassphrase() string {
	return k.hmacSign(k.passphrase)
}

func (k *KuCoin) doRequest(ctx context.Context, method, path string) ([]byte, error) {
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	signature := k.hmacSign(timestamp + method + path)

	req, err := http.NewRequestWithContext(ctx, method, k.base.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("KC-API-KEY", k.base.APIKey)
	req.Header.Set("KC-API-SIGN", signature)
	req.Header.Set("KC-API-TIMESTAMP", timestamp)
	req.Header.Set("KC-API-PASSPHRASE", k.signedPassphrase())
	req.Header.Set("KC-API-KEY-VERSION", "2")
	req.Header.Set("Content-Type", "application/json")

	body, err := k.base.DoRequest(req)
	if err != nil {
		return nil, err
	}

	var result struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
	}
	json.Unmarshal(body, &result)
	if result.Code != "200000" {
		return nil, fmt.Errorf("kucoin API error: %s (code %s)", result.Msg, result.Code)
	}

	return body, nil
}

func (k *KuCoin) TestConnection(ctx context.Context) error {
	_, err := k.doRequest(ctx, "GET", "/api/v1/accounts?type=trade")
	return err
}

func (k *KuCoin) GetBalance(ctx context.Context) (*Balance, error) {
	body, err := k.doRequest(ctx, "GET", "/api/v1/accounts?type=trade")
	if err != nil {
		return nil, fmt.Errorf("trade balance: %w", err)
	}

	var resp struct {
		Data []struct {
			Currency  string `json:"currency"`
			Balance   string `json:"balance"`
			Available string `json:"available"`
			Holds     string `json:"holds"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse balance: %w", err)
	}

	stablecoins := []string{"USDT", "USDC", "USD"}
	totalEquity := 0.0
	totalAvailable := 0.0
	for _, a := range resp.Data {
		for _, sc := range stablecoins {
			if strings.EqualFold(a.Currency, sc) {
				bal, _ := strconv.ParseFloat(a.Balance, 64)
				avail, _ := strconv.ParseFloat(a.Available, 64)
				totalEquity += bal
				totalAvailable += avail
			}
		}
	}

	return &Balance{
		Equity:    totalEquity,
		Available: totalAvailable,
		Currency:  "USDT",
	}, nil
}

func (k *KuCoin) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := k.doRequest(ctx, "GET", "/api/v1/positions")
	if err != nil {
		// KuCoin spot-only accounts may not have futures enabled
		return nil, nil
	}

	var resp struct {
		Data []struct {
			Symbol        string `json:"symbol"`
			CurrentQty    int64  `json:"currentQty"`
			AvgEntryPrice string `json:"avgEntryPrice"`
			MarkPrice     string `json:"markPrice"`
			UnrealisedPnl string `json:"unrealisedPnl"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	positions := make([]*Position, 0, len(resp.Data))
	for _, p := range resp.Data {
		if p.CurrentQty == 0 {
			continue
		}
		entry, _ := strconv.ParseFloat(p.AvgEntryPrice, 64)
		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		upl, _ := strconv.ParseFloat(p.UnrealisedPnl, 64)

		side := "long"
		size := float64(p.CurrentQty)
		if p.CurrentQty < 0 {
			side = "short"
			size = -size
		}

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          size,
			EntryPrice:    entry,
			MarkPrice:     mark,
			UnrealizedPnL: upl,
			MarketType:    MarketSwap,
		})
	}

	return positions, nil
}

func (k *KuCoin) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	path := fmt.Sprintf("/api/v1/fills?startAt=%d&endAt=%d&pageSize=500",
		start.UnixMilli(), end.UnixMilli())

	body, err := k.doRequest(ctx, "GET", path)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data struct {
			Items []struct {
				TradeID   string `json:"tradeId"`
				Symbol    string `json:"symbol"`
				Side      string `json:"side"`
				Price     string `json:"price"`
				Size      string `json:"size"`
				Fee       string `json:"fee"`
				FeeCcy    string `json:"feeCurrency"`
				CreatedAt int64  `json:"createdAt"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(resp.Data.Items))
	for _, t := range resp.Data.Items {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Size, 64)
		fee, _ := strconv.ParseFloat(t.Fee, 64)

		trades = append(trades, &Trade{
			ID:          t.TradeID,
			Symbol:      t.Symbol,
			Side:        strings.ToLower(t.Side),
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.FeeCcy,
			Timestamp:   time.UnixMilli(t.CreatedAt),
			MarketType:  MarketSpot,
		})
	}

	return trades, nil
}

// GetCashflows returns nil — not reliably available on KuCoin.
func (k *KuCoin) GetCashflows(_ context.Context, _ time.Time) ([]*Cashflow, error) {
	return nil, nil
}

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

const bingxAPI = "https://open-api.bingx.com"

// BingX implements Connector for BingX exchange using native HTTP.
// Uses HMAC-SHA256 query-param signing (similar to Binance).
type BingX struct {
	base CryptoBase
}

// NewBingX creates a new BingX connector.
func NewBingX(creds *Credentials) *BingX {
	return &BingX{
		base: NewCryptoBase(creds.APIKey, creds.APISecret, bingxAPI),
	}
}

func (b *BingX) Exchange() string { return "bingx" }

func (b *BingX) sign(params string) string {
	mac := hmac.New(sha256.New, []byte(b.base.APISecret))
	mac.Write([]byte(params))
	return hex.EncodeToString(mac.Sum(nil))
}

func (b *BingX) signedGET(ctx context.Context, path, params string) ([]byte, error) {
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	queryString := params
	if queryString != "" {
		queryString += "&"
	}
	queryString += "timestamp=" + ts

	signature := b.sign(queryString)
	reqURL := b.base.BaseURL + path + "?" + queryString + "&signature=" + signature

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-BX-APIKEY", b.base.APIKey)

	return b.base.DoRequest(req)
}

func (b *BingX) TestConnection(ctx context.Context) error {
	_, err := b.signedGET(ctx, "/openApi/swap/v2/user/balance", "")
	return err
}

func (b *BingX) GetBalance(ctx context.Context) (*Balance, error) {
	// Swap (perpetual futures) balance
	body, err := b.signedGET(ctx, "/openApi/swap/v2/user/balance", "")
	if err != nil {
		return nil, fmt.Errorf("swap balance: %w", err)
	}

	var resp struct {
		Code int `json:"code"`
		Data struct {
			Balance struct {
				Balance       string `json:"balance"`
				Equity        string `json:"equity"`
				UnrealizedPnL string `json:"unrealizedProfit"`
				Available     string `json:"availableMargin"`
			} `json:"balance"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse swap balance: %w", err)
	}

	equity, _ := strconv.ParseFloat(resp.Data.Balance.Equity, 64)
	available, _ := strconv.ParseFloat(resp.Data.Balance.Available, 64)
	unrealized, _ := strconv.ParseFloat(resp.Data.Balance.UnrealizedPnL, 64)

	// Spot balance (best effort)
	spotBody, err := b.signedGET(ctx, "/openApi/spot/v1/account/balance", "")
	if err == nil {
		var spotResp struct {
			Data struct {
				Balances []struct {
					Asset  string `json:"asset"`
					Free   string `json:"free"`
					Locked string `json:"locked"`
				} `json:"balances"`
			} `json:"data"`
		}
		if json.Unmarshal(spotBody, &spotResp) == nil {
			stablecoins := []string{"USDT", "USDC", "USD"}
			for _, bal := range spotResp.Data.Balances {
				for _, sc := range stablecoins {
					if strings.EqualFold(bal.Asset, sc) {
						free, _ := strconv.ParseFloat(bal.Free, 64)
						locked, _ := strconv.ParseFloat(bal.Locked, 64)
						equity += free + locked
						available += free
					}
				}
			}
		}
	}

	return &Balance{
		Equity:        equity,
		Available:     available,
		UnrealizedPnL: unrealized,
		Currency:      "USDT",
	}, nil
}

func (b *BingX) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := b.signedGET(ctx, "/openApi/swap/v2/user/positions", "")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			Symbol        string `json:"symbol"`
			PositionSide  string `json:"positionSide"` // "LONG" or "SHORT"
			PositionAmt   string `json:"positionAmt"`
			AvgPrice      string `json:"avgPrice"`
			MarkPrice     string `json:"markPrice"`
			UnrealizedPnL string `json:"unrealizedProfit"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	positions := make([]*Position, 0, len(resp.Data))
	for _, p := range resp.Data {
		size, _ := strconv.ParseFloat(p.PositionAmt, 64)
		if size == 0 {
			continue
		}
		entry, _ := strconv.ParseFloat(p.AvgPrice, 64)
		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		upl, _ := strconv.ParseFloat(p.UnrealizedPnL, 64)

		side := "long"
		if strings.EqualFold(p.PositionSide, "SHORT") || size < 0 {
			side = "short"
			if size < 0 {
				size = -size
			}
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

func (b *BingX) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	params := fmt.Sprintf("startTime=%d&endTime=%d&limit=1000",
		start.UnixMilli(), end.UnixMilli())

	body, err := b.signedGET(ctx, "/openApi/swap/v2/user/historyOrders", params)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data struct {
			Orders []struct {
				OrderID    string `json:"orderId"`
				Symbol     string `json:"symbol"`
				Side       string `json:"side"`
				AvgPrice   string `json:"avgPrice"`
				Volume     string `json:"executedQty"`
				Fee        string `json:"commission"`
				UpdateTime int64  `json:"updateTime"`
				Profit     string `json:"profit"`
			} `json:"orders"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(resp.Data.Orders))
	for _, t := range resp.Data.Orders {
		price, _ := strconv.ParseFloat(t.AvgPrice, 64)
		qty, _ := strconv.ParseFloat(t.Volume, 64)
		if qty == 0 {
			continue // skip unfilled orders
		}
		fee, _ := strconv.ParseFloat(t.Fee, 64)
		pnl, _ := strconv.ParseFloat(t.Profit, 64)

		trades = append(trades, &Trade{
			ID:          t.OrderID,
			Symbol:      t.Symbol,
			Side:        strings.ToLower(t.Side),
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: "USDT",
			RealizedPnL: pnl,
			Timestamp:   time.UnixMilli(t.UpdateTime),
			MarketType:  MarketSwap,
		})
	}

	return trades, nil
}

// GetCashflows returns nil — not reliably available on BingX.
func (b *BingX) GetCashflows(_ context.Context, _ time.Time) ([]*Cashflow, error) {
	return nil, nil
}

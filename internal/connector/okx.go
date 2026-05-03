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
	"time"
)

const okxAPI = "https://www.okx.com"

// OKX implements Connector for OKX exchange
type OKX struct {
	apiKey     string
	apiSecret  string
	passphrase string
	client     *http.Client
}

// NewOKX creates a new OKX connector
func NewOKX(creds *Credentials) *OKX {
	return &OKX{
		apiKey:     creds.APIKey,
		apiSecret:  creds.APISecret,
		passphrase: creds.Passphrase,
		client:     &http.Client{Timeout: 30 * time.Second},
	}
}

func (o *OKX) Exchange() string {
	return "okx"
}

func (o *OKX) sign(timestamp, method, path, body string) string {
	message := timestamp + method + path + body
	h := hmac.New(sha256.New, []byte(o.apiSecret))
	h.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func (o *OKX) doRequest(ctx context.Context, method, path string) ([]byte, error) {
	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	signature := o.sign(timestamp, method, path, "")

	req, err := http.NewRequestWithContext(ctx, method, okxAPI+path, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("OK-ACCESS-KEY", o.apiKey)
	req.Header.Set("OK-ACCESS-SIGN", signature)
	req.Header.Set("OK-ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("OK-ACCESS-PASSPHRASE", o.passphrase)
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}

	// CONN-AUDIT-001: bounded read.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return nil, err
	}

	var result struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
	}
	json.Unmarshal(body, &result)
	if result.Code != "0" {
		return nil, fmt.Errorf("okx API error: %s", result.Msg)
	}

	return body, nil
}

func (o *OKX) TestConnection(ctx context.Context) error {
	_, err := o.doRequest(ctx, "GET", "/api/v5/account/balance")
	return err
}

func (o *OKX) GetBalance(ctx context.Context) (*Balance, error) {
	body, err := o.doRequest(ctx, "GET", "/api/v5/account/balance")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			TotalEq string `json:"totalEq"`
			IsoEq   string `json:"isoEq"`
			AdjEq   string `json:"adjEq"`
			Details []struct {
				Ccy      string `json:"ccy"`
				Eq       string `json:"eq"`
				AvailBal string `json:"availBal"`
				UPL      string `json:"upl"`
			} `json:"details"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	if len(resp.Data) == 0 {
		return &Balance{Currency: "USDT"}, nil
	}

	account := resp.Data[0]
	equity, _ := strconv.ParseFloat(account.TotalEq, 64)

	// Find USDT balance
	var available, unrealized float64
	for _, d := range account.Details {
		if d.Ccy == "USDT" {
			available, _ = strconv.ParseFloat(d.AvailBal, 64)
			unrealized, _ = strconv.ParseFloat(d.UPL, 64)
			break
		}
	}

	return &Balance{
		Available:     available,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USDT",
	}, nil
}

func (o *OKX) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := o.doRequest(ctx, "GET", "/api/v5/account/positions")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			InstId   string `json:"instId"`
			PosSide  string `json:"posSide"`
			Pos      string `json:"pos"`
			AvgPx    string `json:"avgPx"`
			MarkPx   string `json:"markPx"`
			Upl      string `json:"upl"`
			InstType string `json:"instType"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var positions []*Position
	for _, p := range resp.Data {
		size, _ := strconv.ParseFloat(p.Pos, 64)
		if size == 0 {
			continue
		}

		entry, _ := strconv.ParseFloat(p.AvgPx, 64)
		mark, _ := strconv.ParseFloat(p.MarkPx, 64)
		unrealized, _ := strconv.ParseFloat(p.Upl, 64)

		side := "long"
		if p.PosSide == "short" || size < 0 {
			side = "short"
			if size < 0 {
				size = -size
			}
		}

		marketType := "swap"
		if p.InstType == "FUTURES" {
			marketType = "futures"
		} else if p.InstType == "OPTION" {
			marketType = "options"
		}

		positions = append(positions, &Position{
			Symbol:        p.InstId,
			Side:          side,
			Size:          size,
			EntryPrice:    entry,
			MarkPrice:     mark,
			UnrealizedPnL: unrealized,
			MarketType:    marketType,
		})
	}

	return positions, nil
}

func (o *OKX) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	path := fmt.Sprintf("/api/v5/trade/fills-history?instType=SWAP&begin=%d&end=%d&limit=100",
		start.UnixMilli(), end.UnixMilli())

	body, err := o.doRequest(ctx, "GET", path)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			TradeId  string `json:"tradeId"`
			InstId   string `json:"instId"`
			Side     string `json:"side"`
			FillPx   string `json:"fillPx"`
			FillSz   string `json:"fillSz"`
			Fee      string `json:"fee"`
			FeeCcy   string `json:"feeCcy"`
			Ts       string `json:"ts"`
			InstType string `json:"instType"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, t := range resp.Data {
		price, _ := strconv.ParseFloat(t.FillPx, 64)
		qty, _ := strconv.ParseFloat(t.FillSz, 64)
		fee, _ := strconv.ParseFloat(t.Fee, 64)
		ts, _ := strconv.ParseInt(t.Ts, 10, 64)

		marketType := "swap"
		if t.InstType == "SPOT" {
			marketType = "spot"
		} else if t.InstType == "FUTURES" {
			marketType = "futures"
		}

		trades = append(trades, &Trade{
			ID:          t.TradeId,
			Symbol:      t.InstId,
			Side:        t.Side,
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.FeeCcy,
			Timestamp:   time.UnixMilli(ts),
			MarketType:  marketType,
		})
	}

	return trades, nil
}

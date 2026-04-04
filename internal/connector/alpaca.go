package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	alpacaLiveAPI  = "https://api.alpaca.markets"
	alpacaPaperAPI = "https://paper-api.alpaca.markets"
	alpacaDataAPI  = "https://data.alpaca.markets"
)

// Alpaca implements Connector for Alpaca Markets
type Alpaca struct {
	apiKey    string
	apiSecret string
	client    *http.Client
	baseURL   string
}

// NewAlpaca creates a new Alpaca connector
func NewAlpaca(creds *Credentials) *Alpaca {
	baseURL := alpacaLiveAPI
	// Use paper trading if key starts with "PK"
	if len(creds.APIKey) > 2 && creds.APIKey[:2] == "PK" {
		baseURL = alpacaPaperAPI
	}

	return &Alpaca{
		apiKey:    creds.APIKey,
		apiSecret: creds.APISecret,
		client:    &http.Client{Timeout: 30 * time.Second},
		baseURL:   baseURL,
	}
}

func (a *Alpaca) Exchange() string {
	return "alpaca"
}

// DetectIsPaper reports whether credentials target Alpaca paper trading.
// TS parity: keys prefixed with "PK" indicate paper accounts.
func (a *Alpaca) DetectIsPaper(_ context.Context) (bool, error) {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(a.apiKey)), "PK"), nil
}

func (a *Alpaca) doRequest(ctx context.Context, baseURL, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+path, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("APCA-API-KEY-ID", a.apiKey)
	req.Header.Set("APCA-API-SECRET-KEY", a.apiSecret)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("alpaca API error: %s", string(body))
	}

	return body, nil
}

func (a *Alpaca) TestConnection(ctx context.Context) error {
	_, err := a.doRequest(ctx, a.baseURL, "/v2/account")
	return err
}

func (a *Alpaca) GetBalance(ctx context.Context) (*Balance, error) {
	body, err := a.doRequest(ctx, a.baseURL, "/v2/account")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Cash             string `json:"cash"`
		PortfolioValue   string `json:"portfolio_value"`
		Equity           string `json:"equity"`
		BuyingPower      string `json:"buying_power"`
		LongMarketValue  string `json:"long_market_value"`
		ShortMarketValue string `json:"short_market_value"`
		UnrealizedPL     string `json:"unrealized_pl"`
		UnrealizedPLPC   string `json:"unrealized_plpc"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	equity, _ := strconv.ParseFloat(resp.Equity, 64)
	cash, _ := strconv.ParseFloat(resp.Cash, 64)
	unrealized, _ := strconv.ParseFloat(resp.UnrealizedPL, 64)

	return &Balance{
		Available:     cash,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USD",
	}, nil
}

func (a *Alpaca) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := a.doRequest(ctx, a.baseURL, "/v2/positions")
	if err != nil {
		return nil, err
	}

	var resp []struct {
		Symbol        string `json:"symbol"`
		Qty           string `json:"qty"`
		Side          string `json:"side"`
		AvgEntryPrice string `json:"avg_entry_price"`
		CurrentPrice  string `json:"current_price"`
		UnrealizedPL  string `json:"unrealized_pl"`
		AssetClass    string `json:"asset_class"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var positions []*Position
	for _, p := range resp {
		qty, _ := strconv.ParseFloat(p.Qty, 64)
		if qty == 0 {
			continue
		}

		entry, _ := strconv.ParseFloat(p.AvgEntryPrice, 64)
		current, _ := strconv.ParseFloat(p.CurrentPrice, 64)
		unrealized, _ := strconv.ParseFloat(p.UnrealizedPL, 64)

		side := "long"
		if qty < 0 {
			side = "short"
			qty = -qty
		}

		marketType := "stocks"
		if p.AssetClass == "crypto" {
			marketType = "spot"
		}

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          qty,
			EntryPrice:    entry,
			MarkPrice:     current,
			UnrealizedPnL: unrealized,
			MarketType:    marketType,
		})
	}

	return positions, nil
}

func (a *Alpaca) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	path := fmt.Sprintf("/v2/account/activities/FILL?after=%s&until=%s",
		start.Format(time.RFC3339), end.Format(time.RFC3339))

	body, err := a.doRequest(ctx, a.baseURL, path)
	if err != nil {
		return nil, err
	}

	var resp []struct {
		ID              string `json:"id"`
		Symbol          string `json:"symbol"`
		Side            string `json:"side"`
		Price           string `json:"price"`
		Qty             string `json:"qty"`
		TransactionTime string `json:"transaction_time"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, t := range resp {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Qty, 64)
		ts, _ := time.Parse(time.RFC3339, t.TransactionTime)

		trades = append(trades, &Trade{
			ID:          t.ID,
			Symbol:      t.Symbol,
			Side:        t.Side,
			Price:       price,
			Quantity:    qty,
			Fee:         0, // Alpaca is commission-free
			FeeCurrency: "USD",
			Timestamp:   ts,
			MarketType:  "stocks",
		})
	}

	return trades, nil
}

// GetCashflows returns deposits/withdrawals. Alpaca does not expose
// capital flows via API, so this always returns empty (TS parity).
func (a *Alpaca) GetCashflows(_ context.Context, _ time.Time) ([]*Cashflow, error) {
	return nil, nil
}

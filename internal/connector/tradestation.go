package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	tradeStationAPI     = "https://api.tradestation.com/v3"
	tradeStationAuthURL = "https://signin.tradestation.com/oauth/token"

	// QUAL-001: extracted to remove a 5-way duplication.
	tradestationPathAccounts = "/brokerage/accounts"
)

// TradeStation implements Connector for TradeStation
type TradeStation struct {
	clientID     string
	clientSecret string
	refreshToken string
	accessToken  string
	tokenExpiry  time.Time
	client       *http.Client
}

// NewTradeStation creates a new TradeStation connector
func NewTradeStation(creds *Credentials) *TradeStation {
	return &TradeStation{
		clientID:     creds.APIKey,
		clientSecret: creds.APISecret,
		refreshToken: creds.Passphrase, // RefreshToken stored in passphrase field
		client:       &http.Client{Timeout: 30 * time.Second},
	}
}

func (t *TradeStation) Exchange() string {
	return "tradestation"
}

// DetectIsPaper mirrors TS behavior: all accounts must be simulation ("Sim*").
func (t *TradeStation) DetectIsPaper(ctx context.Context) (bool, error) {
	body, err := t.doRequest(ctx, tradestationPathAccounts)
	if err != nil {
		return false, err
	}

	var accountsResp struct {
		Accounts []struct {
			AccountType string `json:"AccountType"`
		} `json:"Accounts"`
	}
	if err := json.Unmarshal(body, &accountsResp); err != nil {
		return false, err
	}
	if len(accountsResp.Accounts) == 0 {
		return false, nil
	}

	for _, acct := range accountsResp.Accounts {
		if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(acct.AccountType)), "sim") {
			return false, nil
		}
	}
	return true, nil
}

func (t *TradeStation) refreshAccessToken(ctx context.Context) error {
	if t.accessToken != "" && time.Now().Before(t.tokenExpiry) {
		return nil
	}

	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("client_id", t.clientID)
	data.Set("client_secret", t.clientSecret)
	data.Set("refresh_token", t.refreshToken)

	req, err := http.NewRequestWithContext(ctx, "POST", tradeStationAuthURL, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}

	// CONN-AUDIT-001: bounded read for both error and success paths.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token refresh failed: %s", TruncatedBody(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return err
	}

	t.accessToken = tokenResp.AccessToken
	t.tokenExpiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn-60) * time.Second)

	return nil
}

func (t *TradeStation) doRequest(ctx context.Context, path string) ([]byte, error) {
	if err := t.refreshAccessToken(ctx); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "GET", tradeStationAPI+path, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+t.accessToken)

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}

	// CONN-AUDIT-001 + 002: bounded read + truncated body in errors.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tradestation API error: %s", TruncatedBody(body))
	}

	return body, nil
}

func (t *TradeStation) TestConnection(ctx context.Context) error {
	_, err := t.doRequest(ctx, tradestationPathAccounts)
	return err
}

func (t *TradeStation) GetBalance(ctx context.Context) (*Balance, error) {
	// Get accounts first
	body, err := t.doRequest(ctx, tradestationPathAccounts)
	if err != nil {
		return nil, err
	}

	var accountsResp struct {
		Accounts []struct {
			AccountID   string `json:"AccountID"`
			AccountType string `json:"AccountType"`
		} `json:"Accounts"`
	}

	if err := json.Unmarshal(body, &accountsResp); err != nil {
		return nil, err
	}

	if len(accountsResp.Accounts) == 0 {
		return nil, fmt.Errorf("no accounts found")
	}

	// Get balances for first account
	accountID := accountsResp.Accounts[0].AccountID
	body, err = t.doRequest(ctx, fmt.Sprintf("/brokerage/accounts/%s/balances", accountID))
	if err != nil {
		return nil, err
	}

	var balResp struct {
		Balances []struct {
			CashBalance          float64 `json:"CashBalance"`
			Equity               float64 `json:"Equity"`
			MarketValue          float64 `json:"MarketValue"`
			TodaysProfitLoss     float64 `json:"TodaysProfitLoss"`
			UnrealizedProfitLoss float64 `json:"UnrealizedProfitLoss"`
		} `json:"Balances"`
	}

	if err := json.Unmarshal(body, &balResp); err != nil {
		return nil, err
	}

	if len(balResp.Balances) == 0 {
		return nil, fmt.Errorf("no balance data")
	}

	bal := balResp.Balances[0]
	return &Balance{
		Available:     bal.CashBalance,
		Equity:        bal.Equity,
		UnrealizedPnL: bal.UnrealizedProfitLoss,
		Currency:      "USD",
	}, nil
}

func (t *TradeStation) GetPositions(ctx context.Context) ([]*Position, error) {
	// Get accounts first
	body, err := t.doRequest(ctx, tradestationPathAccounts)
	if err != nil {
		return nil, err
	}

	var accountsResp struct {
		Accounts []struct {
			AccountID string `json:"AccountID"`
		} `json:"Accounts"`
	}

	if err := json.Unmarshal(body, &accountsResp); err != nil {
		return nil, err
	}

	if len(accountsResp.Accounts) == 0 {
		return nil, nil
	}

	accountID := accountsResp.Accounts[0].AccountID
	body, err = t.doRequest(ctx, fmt.Sprintf("/brokerage/accounts/%s/positions", accountID))
	if err != nil {
		return nil, err
	}

	var posResp struct {
		Positions []struct {
			Symbol               string  `json:"Symbol"`
			Quantity             float64 `json:"Quantity"`
			AveragePrice         float64 `json:"AveragePrice"`
			Last                 float64 `json:"Last"`
			UnrealizedProfitLoss float64 `json:"UnrealizedProfitLoss"`
			AssetType            string  `json:"AssetType"`
			LongShort            string  `json:"LongShort"`
		} `json:"Positions"`
	}

	if err := json.Unmarshal(body, &posResp); err != nil {
		return nil, err
	}

	var positions []*Position
	for _, p := range posResp.Positions {
		if p.Quantity == 0 {
			continue
		}

		side := "long"
		if p.LongShort == "Short" {
			side = "short"
		}

		marketType := "stocks"
		switch p.AssetType {
		case "FUTURE":
			marketType = "futures"
		case "OPTION", "STOCKOPTION", "INDEXOPTION", "FUTUREOPTION":
			marketType = "options"
		case "FOREX":
			marketType = "forex"
		}

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          p.Quantity,
			EntryPrice:    p.AveragePrice,
			MarkPrice:     p.Last,
			UnrealizedPnL: p.UnrealizedProfitLoss,
			MarketType:    marketType,
		})
	}

	return positions, nil
}

func (t *TradeStation) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	// Get accounts first
	body, err := t.doRequest(ctx, tradestationPathAccounts)
	if err != nil {
		return nil, err
	}

	var accountsResp struct {
		Accounts []struct {
			AccountID string `json:"AccountID"`
		} `json:"Accounts"`
	}

	if err := json.Unmarshal(body, &accountsResp); err != nil {
		return nil, err
	}

	if len(accountsResp.Accounts) == 0 {
		return nil, nil
	}

	accountID := accountsResp.Accounts[0].AccountID
	path := fmt.Sprintf("/brokerage/accounts/%s/orders?since=%s",
		accountID, start.Format("2006-01-02"))

	body, err = t.doRequest(ctx, path)
	if err != nil {
		return nil, err
	}

	var ordersResp struct {
		Orders []struct {
			OrderID        string  `json:"OrderID"`
			Symbol         string  `json:"Symbol"`
			BuyOrSell      string  `json:"BuyOrSell"`
			FilledPrice    float64 `json:"FilledPrice"`
			FilledQty      float64 `json:"FilledQty"`
			Status         string  `json:"Status"`
			ClosedDateTime string  `json:"ClosedDateTime"`
			AssetType      string  `json:"AssetType"`
			Commission     float64 `json:"Commission"`
		} `json:"Orders"`
	}

	if err := json.Unmarshal(body, &ordersResp); err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, o := range ordersResp.Orders {
		if o.Status != "FLL" { // Only filled orders
			continue
		}

		ts, _ := time.Parse(time.RFC3339, o.ClosedDateTime)
		if ts.Before(start) || ts.After(end) {
			continue
		}

		side := "buy"
		if o.BuyOrSell == "Sell" {
			side = "sell"
		}

		marketType := "stocks"
		switch o.AssetType {
		case "FUTURE":
			marketType = "futures"
		case "OPTION", "STOCKOPTION":
			marketType = "options"
		case "FOREX":
			marketType = "forex"
		}

		trades = append(trades, &Trade{
			ID:          o.OrderID,
			Symbol:      o.Symbol,
			Side:        side,
			Price:       o.FilledPrice,
			Quantity:    o.FilledQty,
			Fee:         o.Commission,
			FeeCurrency: "USD",
			Timestamp:   ts,
			MarketType:  marketType,
		})
	}

	return trades, nil
}

// GetCashflows returns deposits/withdrawals. TradeStation does not reliably
// expose capital flows via API, so this returns empty (TS parity).
func (t *TradeStation) GetCashflows(_ context.Context, _ time.Time) ([]*Cashflow, error) {
	return nil, nil
}

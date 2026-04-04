package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const lighterAPI = "https://mainnet.zklighter.elliot.ai"

// Lighter is a read-only DEX connector for the Lighter Protocol.
type Lighter struct {
	walletAddress string
	accountIndex  *int
	client        *http.Client
}

// NewLighter creates a new Lighter connector.
func NewLighter(creds *Credentials) *Lighter {
	addr := creds.WalletAddress
	if addr == "" {
		addr = creds.APIKey // Fallback: wallet address stored in APIKey field
	}
	return &Lighter{
		walletAddress: addr,
		client:        &http.Client{Timeout: 30 * time.Second},
	}
}

func (l *Lighter) Exchange() string { return "lighter" }

// DetectIsPaper mirrors TS behavior: Lighter connector targets mainnet.
func (l *Lighter) DetectIsPaper(_ context.Context) (bool, error) {
	return false, nil
}

func (l *Lighter) TestConnection(ctx context.Context) error {
	_, err := l.GetBalance(ctx)
	return err
}

func (l *Lighter) GetBalance(ctx context.Context) (*Balance, error) {
	account, err := l.fetchAccount(ctx)
	if err != nil {
		return nil, err
	}

	equity := parseFloatOrZero(account.TotalAssetValue)
	available := parseFloatOrZero(account.AvailableBalance)
	collateral := parseFloatOrZero(account.Collateral)

	return &Balance{
		Equity:        equity,
		Available:     available,
		UnrealizedPnL: equity - collateral,
		Currency:      "USD",
	}, nil
}

func (l *Lighter) GetPositions(ctx context.Context) ([]*Position, error) {
	account, err := l.fetchAccount(ctx)
	if err != nil {
		return nil, err
	}

	var positions []*Position
	for _, p := range account.Positions {
		size := parseFloatOrZero(p.Size)
		if size == 0 {
			continue
		}

		side := "long"
		if p.Sign < 0 {
			side = "short"
		}

		entryPx := parseFloatOrZero(p.AvgEntryPrice)
		pnl := parseFloatOrZero(p.UnrealizedPnl)

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          size,
			EntryPrice:    entryPx,
			UnrealizedPnL: pnl,
			MarketType:    MarketSwap,
		})
	}

	return positions, nil
}

func (l *Lighter) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	accountIndex, err := l.getAccountIndex(ctx)
	if err != nil {
		return nil, err
	}

	fills, err := l.fetchAllTrades(ctx, accountIndex)
	if err != nil {
		// Fallback to legacy endpoint for backward compatibility.
		fills, err = l.fetchLegacyFills(ctx)
	}
	if err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, f := range fills {
		ts := lighterTimestamp(f.Timestamp)
		if ts.Before(start) || ts.After(end) {
			continue
		}

		price := parseFloatOrZero(f.Price)
		qty := math.Abs(parseFloatOrZero(f.Size))

		// Side and fee resolution aligned with TS implementation.
		isBuyer := f.BidAccountID == accountIndex
		isTaker := false
		if f.IsMakerAsk {
			isTaker = f.BidAccountID == accountIndex
		} else {
			isTaker = f.AskAccountID == accountIndex
		}
		side := "sell"
		if isBuyer {
			side = "buy"
		}
		fee := float64(f.MakerFee)
		if isTaker {
			fee = float64(f.TakerFee)
		}

		trades = append(trades, &Trade{
			ID:          fmt.Sprintf("%d", f.TradeID),
			Symbol:      fmt.Sprintf("market_%d", f.MarketID),
			Side:        side,
			Price:       price,
			Quantity:    qty,
			Fee:         math.Abs(fee),
			FeeCurrency: "USDC",
			Timestamp:   ts,
			MarketType:  MarketSwap,
		})
	}

	return trades, nil
}

type lighterAccountResponse struct {
	Accounts []lighterAccount `json:"accounts"`
}

type lighterAccount struct {
	Index            int               `json:"index"`
	L1Address        string            `json:"l1_address"`
	AvailableBalance string            `json:"available_balance"`
	Collateral       string            `json:"collateral"`
	TotalAssetValue  string            `json:"total_asset_value"`
	Positions        []lighterPosition `json:"positions"`
}

type lighterPosition struct {
	MarketID      int    `json:"market_id"`
	Symbol        string `json:"symbol"`
	Sign          int    `json:"sign"` // 1=long, -1=short
	Size          string `json:"size"`
	AvgEntryPrice string `json:"avg_entry_price"`
	UnrealizedPnl string `json:"unrealized_pnl"`
}

type lighterTrade struct {
	TradeID      int    `json:"trade_id"`
	Timestamp    int64  `json:"timestamp"`
	MarketID     int    `json:"market_id"`
	Size         string `json:"size"`
	Price        string `json:"price"`
	IsMakerAsk   bool   `json:"is_maker_ask"`
	AskAccountID int    `json:"ask_account_id"`
	BidAccountID int    `json:"bid_account_id"`
	TakerFee     int    `json:"taker_fee"`
	MakerFee     int    `json:"maker_fee"`
}

type lighterTradesResponse struct {
	NextCursor string         `json:"next_cursor"`
	Trades     []lighterTrade `json:"trades"`
	Fills      []lighterTrade `json:"fills"` // Legacy key on older endpoint variants
}

func (l *Lighter) getAccountIndex(ctx context.Context) (int, error) {
	if l.accountIndex != nil {
		return *l.accountIndex, nil
	}
	account, err := l.fetchAccount(ctx)
	if err != nil {
		return 0, err
	}
	l.accountIndex = &account.Index
	return account.Index, nil
}

func (l *Lighter) fetchAccount(ctx context.Context) (*lighterAccount, error) {
	// Primary (current) API contract.
	data, err := l.doGet(ctx, "/api/v1/account", map[string]string{
		"by":    "l1_address",
		"value": l.walletAddress,
	})
	if err == nil {
		var resp lighterAccountResponse
		if unmarshalErr := json.Unmarshal(data, &resp); unmarshalErr == nil && len(resp.Accounts) > 0 {
			account := resp.Accounts[0]
			l.accountIndex = &account.Index
			return &account, nil
		}
	}

	// Legacy fallback used by older deployments.
	legacyData, legacyErr := l.doGet(ctx, fmt.Sprintf("/api/v1/account/%s", l.walletAddress), nil)
	if legacyErr != nil {
		if err != nil {
			return nil, err
		}
		return nil, legacyErr
	}

	var account lighterAccount
	if err := json.Unmarshal(legacyData, &account); err != nil {
		return nil, fmt.Errorf("parse account: %w", err)
	}
	l.accountIndex = &account.Index
	return &account, nil
}

func (l *Lighter) fetchAllTrades(ctx context.Context, accountIndex int) ([]lighterTrade, error) {
	const maxPages = 10

	var all []lighterTrade
	var cursor string

	for page := 0; page < maxPages; page++ {
		query := map[string]string{
			"account_index": strconv.Itoa(accountIndex),
			"type":          "trade",
		}
		if cursor != "" {
			query["cursor"] = cursor
		}

		data, err := l.doGet(ctx, "/api/v1/trades", query)
		if err != nil {
			return nil, err
		}

		var resp lighterTradesResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			return nil, fmt.Errorf("parse trades: %w", err)
		}

		pageTrades := resp.Trades
		if len(pageTrades) == 0 && len(resp.Fills) > 0 {
			pageTrades = resp.Fills
		}
		all = append(all, pageTrades...)

		if resp.NextCursor == "" || len(pageTrades) == 0 {
			break
		}
		cursor = resp.NextCursor
	}

	return all, nil
}

func (l *Lighter) fetchLegacyFills(ctx context.Context) ([]lighterTrade, error) {
	data, err := l.doGet(ctx, fmt.Sprintf("/api/v1/fills/%s", l.walletAddress), nil)
	if err != nil {
		return nil, err
	}

	var resp lighterTradesResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("parse legacy fills: %w", err)
	}
	return resp.Fills, nil
}

func (l *Lighter) doGet(ctx context.Context, path string, query map[string]string) (json.RawMessage, error) {
	endpoint, err := url.Parse(lighterAPI + path)
	if err != nil {
		return nil, err
	}
	if len(query) > 0 {
		q := endpoint.Query()
		for k, v := range query {
			q.Set(k, v)
		}
		endpoint.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := l.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lighter API error %d: %s", resp.StatusCode, string(data))
	}

	return data, nil
}

func parseFloatOrZero(value string) float64 {
	result, _ := strconv.ParseFloat(value, 64)
	return result
}

// GetBalanceByMarket returns swap market balance (Lighter is perps-only).
func (l *Lighter) GetBalanceByMarket(ctx context.Context) ([]*MarketBalance, error) {
	bal, err := l.GetBalance(ctx)
	if err != nil {
		return nil, err
	}
	if bal.Equity <= 0 {
		return nil, nil
	}
	return []*MarketBalance{{
		MarketType:      MarketSwap,
		Equity:          bal.Equity,
		AvailableMargin: bal.Available,
	}}, nil
}

func lighterTimestamp(raw int64) time.Time {
	// New API returns seconds; legacy variants may return milliseconds.
	if raw > 1_000_000_000_000 {
		return time.UnixMilli(raw)
	}
	return time.Unix(raw, 0)
}

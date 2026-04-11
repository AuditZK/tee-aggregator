package connector

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const lighterAPI = "https://mainnet.zklighter.elliot.ai"

// Lighter is a read-only DEX connector for the Lighter Protocol.
// Credentials: apiKey = read-only auth token (ro:ACCOUNT_INDEX:...).
// The account index is extracted from the token.
type Lighter struct {
	authToken    string
	accountIndex *int
	client       *http.Client
}

// NewLighter creates a new Lighter connector.
// creds.APIKey = read-only auth token (e.g. "ro:713194:single:...").
func NewLighter(creds *Credentials) *Lighter {
	token := strings.TrimSpace(creds.APIKey)

	l := &Lighter{
		authToken: token,
		client:    &http.Client{Timeout: 30 * time.Second},
	}

	// Extract account_index from token: "ro:713194:single:..."
	if parts := strings.SplitN(token, ":", 3); len(parts) >= 2 {
		if idx, err := strconv.Atoi(parts[1]); err == nil {
			l.accountIndex = &idx
		}
	}

	return l
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

	// Use /api/v1/export with auth token (new API)
	csvRows, err := l.fetchExportTrades(ctx, accountIndex, start, end)
	if err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, row := range csvRows {
		ts, _ := time.Parse("2006-01-02 15:04:05", row.Date)
		if ts.IsZero() || ts.Before(start) || ts.After(end) {
			continue
		}

		side := "buy"
		sLower := strings.ToLower(row.Side)
		if strings.Contains(sLower, "short") || strings.Contains(sLower, "sell") {
			side = "sell"
		}

		trades = append(trades, &Trade{
			Symbol:      row.Market,
			Side:        side,
			Price:       row.Price,
			Quantity:    row.Size,
			Fee:         row.Fee,
			FeeCurrency: "USDC",
			RealizedPnL: row.ClosedPnL,
			Timestamp:   ts.UTC(),
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

type lighterExportRow struct {
	Market    string
	Side      string
	Date      string
	Price     float64
	Size      float64
	ClosedPnL float64
	Fee       float64
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
	if l.accountIndex != nil && *l.accountIndex > 0 {
		data, err := l.doGet(ctx, "/api/v1/account", map[string]string{
			"by":    "index",
			"value": strconv.Itoa(*l.accountIndex),
		})
		if err == nil {
			var resp lighterAccountResponse
			if json.Unmarshal(data, &resp) == nil && len(resp.Accounts) > 0 {
				return &resp.Accounts[0], nil
			}
		}
		return nil, fmt.Errorf("lighter: could not fetch account (index=%d)", *l.accountIndex)
	}

	return nil, fmt.Errorf("lighter: no account_index available (check auth token format)")
}

// fetchExportTrades uses /api/v1/export to get trade history as CSV.
func (l *Lighter) fetchExportTrades(ctx context.Context, accountIndex int, start, end time.Time) ([]lighterExportRow, error) {
	startMs := start.UnixMilli()
	endMs := end.UnixMilli()

	query := map[string]string{
		"type":            "trade",
		"account_index":   strconv.Itoa(accountIndex),
		"start_timestamp": strconv.FormatInt(startMs, 10),
		"end_timestamp":   strconv.FormatInt(endMs, 10),
	}

	data, err := l.doGetAuth(ctx, "/api/v1/export", query)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Code    int    `json:"code"`
		DataURL string `json:"data_url"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Code == 22504 {
		// No export data found — not an error, just no trades
		return nil, nil
	}
	if resp.DataURL == "" {
		return nil, fmt.Errorf("lighter export: no data_url (code=%d, msg=%s)", resp.Code, resp.Message)
	}

	// Download CSV from S3
	return l.downloadCSV(ctx, resp.DataURL)
}

func (l *Lighter) downloadCSV(ctx context.Context, csvURL string) ([]lighterExportRow, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", csvURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := l.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lighter CSV download: HTTP %d", resp.StatusCode)
	}

	reader := csv.NewReader(resp.Body)
	// Header: Market,Side,Date,Trade Value,Size,Price,Closed PnL,Fee,Role,Type
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("lighter CSV: read header: %w", err)
	}

	// Build column index map
	colIdx := make(map[string]int)
	for i, h := range header {
		colIdx[strings.TrimSpace(h)] = i
	}

	var rows []lighterExportRow
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		row := lighterExportRow{
			Market:    csvCol(record, colIdx, "Market"),
			Side:      csvCol(record, colIdx, "Side"),
			Date:      csvCol(record, colIdx, "Date"),
			Price:     csvColFloat(record, colIdx, "Price"),
			Size:      csvColFloat(record, colIdx, "Size"),
			ClosedPnL: csvColFloat(record, colIdx, "Closed PnL"),
			Fee:       csvColFloat(record, colIdx, "Fee"),
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func csvCol(record []string, idx map[string]int, col string) string {
	if i, ok := idx[col]; ok && i < len(record) {
		return strings.TrimSpace(record[i])
	}
	return ""
}

func csvColFloat(record []string, idx map[string]int, col string) float64 {
	s := csvCol(record, idx, col)
	if s == "" || s == "-" {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	return math.Abs(v)
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

// doGetAuth sends a GET with the authorization header.
func (l *Lighter) doGetAuth(ctx context.Context, path string, query map[string]string) (json.RawMessage, error) {
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
	req.Header.Set("authorization", l.authToken)

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

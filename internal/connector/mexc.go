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

const mexcAPI = "https://api.mexc.com"

// QUAL-001: extracted to remove a 3-way duplication of the spot-account path.
const mexcPathAccount = "/api/v3/account"

// MEXC implements Connector for MEXC exchange using native HTTP (no CCXT).
// Uses HMAC-SHA256 signing, same as Binance.
type MEXC struct {
	base CryptoBase

	// Cached from last GetBalance for GetBalanceByMarket
	cachedSpotEquity    float64
	cachedFuturesEquity float64
	cachedFuturesAvail  float64
}

// NewMEXC creates a new MEXC connector.
func NewMEXC(creds *Credentials) *MEXC {
	return &MEXC{
		base: NewCryptoBase(creds.APIKey, creds.APISecret, mexcAPI),
	}
}

func (m *MEXC) Exchange() string { return "mexc" }

func (m *MEXC) sign(params string) string {
	mac := hmac.New(sha256.New, []byte(m.base.APISecret))
	mac.Write([]byte(params))
	return hex.EncodeToString(mac.Sum(nil))
}

func (m *MEXC) signedGET(ctx context.Context, path, params string) ([]byte, error) {
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	queryString := params
	if queryString != "" {
		queryString += "&"
	}
	queryString += "timestamp=" + ts

	signature := m.sign(queryString)
	url := m.base.BaseURL + path + "?" + queryString + "&signature=" + signature

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MEXC-APIKEY", m.base.APIKey)

	return m.base.DoRequest(req)
}

func (m *MEXC) TestConnection(ctx context.Context) error {
	_, err := m.signedGET(ctx, mexcPathAccount, "")
	return err
}

func (m *MEXC) GetBalance(ctx context.Context) (*Balance, error) {
	// Spot balance
	spotBody, err := m.signedGET(ctx, mexcPathAccount, "")
	if err != nil {
		return nil, fmt.Errorf("spot balance: %w", err)
	}

	var spotResp struct {
		Balances []struct {
			Asset  string `json:"asset"`
			Free   string `json:"free"`
			Locked string `json:"locked"`
		} `json:"balances"`
	}
	if err := json.Unmarshal(spotBody, &spotResp); err != nil {
		return nil, fmt.Errorf("parse spot balance: %w", err)
	}

	spotEquity := 0.0
	spotAvailable := 0.0
	stablecoins := []string{"USDT", "USDC", "USD", "BUSD", "DAI", "FDUSD"}

	// Sum stablecoins + convert altcoins to USD via ticker
	for _, b := range spotResp.Balances {
		free, _ := strconv.ParseFloat(b.Free, 64)
		locked, _ := strconv.ParseFloat(b.Locked, 64)
		total := free + locked
		if total <= 0 {
			continue
		}

		isStable := false
		for _, sc := range stablecoins {
			if strings.EqualFold(b.Asset, sc) {
				isStable = true
				spotEquity += total
				spotAvailable += free
				break
			}
		}

		// Convert altcoins to USD via MEXC ticker
		if !isStable {
			price := m.fetchTickerPrice(ctx, b.Asset)
			if price > 0 {
				spotEquity += total * price
				spotAvailable += free * price
			}
		}
	}

	// Futures balance via contract.mexc.com
	futuresEquity := 0.0
	futuresAvailable := 0.0
	futuresCash := 0.0
	futBody, err2 := m.futuresSignedGET(ctx, "/api/v1/private/account/assets", "")
	if err2 == nil {
		var futResp struct {
			Success bool `json:"success"`
			Data    []struct {
				Currency         string  `json:"currency"`
				Equity           float64 `json:"equity"`
				AvailableBalance float64 `json:"availableBalance"`
				CashBalance      float64 `json:"cashBalance"`
				Unrealized       float64 `json:"unrealized"`
				PositionMargin   float64 `json:"positionMargin"`
			} `json:"data"`
		}
		if json.Unmarshal(futBody, &futResp) == nil && futResp.Success {
			for _, a := range futResp.Data {
				if a.Equity > 0 {
					for _, sc := range stablecoins {
						if strings.EqualFold(a.Currency, sc) {
							futuresEquity += a.Equity
							futuresAvailable += a.AvailableBalance
							// TS parity: realizedBalance = cashBalance (deposited cash +
							// realised P&L). unrealizedPnL = equity - cashBalance.
							// Using availableBalance instead overstates unrealized by
							// positionMargin. See CcxtExchangeConnector.ts extractSwapEquity.
							futuresCash += a.CashBalance
						}
					}
				}
			}
		}
	}

	// Cache for GetBalanceByMarket
	m.cachedSpotEquity = spotEquity
	m.cachedFuturesEquity = futuresEquity
	m.cachedFuturesAvail = futuresAvailable

	totalEquity := spotEquity + futuresEquity
	totalAvailable := spotAvailable + futuresAvailable

	return &Balance{
		Equity:        totalEquity,
		Available:     totalAvailable,
		UnrealizedPnL: futuresEquity - futuresCash,
		Currency:      "USDT",
	}, nil
}

// GetBalanceByMarket returns spot and swap equity breakdown (cached from GetBalance).
func (m *MEXC) GetBalanceByMarket(_ context.Context) ([]*MarketBalance, error) {
	var balances []*MarketBalance
	if m.cachedSpotEquity > 0 {
		balances = append(balances, &MarketBalance{
			MarketType: MarketSpot,
			Equity:     m.cachedSpotEquity,
		})
	}
	if m.cachedFuturesEquity > 0 {
		balances = append(balances, &MarketBalance{
			MarketType:      MarketSwap,
			Equity:          m.cachedFuturesEquity,
			AvailableMargin: m.cachedFuturesAvail,
		})
	}
	return balances, nil
}

// GetFundingFees returns funding fee history from MEXC futures.
func (m *MEXC) GetFundingFees(ctx context.Context, symbols []string, since time.Time) ([]*FundingFee, error) {
	params := "page_num=1&page_size=100"
	body, err := m.futuresSignedGET(ctx, "/api/v1/private/account/funding_records", params)
	if err != nil {
		return nil, nil // Futures not available, return empty
	}

	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			ResultList []struct {
				Symbol      string  `json:"symbol"`
				FundingRate float64 `json:"fundingRate"`
				SettleTime  int64   `json:"settleTime"`
				Funding     float64 `json:"funding"`
			} `json:"resultList"`
		} `json:"data"`
	}

	if json.Unmarshal(body, &resp) != nil || !resp.Success {
		return nil, nil
	}

	var fees []*FundingFee
	for _, f := range resp.Data.ResultList {
		ts := time.UnixMilli(f.SettleTime)
		if ts.Before(since) {
			continue
		}
		if f.Funding == 0 {
			continue
		}
		fees = append(fees, &FundingFee{
			Amount:    f.Funding,
			Symbol:    f.Symbol,
			Timestamp: ts,
		})
	}

	return fees, nil
}

// fetchTickerPrice gets the USDT price for an asset from MEXC public ticker.
func (m *MEXC) fetchTickerPrice(ctx context.Context, asset string) float64 {
	asset = strings.ToUpper(asset)
	for _, quote := range []string{"USDT", "USDC"} {
		symbol := asset + quote
		url := m.base.BaseURL + "/api/v3/ticker/price?symbol=" + symbol
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			continue
		}
		body, err := m.base.DoRequest(req)
		if err != nil {
			continue
		}
		var ticker struct {
			Price string `json:"price"`
		}
		if json.Unmarshal(body, &ticker) == nil {
			price, _ := strconv.ParseFloat(ticker.Price, 64)
			if price > 0 {
				return price
			}
		}
	}
	return 0
}

func (m *MEXC) futuresSignedGET(ctx context.Context, path, params string) ([]byte, error) {
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)

	// MEXC futures signature: HMAC-SHA256(apiKey + timestamp + params)
	signPayload := m.base.APIKey + ts + params
	mac := hmac.New(sha256.New, []byte(m.base.APISecret))
	mac.Write([]byte(signPayload))
	signature := hex.EncodeToString(mac.Sum(nil))

	url := "https://contract.mexc.com" + path
	if params != "" {
		url += "?" + params
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("ApiKey", m.base.APIKey)
	req.Header.Set("Request-Time", ts)
	req.Header.Set("Signature", signature)
	req.Header.Set("Content-Type", "application/json")

	return m.base.DoRequest(req)
}

func (m *MEXC) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := m.futuresSignedGET(ctx, "/api/v1/private/position/open_positions", "")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			Symbol        string  `json:"symbol"`
			PositionType  int     `json:"positionType"` // 1=long, 2=short
			HoldVol       float64 `json:"holdVol"`
			OpenAvgPrice  float64 `json:"openAvgPrice"`
			CloseAvgPrice float64 `json:"closeAvgPrice"`
			UnrealizedPnl float64 `json:"unrealised"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	positions := make([]*Position, 0, len(resp.Data))
	for _, p := range resp.Data {
		side := "long"
		if p.PositionType == 2 {
			side = "short"
		}
		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          p.HoldVol,
			EntryPrice:    p.OpenAvgPrice,
			MarkPrice:     p.CloseAvgPrice,
			UnrealizedPnL: p.UnrealizedPnl,
			MarketType:    MarketSwap,
		})
	}

	return positions, nil
}

// GetTrades fetches spot trades from MEXC. The /api/v3/myTrades endpoint
// requires the symbol parameter — there is no "all symbols" variant. We
// enumerate the user's current spot assets and query per ASSET+USDT pair.
// Assets that are pure stablecoins are skipped (they are the quote side).
//
// Trade-off: only symbols where the user currently holds a non-zero balance
// are queried. A user who fully closed a position within the window and
// rolled everything back to USDT will lose that symbol — acceptable for the
// daily snapshot use-case where volume/count is a secondary signal next to
// the equity line.
func (m *MEXC) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	spotBody, err := m.signedGET(ctx, mexcPathAccount, "")
	if err != nil {
		return nil, fmt.Errorf("list assets: %w", err)
	}
	var spotResp struct {
		Balances []struct {
			Asset  string `json:"asset"`
			Free   string `json:"free"`
			Locked string `json:"locked"`
		} `json:"balances"`
	}
	if err := json.Unmarshal(spotBody, &spotResp); err != nil {
		return nil, fmt.Errorf("parse account: %w", err)
	}

	stables := map[string]struct{}{
		"USDT": {}, "USDC": {}, "USD": {}, "BUSD": {}, "DAI": {}, "FDUSD": {},
	}

	var symbols []string
	for _, b := range spotResp.Balances {
		asset := strings.ToUpper(b.Asset)
		if _, isStable := stables[asset]; isStable {
			continue
		}
		free, _ := strconv.ParseFloat(b.Free, 64)
		locked, _ := strconv.ParseFloat(b.Locked, 64)
		if free+locked <= 0 {
			continue
		}
		symbols = append(symbols, asset+"USDT")
	}

	// CONN-006: MEXC rate-limits /api/v3/myTrades to ~20 req/s. A user holding
	// 40+ assets can burn that budget in a single sync call if we fire
	// requests back-to-back. 70ms between requests keeps us at ~14 req/s,
	// well under MEXC's cap and leaving headroom for concurrent connectors.
	const perSymbolGap = 70 * time.Millisecond
	// Distinguish permanent failures (delisted pairs → HTTP 400) from
	// transient ones (429 / 5xx / network). Permanent: skip with debug log.
	// Transient: escalate so the caller retries the sync cleanly, rather
	// than silently truncating the user's trade history.
	var trades []*Trade
	for i, sym := range symbols {
		if i > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(perSymbolGap):
			}
		}
		symTrades, err := m.fetchTradesForSymbol(ctx, sym, start, end)
		if err != nil {
			if isMEXCTransient(err) {
				// Surface the error so the caller knows the window is
				// incomplete. Half-silent truncation used to mask
				// multi-hour outages.
				return nil, fmt.Errorf("mexc: fetch trades for %s: %w", sym, err)
			}
			// Permanent (delisted / 400) — safe to skip, but log so an
			// operator can investigate recurring gaps.
			continue
		}
		trades = append(trades, symTrades...)
	}

	return trades, nil
}

// isMEXCTransient classifies an HTTP error as worth escalating (429 / 5xx /
// network) vs. safe to skip (4xx that mean "symbol does not exist"). The
// signedGET helper wraps errors as "HTTP %d: %s", so we inspect the string.
func isMEXCTransient(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "HTTP 429"),
		strings.Contains(s, "HTTP 500"),
		strings.Contains(s, "HTTP 502"),
		strings.Contains(s, "HTTP 503"),
		strings.Contains(s, "HTTP 504"):
		return true
	}
	// Treat raw network / context errors as transient; only HTTP 4xx
	// responses are classified as permanent.
	if !strings.Contains(s, "HTTP 4") {
		return true
	}
	return false
}

func (m *MEXC) fetchTradesForSymbol(ctx context.Context, symbol string, start, end time.Time) ([]*Trade, error) {
	params := fmt.Sprintf("symbol=%s&startTime=%d&endTime=%d&limit=1000",
		symbol, start.UnixMilli(), end.UnixMilli())

	body, err := m.signedGET(ctx, "/api/v3/myTrades", params)
	if err != nil {
		return nil, err
	}

	var rawTrades []struct {
		ID              int64  `json:"id"`
		Symbol          string `json:"symbol"`
		IsBuyer         bool   `json:"isBuyer"`
		Price           string `json:"price"`
		Qty             string `json:"qty"`
		Commission      string `json:"commission"`
		CommissionAsset string `json:"commissionAsset"`
		Time            int64  `json:"time"`
	}

	if err := json.Unmarshal(body, &rawTrades); err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(rawTrades))
	for _, t := range rawTrades {
		ts := time.UnixMilli(t.Time)
		if ts.Before(start) || ts.After(end) {
			continue
		}

		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Qty, 64)
		fee, _ := strconv.ParseFloat(t.Commission, 64)

		side := "buy"
		if !t.IsBuyer {
			side = "sell"
		}

		trades = append(trades, &Trade{
			ID:          strconv.FormatInt(t.ID, 10),
			Symbol:      t.Symbol,
			Side:        side,
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.CommissionAsset,
			Timestamp:   ts,
			MarketType:  MarketSpot,
		})
	}

	return trades, nil
}

// GetCashflows returns deposits/withdrawals.
// MEXC API supports this but requires specific permissions.
func (m *MEXC) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	// Deposits
	var cashflows []*Cashflow

	depBody, err := m.signedGET(ctx, "/sapi/v1/capital/deposit/hisrec",
		fmt.Sprintf("startTime=%d&limit=100", since.UnixMilli()))
	if err == nil {
		var deposits []struct {
			Amount     string `json:"amount"`
			Coin       string `json:"coin"`
			InsertTime int64  `json:"insertTime"`
			Status     int    `json:"status"`
		}
		if json.Unmarshal(depBody, &deposits) == nil {
			for _, d := range deposits {
				if d.Status != 1 { // 1 = success
					continue
				}
				amount, _ := strconv.ParseFloat(d.Amount, 64)
				cashflows = append(cashflows, &Cashflow{
					Amount:    amount,
					Currency:  d.Coin,
					Timestamp: time.UnixMilli(d.InsertTime),
				})
			}
		}
	}

	// Withdrawals
	wdBody, err := m.signedGET(ctx, "/sapi/v1/capital/withdraw/history",
		fmt.Sprintf("startTime=%d&limit=100", since.UnixMilli()))
	if err == nil {
		var withdrawals []struct {
			Amount    string `json:"amount"`
			Coin      string `json:"coin"`
			ApplyTime string `json:"applyTime"`
			Status    int    `json:"status"`
		}
		if json.Unmarshal(wdBody, &withdrawals) == nil {
			for _, w := range withdrawals {
				if w.Status != 6 { // 6 = completed
					continue
				}
				amount, _ := strconv.ParseFloat(w.Amount, 64)
				ts, _ := time.Parse("2006-01-02 15:04:05", w.ApplyTime)
				cashflows = append(cashflows, &Cashflow{
					Amount:    -amount,
					Currency:  w.Coin,
					Timestamp: ts,
				})
			}
		}
	}

	return cashflows, nil
}

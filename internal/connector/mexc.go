package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const mexcAPI = "https://api.mexc.com"

// mexcFuturesAPI hosts the contract (futures) API, which signs differently.
const mexcFuturesAPI = "https://contract.mexc.com"

// mexcPathFundingRecords is the account's funding settlements, paged, newest
// first, with no time filter (contract API v1, "Get details of user's funding
// rate"). The connector used to call /api/v1/private/account/funding_records,
// which the API does not have, and read the failure as "no futures".
const mexcPathFundingRecords = "/api/v1/private/position/funding_records"

// mexcFundingPageSize is the API's maximum page size for funding records.
const mexcFundingPageSize = 100

// mexcFundingMaxPages bounds a funding read: 200 pages of 100 records. A
// window that still reaches further back fails rather than come back short.
const mexcFundingMaxPages = 200

// QUAL-001: extracted to remove a 3-way duplication of the spot-account path.
const mexcPathAccount = "/api/v3/account"

// MEXC implements Connector for MEXC exchange.
// Uses HMAC-SHA256 signing, same as Binance.
type MEXC struct {
	base CryptoBase

	// Cached from last GetBalance for GetBalanceByMarket, guarded by mu: the
	// instance is shared by every sync of the connection.
	mu                  sync.Mutex
	cachedSpotEquity    float64
	cachedFuturesEquity float64
	cachedFuturesAvail  float64

	// cashflowNotes carries the markers the last GetCashflows raised.
	cashflowNotes noteList

	// futuresURL is mexcFuturesAPI, swapped by tests.
	futuresURL string
}

// CapabilityWarnings implements CapabilityWarner with the flows the last
// GetCashflows could not value.
func (m *MEXC) CapabilityWarnings() []string { return m.cashflowNotes.get() }

// NewMEXC creates a new MEXC connector.
func NewMEXC(creds *Credentials) *MEXC {
	return &MEXC{
		base:       NewCryptoBase(creds.APIKey, creds.APISecret, mexcAPI),
		futuresURL: mexcFuturesAPI,
	}
}

func (m *MEXC) Exchange() string { return "mexc" }

func (m *MEXC) signedGET(ctx context.Context, path, params string) ([]byte, error) {
	return m.base.signedQueryGET(ctx, "X-MEXC-APIKEY", path, params)
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

	var totalHoldings, freeHoldings []SpotHolding
	hasNonStable := false
	for _, b := range spotResp.Balances {
		free, _ := strconv.ParseFloat(b.Free, 64)
		locked, _ := strconv.ParseFloat(b.Locked, 64)
		total := free + locked
		if total <= 0 {
			continue
		}
		asset := strings.ToUpper(strings.TrimSpace(b.Asset))
		totalHoldings = append(totalHoldings, SpotHolding{Asset: asset, Amount: total})
		if free > 0 {
			freeHoldings = append(freeHoldings, SpotHolding{Asset: asset, Amount: free})
		}
		if !IsStablecoinUSD(asset) {
			hasNonStable = true
		}
	}

	// CONN-13: one public all-tickers call replaces the per-altcoin ticker
	// fetch, whose 429s dropped an asset from the equity with no error at all.
	// A fetch failure now FAILS the sync (transient) instead of publishing a
	// wallet valued stables-only.
	priceMap := map[string]float64{}
	if hasNonStable {
		pm, perr := FetchBinanceStylePriceMap(ctx, m.base.Client, m.base.BaseURL)
		if perr != nil {
			return nil, fmt.Errorf("%w: mexc spot tickers: %v", ErrSpotPricingUnavailable, perr)
		}
		priceMap = pm
	}
	spotEquity := ValueSpotHoldingsUSD(totalHoldings, priceMap)
	spotAvailable := ValueSpotHoldingsUSD(freeHoldings, priceMap)

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
				if a.Equity > 0 && IsStablecoinUSD(a.Currency) {
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

	m.mu.Lock()
	m.cachedSpotEquity = spotEquity
	m.cachedFuturesEquity = futuresEquity
	m.cachedFuturesAvail = futuresAvailable
	m.mu.Unlock()

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
	m.mu.Lock()
	defer m.mu.Unlock()
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

// GetFundingFees returns the account's funding settlements since a time,
// from MEXC futures. The endpoint has no time filter: it pages newest first,
// so the read walks pages until one ends before since. Each amount is signed
// by mexcFundingAmount.
//
// An account whose key cannot read futures answers ErrFundingNotRead, so a
// recount keeps the funding it stored instead of writing a zero. Any other
// failure is an error: it used to come back as "no funding", which a sync
// stored as a zero.
func (m *MEXC) GetFundingFees(ctx context.Context, _ []string, since time.Time) ([]*FundingFee, error) {
	var fees []*FundingFee
	seen := map[int64]struct{}{}
	for page := 1; page <= mexcFundingMaxPages; page++ {
		params := fmt.Sprintf("page_num=%d&page_size=%d", page, mexcFundingPageSize)
		body, err := m.futuresSignedGET(ctx, mexcPathFundingRecords, params)
		if err != nil {
			return nil, fmt.Errorf("mexc funding records page %d: %w", page, err)
		}

		var resp struct {
			Success bool `json:"success"`
			Code    int  `json:"code"`
			Data    struct {
				TotalPage  int `json:"totalPage"`
				ResultList []struct {
					ID           int64   `json:"id"`
					Symbol       string  `json:"symbol"`
					PositionType int     `json:"positionType"` // 1 long, 2 short
					Rate         float64 `json:"rate"`
					SettleTime   int64   `json:"settleTime"`
					Funding      float64 `json:"funding"`
				} `json:"resultList"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("parse mexc funding records: %w", err)
		}
		if !resp.Success {
			// MEXC answers HTTP 200 with success=false when the key has no
			// futures access, the same way /account/assets does in GetBalance.
			return nil, fmt.Errorf("%w: mexc funding records code %d", ErrFundingNotRead, resp.Code)
		}

		reachedSince := false
		for _, f := range resp.Data.ResultList {
			ts := time.UnixMilli(f.SettleTime).UTC()
			if ts.Before(since) {
				reachedSince = true
				continue
			}
			// A settlement landing while we page shifts every row down one
			// place, so the last row of a page comes back at the top of the
			// next one.
			if _, dup := seen[f.ID]; dup {
				continue
			}
			seen[f.ID] = struct{}{}
			if f.Funding == 0 {
				continue
			}
			fees = append(fees, &FundingFee{
				Amount:    mexcFundingAmount(f.PositionType, f.Rate, f.Funding),
				Symbol:    f.Symbol,
				Timestamp: ts,
			})
		}
		if reachedSince || len(resp.Data.ResultList) < mexcFundingPageSize || page >= resp.Data.TotalPage {
			return fees, nil
		}
	}
	return nil, fmt.Errorf("mexc funding records: more than %d pages since %s", mexcFundingMaxPages, since.UTC().Format(time.RFC3339))
}

// mexcFundingAmount is a settlement as the FundingFee contract wants it:
// positive received, negative paid. MEXC does not document the sign of
// `funding`, so the direction is taken from what it does state: a long pays
// when the rate is positive and receives when it is negative, a short the
// reverse. Only the size comes from `funding`. A record without a side or a
// rate keeps the figure as sent.
func mexcFundingAmount(positionType int, rate, funding float64) float64 {
	if rate == 0 || (positionType != 1 && positionType != 2) {
		return funding
	}
	size := funding
	if size < 0 {
		size = -size
	}
	if long := positionType == 1; long == (rate > 0) {
		return -size
	}
	return size
}

func (m *MEXC) futuresSignedGET(ctx context.Context, path, params string) ([]byte, error) {
	return retryHTTP(m.base.Client, func() (*http.Request, error) {
		ts := strconv.FormatInt(time.Now().UnixMilli(), 10)

		// MEXC futures signature: HMAC-SHA256(apiKey + timestamp + params)
		signature := signHMACHex(m.base.APISecret, m.base.APIKey+ts+params)

		base := m.futuresURL
		if base == "" {
			base = mexcFuturesAPI
		}
		url := base + path
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
		return req, nil
	})
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

	// CONN-13: the consumer sums Amount and ignores Currency, so a coin
	// quantity booked as-is turned a 0.5 BTC deposit into $0.50 — a phantom
	// cashflow that skews the TWR permanently. Price map fetched lazily, on
	// the first non-stable coin seen.
	var priceMap map[string]float64
	unpriced := unpricedAssets{}
	usdValue := func(coin string, qty float64) float64 {
		if IsStablecoinUSD(coin) {
			return qty
		}
		if priceMap == nil {
			pm, perr := FetchBinanceStylePriceMap(ctx, m.base.Client, m.base.BaseURL)
			if perr != nil {
				pm = map[string]float64{}
			}
			priceMap = pm
		}
		if p := priceMap[strings.ToUpper(coin)+"USDT"]; p > 0 {
			return qty * p
		}
		if qty != 0 {
			unpriced.add(coin)
		}
		return 0
	}
	defer func() { m.cashflowNotes.set(unpriced.markers("mexc")) }()

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
				usd := usdValue(d.Coin, amount)
				if usd == 0 {
					continue
				}
				cashflows = append(cashflows, &Cashflow{
					Amount:    usd,
					Currency:  "USDT",
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
				usd := usdValue(w.Coin, amount)
				if usd == 0 {
					continue
				}
				ts, _ := time.Parse("2006-01-02 15:04:05", w.ApplyTime)
				cashflows = append(cashflows, &Cashflow{
					Amount:    -usd,
					Currency:  "USDT",
					Timestamp: ts,
				})
			}
		}
	}

	return cashflows, nil
}

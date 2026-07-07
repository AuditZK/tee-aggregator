package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	binanceSpotAPI    = "https://api.binance.com"
	binanceFuturesAPI = "https://fapi.binance.com"

	// QUAL-001: extracted to remove duplication across signed-request sites.
	binancePathAccount = "/api/v3/account"
)

// Binance implements Connector for Binance exchange.
type Binance struct {
	apiKey    string
	apiSecret string
	client    *http.Client
}

// NewBinance creates a new Binance connector
func NewBinance(creds *Credentials) *Binance {
	return &Binance{
		apiKey:    creds.APIKey,
		apiSecret: creds.APISecret,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

// NewBinanceWithClient creates a Binance connector with a custom HTTP client.
// Used to inject a proxy-configured transport for geo-restricted regions.
func NewBinanceWithClient(creds *Credentials, client *http.Client) *Binance {
	if client.Timeout == 0 {
		client.Timeout = 30 * time.Second
	}
	return &Binance{
		apiKey:    creds.APIKey,
		apiSecret: creds.APISecret,
		client:    client,
	}
}

func (b *Binance) Exchange() string {
	return "binance"
}

func (b *Binance) sign(params url.Values) string {
	return signHMACHex(b.apiSecret, params.Encode())
}

func (b *Binance) doRequest(ctx context.Context, method, baseURL, path string, params url.Values, signed bool) ([]byte, error) {
	return retryHTTP(b.client, func() (*http.Request, error) {
		if signed {
			// Del before re-signing: on a retry attempt params still carries
			// the previous attempt's signature, which must not be part of the
			// next signed payload.
			params.Del("signature")
			params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
			params.Set("signature", b.sign(params))
		}

		reqURL := baseURL + path
		if len(params) > 0 {
			reqURL += "?" + params.Encode()
		}

		req, err := http.NewRequestWithContext(ctx, method, reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-MBX-APIKEY", b.apiKey)
		return req, nil
	})
}

func (b *Binance) TestConnection(ctx context.Context) error {
	params := url.Values{}
	_, err := b.doRequest(ctx, "GET", binanceSpotAPI, binancePathAccount, params, true)
	return err
}

func (b *Binance) GetBalance(ctx context.Context) (*Balance, error) {
	// Get spot balance
	spotBalance, err := b.getSpotBalance(ctx)
	if err != nil {
		return nil, fmt.Errorf("spot balance: %w", err)
	}

	// Get futures balance (ignore error - account may not have futures)
	futuresBalance, _ := b.getFuturesBalance(ctx)

	total := spotBalance
	if futuresBalance != nil {
		total.Equity += futuresBalance.Equity
		total.UnrealizedPnL += futuresBalance.UnrealizedPnL
	}

	return total, nil
}

func (b *Binance) getSpotBalance(ctx context.Context) (*Balance, error) {
	params := url.Values{}
	body, err := b.doRequest(ctx, "GET", binanceSpotAPI, binancePathAccount, params, true)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Balances []struct {
			Asset  string `json:"asset"`
			Free   string `json:"free"`
			Locked string `json:"locked"`
		} `json:"balances"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	// Collect every non-zero holding, not just stablecoins. The old code summed
	// only USDT/BUSD/USD, so any account holding BTC/ETH/altcoins reported an
	// equity of ~0 (CONN-VALUE-001). Stablecoins are valued 1:1 below; all
	// other assets are priced from the public ticker map.
	var holdings []SpotHolding
	var stableAvailable float64 // free stablecoins = liquid USD available
	hasNonStable := false
	for _, bal := range resp.Balances {
		free, _ := strconv.ParseFloat(bal.Free, 64)
		locked, _ := strconv.ParseFloat(bal.Locked, 64)
		total := free + locked
		if total <= 0 {
			continue
		}
		holdings = append(holdings, SpotHolding{Asset: bal.Asset, Amount: total})
		if IsStablecoinUSD(bal.Asset) {
			stableAvailable += free
		} else {
			hasNonStable = true
		}
	}

	// Only pay for the ticker call when there is a non-stable asset to price. A
	// pure-USDT account skips the round-trip entirely. If the fetch fails we
	// fall back to an empty map: stablecoins still value correctly and the sync
	// is never failed over a pricing hiccup.
	priceMap := map[string]float64{}
	if hasNonStable {
		if pm, perr := FetchBinanceStylePriceMap(ctx, b.client, binanceSpotAPI); perr == nil {
			priceMap = pm
		}
	}

	return &Balance{
		Available: stableAvailable,
		Equity:    ValueSpotHoldingsUSD(holdings, priceMap),
		Currency:  "USDT",
	}, nil
}

func (b *Binance) getFuturesBalance(ctx context.Context) (*Balance, error) {
	params := url.Values{}
	body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v2/account", params, true)
	if err != nil {
		return nil, err
	}

	// totalMarginBalance already folds in unrealized PnL and every margin asset
	// in multi-asset mode, so it is the true USDⓈ-M futures equity — strictly
	// better than the previous "USDT wallet balance only" read, which dropped
	// non-USDT collateral. It never overlaps the spot wallet, so summing the two
	// in GetBalance does not double-count.
	var resp struct {
		TotalMarginBalance    string `json:"totalMarginBalance"`
		TotalUnrealizedProfit string `json:"totalUnrealizedProfit"`
		AvailableBalance      string `json:"availableBalance"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	equity, _ := strconv.ParseFloat(resp.TotalMarginBalance, 64)
	unrealized, _ := strconv.ParseFloat(resp.TotalUnrealizedProfit, 64)
	available, _ := strconv.ParseFloat(resp.AvailableBalance, 64)

	return &Balance{
		Available:     available,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USDT",
	}, nil
}

func (b *Binance) GetPositions(ctx context.Context) ([]*Position, error) {
	params := url.Values{}
	body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v2/positionRisk", params, true)
	if err != nil {
		return nil, err
	}

	var resp []struct {
		Symbol           string `json:"symbol"`
		PositionAmt      string `json:"positionAmt"`
		EntryPrice       string `json:"entryPrice"`
		MarkPrice        string `json:"markPrice"`
		UnRealizedProfit string `json:"unRealizedProfit"`
		PositionSide     string `json:"positionSide"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var positions []*Position
	for _, p := range resp {
		size, _ := strconv.ParseFloat(p.PositionAmt, 64)
		if size == 0 {
			continue
		}

		entry, _ := strconv.ParseFloat(p.EntryPrice, 64)
		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		unrealized, _ := strconv.ParseFloat(p.UnRealizedProfit, 64)

		side := "long"
		if size < 0 {
			side = "short"
			size = -size
		}

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          size,
			EntryPrice:    entry,
			MarkPrice:     mark,
			UnrealizedPnL: unrealized,
			MarketType:    "swap",
		})
	}

	return positions, nil
}

func (b *Binance) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	var allTrades []*Trade

	// Spot trades
	spotTrades, err := b.getSpotTrades(ctx, start, end)
	if err == nil {
		allTrades = append(allTrades, spotTrades...)
	}

	// Futures trades
	futuresTrades, err := b.getFuturesTrades(ctx, start, end)
	if err == nil {
		allTrades = append(allTrades, futuresTrades...)
	}

	return allTrades, nil
}

func (b *Binance) getSpotTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	// Get active trading pairs first
	params := url.Values{}
	body, err := b.doRequest(ctx, "GET", binanceSpotAPI, binancePathAccount, params, true)
	if err != nil {
		return nil, err
	}

	var account struct {
		Balances []struct {
			Asset string `json:"asset"`
			Free  string `json:"free"`
		} `json:"balances"`
	}
	json.Unmarshal(body, &account)

	// Get trades for USDT pairs of assets with balance
	var trades []*Trade
	for _, bal := range account.Balances {
		free, _ := strconv.ParseFloat(bal.Free, 64)
		if free < 0.001 || bal.Asset == "USDT" {
			continue
		}

		symbol := bal.Asset + "USDT"
		symbolTrades, err := b.getSpotTradesForSymbol(ctx, symbol, start, end)
		if err == nil {
			trades = append(trades, symbolTrades...)
		}
	}

	return trades, nil
}

func (b *Binance) getSpotTradesForSymbol(ctx context.Context, symbol string, start, end time.Time) ([]*Trade, error) {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("startTime", strconv.FormatInt(start.UnixMilli(), 10))
	params.Set("endTime", strconv.FormatInt(end.UnixMilli(), 10))
	params.Set("limit", "1000")

	body, err := b.doRequest(ctx, "GET", binanceSpotAPI, "/api/v3/myTrades", params, true)
	if err != nil {
		return nil, err
	}

	var resp []struct {
		ID              int64  `json:"id"`
		Symbol          string `json:"symbol"`
		Price           string `json:"price"`
		Qty             string `json:"qty"`
		Commission      string `json:"commission"`
		CommissionAsset string `json:"commissionAsset"`
		Time            int64  `json:"time"`
		IsBuyer         bool   `json:"isBuyer"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, t := range resp {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Qty, 64)
		fee, _ := strconv.ParseFloat(t.Commission, 64)

		side := "sell"
		if t.IsBuyer {
			side = "buy"
		}

		trades = append(trades, &Trade{
			ID:          strconv.FormatInt(t.ID, 10),
			Symbol:      t.Symbol,
			Side:        side,
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.CommissionAsset,
			Timestamp:   time.UnixMilli(t.Time),
			MarketType:  "spot",
		})
	}

	return trades, nil
}

func (b *Binance) getFuturesTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	params := url.Values{}
	params.Set("startTime", strconv.FormatInt(start.UnixMilli(), 10))
	params.Set("endTime", strconv.FormatInt(end.UnixMilli(), 10))
	params.Set("limit", "1000")

	body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v1/userTrades", params, true)
	if err != nil {
		return nil, err
	}

	var resp []struct {
		ID              int64  `json:"id"`
		Symbol          string `json:"symbol"`
		Price           string `json:"price"`
		Qty             string `json:"qty"`
		Commission      string `json:"commission"`
		CommissionAsset string `json:"commissionAsset"`
		Time            int64  `json:"time"`
		Side            string `json:"side"`
		RealizedPnl     string `json:"realizedPnl"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, t := range resp {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Qty, 64)
		fee, _ := strconv.ParseFloat(t.Commission, 64)
		pnl, _ := strconv.ParseFloat(t.RealizedPnl, 64)

		trades = append(trades, &Trade{
			ID:          strconv.FormatInt(t.ID, 10),
			Symbol:      t.Symbol,
			Side:        t.Side,
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.CommissionAsset,
			RealizedPnL: pnl,
			Timestamp:   time.UnixMilli(t.Time),
			MarketType:  "swap",
		})
	}

	return trades, nil
}

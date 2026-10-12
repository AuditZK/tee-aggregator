package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	demo bool
}

// NewBingX creates a new BingX connector. A demo connection reads the VST
// perpetual account, which BingX serves from its own host for the same key.
func NewBingX(creds *Credentials) *BingX {
	host := bingxAPI
	if creds.Demo {
		host = bingxVSTAPI
	}
	return &BingX{
		base: NewCryptoBase(creds.APIKey, creds.APISecret, host),
		demo: creds.Demo,
	}
}

// DetectIsPaper reports a demo connection: every live wallet empty while the
// demo perpetual account trades. Decided at connect and stored, so a sync
// never switches a series between the two accounts.
func (b *BingX) DetectIsPaper(ctx context.Context) (bool, error) {
	if b.demo {
		return true, nil
	}
	live, err := b.liveHoldsFunds(ctx)
	if err != nil || live {
		return false, err
	}
	return b.demoTrades(ctx)
}

func (b *BingX) liveHoldsFunds(ctx context.Context) (bool, error) {
	body, err := b.signedGET(ctx, "/openApi/account/v1/allAccountBalance", "")
	if err != nil {
		return false, fmt.Errorf("read bingx account overview: %w", err)
	}
	var overview struct {
		Code int `json:"code"`
		Data []struct {
			USDTBalance string `json:"usdtBalance"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &overview); err != nil {
		return false, fmt.Errorf("parse bingx account overview: %w", err)
	}
	if overview.Code != 0 {
		return false, fmt.Errorf("read bingx account overview: bingx code %d: %s", overview.Code, vendorErrorDetail(string(body)))
	}
	for _, a := range overview.Data {
		if !isZeroAmount(a.USDTBalance) {
			return true, nil
		}
	}

	// The overview leaves the fund wallet out.
	body, err = b.signedGET(ctx, "/openApi/fund/v1/account/balance", "")
	if err != nil {
		return false, fmt.Errorf("read bingx fund wallet: %w", err)
	}
	var fund struct {
		Code int `json:"code"`
		Data struct {
			Assets []struct {
				Free   string `json:"free"`
				Locked string `json:"locked"`
			} `json:"assets"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &fund); err != nil {
		return false, fmt.Errorf("parse bingx fund wallet: %w", err)
	}
	if fund.Code != 0 {
		return false, fmt.Errorf("read bingx fund wallet: bingx code %d: %s", fund.Code, vendorErrorDetail(string(body)))
	}
	for _, a := range fund.Data.Assets {
		if !isZeroAmount(a.Free) || !isZeroAmount(a.Locked) {
			return true, nil
		}
	}
	return false, nil
}

// demoTrades reports activity on the demo account beyond the grant BingX
// credits every user: an open position or any income other than a transfer.
func (b *BingX) demoTrades(ctx context.Context) (bool, error) {
	demo := b.base
	if demo.BaseURL == bingxAPI {
		demo.BaseURL = bingxVSTAPI
	}
	body, err := demo.signedQueryGET(ctx, "X-BX-APIKEY", "/openApi/swap/v2/user/income", "")
	if err != nil {
		return false, fmt.Errorf("read bingx demo income: %w", err)
	}
	var income struct {
		Code int `json:"code"`
		Data []struct {
			IncomeType string `json:"incomeType"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &income); err != nil {
		return false, fmt.Errorf("parse bingx demo income: %w", err)
	}
	if income.Code != 0 {
		return false, fmt.Errorf("read bingx demo income: bingx code %d: %s", income.Code, vendorErrorDetail(string(body)))
	}
	for _, row := range income.Data {
		if !strings.EqualFold(row.IncomeType, "TRANSFER") {
			return true, nil
		}
	}
	return false, nil
}

func (b *BingX) Exchange() string { return "bingx" }

func (b *BingX) signedGET(ctx context.Context, path, params string) ([]byte, error) {
	return b.base.signedQueryGET(ctx, "X-BX-APIKEY", path, params)
}

func (b *BingX) TestConnection(ctx context.Context) error {
	_, err := b.signedGET(ctx, "/openApi/swap/v2/user/balance", "")
	return err
}

// parseBingXAmount refuses to swallow a malformed financial value: a dropped
// ParseFloat error reads as a zero balance, indistinguishable from a real
// empty account. An absent field stays 0 — BingX omits wallets the account
// never opened.
func parseBingXAmount(field, raw string) (float64, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("parse swap %s: %w", field, err)
	}
	return f, nil
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

	// CONN-15a: BingX answers business errors with HTTP 200 and code != 0,
	// leaving Data empty. Without this the empty strings below parsed to 0
	// and TestConnection accepted invalid credentials.
	if resp.Code != 0 {
		return nil, fmt.Errorf("swap balance: bingx code %d: %s", resp.Code, vendorErrorDetail(string(body)))
	}

	equity, err := parseBingXAmount("equity", resp.Data.Balance.Equity)
	if err != nil {
		return nil, err
	}
	available, err := parseBingXAmount("availableMargin", resp.Data.Balance.Available)
	if err != nil {
		return nil, err
	}
	unrealized, err := parseBingXAmount("unrealizedProfit", resp.Data.Balance.UnrealizedPnL)
	if err != nil {
		return nil, err
	}

	// The demo host has no spot account.
	if b.demo {
		return &Balance{Equity: equity, Available: available, UnrealizedPnL: unrealized, Currency: "USDT"}, nil
	}

	// Spot balance (best effort on the fetch; strict on the valuation).
	// CONN-12: the old loop summed only USDT/USDC/USD and silently dropped
	// every other holding, so a spot bag of BTC counted as nothing.
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
			var holdings []SpotHolding
			hasNonStable := false
			for _, bal := range spotResp.Data.Balances {
				free, _ := strconv.ParseFloat(bal.Free, 64)
				locked, _ := strconv.ParseFloat(bal.Locked, 64)
				total := free + locked
				if total <= 0 {
					continue
				}
				holdings = append(holdings, SpotHolding{Asset: bal.Asset, Amount: total})
				if IsStablecoinUSD(bal.Asset) {
					available += free
				} else {
					hasNonStable = true
				}
			}
			priceMap := map[string]float64{}
			if hasNonStable {
				pm, perr := b.fetchPriceMap(ctx)
				if perr != nil {
					return nil, fmt.Errorf("%w: bingx spot tickers: %v", ErrSpotPricingUnavailable, perr)
				}
				priceMap = pm
			}
			equity += ValueSpotHoldingsUSD(holdings, priceMap)
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

// bingxOrderWindow is the widest span allOrders answers for in one call.
const bingxOrderWindow = 7 * 24 * time.Hour

func (b *BingX) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	var trades []*Trade
	for from := start; from.Before(end); from = from.Add(bingxOrderWindow) {
		to := from.Add(bingxOrderWindow)
		if to.After(end) {
			to = end
		}
		page, err := b.filledOrders(ctx, from, to)
		if err != nil {
			return nil, err
		}
		trades = append(trades, page...)
	}
	return trades, nil
}

// filledOrders reads one window of perpetual orders. The path used to be
// /user/historyOrders, which BingX does not serve: its refusal parsed as an
// empty list and every BingX day recorded zero trades.
func (b *BingX) filledOrders(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	params := fmt.Sprintf("startTime=%d&endTime=%d&limit=1000",
		start.UnixMilli(), end.UnixMilli())

	body, err := b.signedGET(ctx, "/openApi/swap/v2/trade/allOrders", params)
	if err != nil {
		return nil, fmt.Errorf("read bingx orders: %w", err)
	}

	var resp struct {
		Code int `json:"code"`
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
		return nil, fmt.Errorf("parse bingx orders: %w", err)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("read bingx orders: bingx code %d: %s", resp.Code, vendorErrorDetail(string(body)))
	}

	trades := make([]*Trade, 0, len(resp.Data.Orders))
	for _, t := range resp.Data.Orders {
		price, _ := strconv.ParseFloat(t.AvgPrice, 64)
		qty, _ := strconv.ParseFloat(t.Volume, 64)
		if qty == 0 {
			continue // skip unfilled orders
		}
		fee, _ := strconv.ParseFloat(t.Fee, 64)
		// The commission's sign differs between BingX endpoints, and its
		// perpetuals pay no maker rebate; Trade.Fee books a cost as positive.
		fee = math.Abs(fee)
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

// fetchPriceMap loads every spot pair's last price in one public call and
// keys it Binance-style (BTC-USDT -> BTCUSDT) for ValueSpotHoldingsUSD.
func (b *BingX) fetchPriceMap(ctx context.Context) (map[string]float64, error) {
	body, err := retryHTTP(b.base.Client, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "GET", b.base.BaseURL+"/openApi/spot/v1/ticker/24hr", nil)
	})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Data []struct {
			Symbol    string          `json:"symbol"`
			LastPrice json.RawMessage `json:"lastPrice"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	prices := make(map[string]float64, len(resp.Data))
	for _, t := range resp.Data {
		if p := ParseLooseNumber(t.LastPrice); p > 0 {
			prices[strings.ToUpper(strings.ReplaceAll(t.Symbol, "-", ""))] = p
		}
	}
	return prices, nil
}

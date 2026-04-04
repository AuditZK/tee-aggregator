package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	ccxt "github.com/ccxt/ccxt/go/v4"
)

var ccxtStablecoins = []string{"USDT", "USDC", "USD", "BUSD", "DAI", "FDUSD"}

// CCXTConnector wraps a CCXT dynamic exchange instance for broad exchange coverage.
type CCXTConnector struct {
	exchange string
	core     ccxt.ICoreExchange

	mu      sync.RWMutex
	markets map[string]ccxt.MarketInterface
	loaded  bool
}

// NewCCXT creates a CCXT-backed connector for a given exchange id.
func NewCCXT(exchange string, creds *Credentials) (Connector, error) {
	exchangeID := strings.ToLower(strings.TrimSpace(exchange))

	cfg := map[string]interface{}{
		"apiKey":          creds.APIKey,
		"secret":          creds.APISecret,
		"enableRateLimit": true,
		"options": map[string]interface{}{
			"defaultType": "swap",
			"recvWindow":  10000,
		},
	}
	if p := strings.TrimSpace(creds.Passphrase); p != "" {
		cfg["password"] = p
	}

	if proxy := strings.TrimSpace(os.Getenv("EXCHANGE_HTTP_PROXY")); proxy != "" {
		if shouldUseProxy(exchangeID, os.Getenv("PROXY_EXCHANGES")) {
			cfg["httpProxy"] = proxy
		}
	}

	core, ok := ccxt.DynamicallyCreateInstance(exchangeID, cfg)
	if !ok || core == nil {
		return nil, fmt.Errorf("ccxt exchange %q is not available", exchangeID)
	}

	return &CCXTConnector{
		exchange: exchangeID,
		core:     core,
		markets:  make(map[string]ccxt.MarketInterface),
	}, nil
}

func (c *CCXTConnector) Exchange() string {
	return c.exchange
}

// DetectIsPaper mirrors TS behavior for CCXT connectors:
// authenticated CCXT API usage is treated as live (non-demo) by default.
func (c *CCXTConnector) DetectIsPaper(_ context.Context) (bool, error) {
	return false, nil
}

// FetchKYCLevel returns normalized KYC level for exchanges that expose it.
// TS parity support: bybit, okx, kucoin.
func (c *CCXTConnector) FetchKYCLevel(ctx context.Context) (string, error) {
	switch c.exchange {
	case "bybit":
		return c.fetchBybitKYCLevel(ctx)
	case "okx":
		return c.fetchOKXKYCLevel(ctx)
	case "kucoin":
		return c.fetchKucoinKYCLevel(ctx)
	default:
		return "", nil
	}
}

func (c *CCXTConnector) fetchBybitKYCLevel(ctx context.Context) (string, error) {
	res, err := c.call(ctx, func() <-chan interface{} {
		return c.core.CallInternal("privateGetV5UserQueryApi")
	})
	if err != nil {
		return "", err
	}

	root, ok := mapFromAny(res)
	if !ok {
		return "", nil
	}
	result, ok := mapFromAny(root["result"])
	if !ok {
		return "", nil
	}

	switch strings.TrimSpace(firstStringFromMap(result, "kycLevel")) {
	case "LEVEL_1":
		return "basic", nil
	case "LEVEL_2":
		return "advanced", nil
	case "LEVEL_DEFAULT":
		return "none", nil
	default:
		return "none", nil
	}
}

func (c *CCXTConnector) fetchOKXKYCLevel(ctx context.Context) (string, error) {
	res, err := c.call(ctx, func() <-chan interface{} {
		return c.core.CallInternal("privateGetAccountConfig")
	})
	if err != nil {
		return "", err
	}

	root, ok := mapFromAny(res)
	if !ok {
		return "", nil
	}
	data, ok := sliceFromAny(root["data"])
	if !ok || len(data) == 0 {
		return "", nil
	}
	first, ok := mapFromAny(data[0])
	if !ok {
		return "", nil
	}

	switch strings.TrimSpace(firstStringFromMap(first, "kycLv")) {
	case "1":
		return "basic", nil
	case "2":
		return "intermediate", nil
	case "3":
		return "advanced", nil
	case "0":
		return "none", nil
	default:
		return "none", nil
	}
}

func (c *CCXTConnector) fetchKucoinKYCLevel(ctx context.Context) (string, error) {
	res, err := c.call(ctx, func() <-chan interface{} {
		return c.core.CallInternal("privateGetUserApiKey")
	})
	if err != nil {
		return "", err
	}

	root, ok := mapFromAny(res)
	if !ok {
		return "", nil
	}
	data, ok := mapFromAny(root["data"])
	if !ok {
		return "", nil
	}
	status := firstNumericFromMap(data, "kycStatus")

	switch int(status) {
	case 1:
		return "basic", nil
	case 2:
		return "advanced", nil
	case 0:
		return "none", nil
	default:
		return "none", nil
	}
}

func (c *CCXTConnector) TestConnection(ctx context.Context) error {
	if err := c.ensureMarkets(ctx); err != nil {
		return err
	}
	_, err := c.call(ctx, func() <-chan interface{} {
		return c.core.FetchBalance(map[string]interface{}{})
	})
	return err
}

func (c *CCXTConnector) GetBalance(ctx context.Context) (*Balance, error) {
	if err := c.ensureMarkets(ctx); err != nil {
		return nil, err
	}

	res, err := c.call(ctx, func() <-chan interface{} {
		return c.core.FetchBalance(map[string]interface{}{})
	})
	if err != nil {
		return nil, err
	}

	balances := ccxt.NewBalances(res)
	available, equity, currency := extractStablecoinBalance(balances)
	if equity == 0 {
		available, equity = extractAggregateBalance(balances)
		if currency == "" {
			currency = "USD"
		}
	}

	unrealized := 0.0
	positions, posErr := c.fetchPositions(ctx)
	if posErr == nil {
		for _, p := range positions {
			unrealized += p.UnrealizedPnL
		}
	}
	if equity == 0 && available > 0 {
		equity = available
	}
	if currency == "" {
		currency = "USD"
	}

	return &Balance{
		Available:     available,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      currency,
	}, nil
}

func (c *CCXTConnector) GetPositions(ctx context.Context) ([]*Position, error) {
	return c.fetchPositions(ctx)
}

func (c *CCXTConnector) fetchPositions(ctx context.Context) ([]*Position, error) {
	if err := c.ensureMarkets(ctx); err != nil {
		return nil, err
	}

	res, err := c.call(ctx, func() <-chan interface{} {
		return c.core.FetchPositions(nil, map[string]interface{}{})
	})
	if err != nil {
		return nil, err
	}

	raw := ccxt.NewPositionArray(res)
	out := make([]*Position, 0, len(raw))
	for _, p := range raw {
		contracts := ptrFloat(p.Contracts)
		size := math.Abs(contracts)
		mark := ptrFloat(p.MarkPrice)
		if mark == 0 {
			mark = ptrFloat(p.LastPrice)
		}
		if size == 0 {
			notional := math.Abs(ptrFloat(p.Notional))
			if notional > 0 && mark > 0 {
				size = notional / mark
			}
		}
		if size == 0 {
			continue
		}

		symbol := ptrString(p.Symbol)
		side := strings.ToLower(strings.TrimSpace(ptrString(p.Side)))
		if side == "" {
			side = "long"
		}

		out = append(out, &Position{
			Symbol:        symbol,
			Side:          side,
			Size:          size,
			EntryPrice:    ptrFloat(p.EntryPrice),
			MarkPrice:     mark,
			UnrealizedPnL: ptrFloat(p.UnrealizedPnl),
			MarketType:    c.marketTypeForSymbol(symbol, MarketSwap),
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Symbol < out[j].Symbol
	})

	return out, nil
}

// GetCashflows returns deposits and withdrawals since the given date.
// Uses CCXT fetchDeposits + fetchWithdrawals.
func (c *CCXTConnector) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	if err := c.ensureMarkets(ctx); err != nil {
		return nil, err
	}

	var cashflows []*Cashflow

	// Fetch deposits
	depositsRes, err := c.call(ctx, func() <-chan interface{} {
		return c.core.FetchDeposits(nil, since.UnixMilli(), nil, map[string]interface{}{})
	})
	if err == nil {
		deposits := ccxt.NewTransactionArray(depositsRes)
		for _, d := range deposits {
			amount := ptrFloat(d.Amount)
			if amount <= 0 {
				continue
			}
			ts := time.UnixMilli(ptrInt64(d.Timestamp)).UTC()
			cashflows = append(cashflows, &Cashflow{
				Amount:    amount,
				Currency:  ptrString(d.Currency),
				Timestamp: ts,
			})
		}
	}

	// Fetch withdrawals
	withdrawalsRes, err := c.call(ctx, func() <-chan interface{} {
		return c.core.FetchWithdrawals(nil, since.UnixMilli(), nil, map[string]interface{}{})
	})
	if err == nil {
		withdrawals := ccxt.NewTransactionArray(withdrawalsRes)
		for _, w := range withdrawals {
			amount := ptrFloat(w.Amount)
			if amount <= 0 {
				continue
			}
			ts := time.UnixMilli(ptrInt64(w.Timestamp)).UTC()
			cashflows = append(cashflows, &Cashflow{
				Amount:    -amount, // negative = withdrawal
				Currency:  ptrString(w.Currency),
				Timestamp: ts,
			})
		}
	}

	return cashflows, nil
}

func (c *CCXTConnector) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	if err := c.ensureMarkets(ctx); err != nil {
		return nil, err
	}
	if end.Before(start) {
		return nil, fmt.Errorf("invalid range: end before start")
	}

	tradesByKey := make(map[string]*Trade)
	marketHints := []string{"swap", "spot", "future", "options", "margin"}

	for _, hint := range marketHints {
		c.core.ExtendExchangeOptions(map[string]interface{}{"defaultType": hint})
		trades, err := c.fetchMyTrades(ctx, "", start, end, hint)
		if err != nil {
			continue
		}
		for _, t := range trades {
			tradesByKey[tradeKey(t)] = t
		}
	}

	// Fallback for exchanges requiring symbol on fetchMyTrades.
	if len(tradesByKey) == 0 {
		for _, symbol := range c.discoverActiveSymbols(ctx) {
			for _, hint := range []string{"spot", "swap"} {
				c.core.ExtendExchangeOptions(map[string]interface{}{"defaultType": hint})
				trades, err := c.fetchMyTrades(ctx, symbol, start, end, hint)
				if err != nil {
					continue
				}
				for _, t := range trades {
					tradesByKey[tradeKey(t)] = t
				}
			}
		}
	}

	out := make([]*Trade, 0, len(tradesByKey))
	for _, t := range tradesByKey {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Timestamp.Before(out[j].Timestamp)
	})
	return out, nil
}

func (c *CCXTConnector) fetchMyTrades(
	ctx context.Context,
	symbol string,
	start time.Time,
	end time.Time,
	marketHint string,
) ([]*Trade, error) {
	args := []interface{}{nil, start.UnixMilli(), int64(1000), map[string]interface{}{}}
	if symbol != "" {
		args[0] = symbol
	}

	res, err := c.call(ctx, func() <-chan interface{} {
		return c.core.FetchMyTrades(args...)
	})
	if err != nil {
		return nil, err
	}

	raw := ccxt.NewTradeArray(res)
	out := make([]*Trade, 0, len(raw))
	defaultMarketType := marketTypeFromHint(marketHint)

	for _, t := range raw {
		ts := ptrInt64(t.Timestamp)
		if ts == 0 {
			ts = parseDatetimeMillis(ptrString(t.Datetime))
		}
		if ts == 0 {
			continue
		}

		tradeTime := time.UnixMilli(ts).UTC()
		if tradeTime.Before(start) || tradeTime.After(end) {
			continue
		}

		tradeSymbol := ptrString(t.Symbol)
		if tradeSymbol == "" {
			tradeSymbol = symbol
		}

		id := ptrString(t.Id)
		side := strings.ToLower(strings.TrimSpace(ptrString(t.Side)))
		amount := ptrFloat(t.Amount)
		price := ptrFloat(t.Price)
		cost := ptrFloat(t.Cost)
		if price == 0 && amount > 0 && cost > 0 {
			price = cost / amount
		}
		if id == "" {
			id = fmt.Sprintf("%s:%d:%s:%f:%f", tradeSymbol, ts, side, amount, price)
		}

		info := t.Info
		realized := firstNumericFromMap(info,
			"realizedPnl",
			"realized_pnl",
			"realizedPNL",
			"pnl",
		)

		feeCurrency := firstStringFromMap(info,
			"feeCurrency",
			"fee_currency",
			"commissionAsset",
			"currency",
		)

		out = append(out, &Trade{
			ID:          id,
			Symbol:      tradeSymbol,
			Side:        side,
			Price:       price,
			Quantity:    amount,
			Fee:         ptrFloat(t.Fee.Cost),
			FeeCurrency: feeCurrency,
			RealizedPnL: realized,
			Timestamp:   tradeTime,
			MarketType:  c.marketTypeForSymbol(tradeSymbol, defaultMarketType),
		})
	}

	return out, nil
}

func (c *CCXTConnector) discoverActiveSymbols(ctx context.Context) []string {
	symbols := make(map[string]struct{})

	if positions, err := c.fetchPositions(ctx); err == nil {
		for _, p := range positions {
			if p.Symbol != "" {
				symbols[p.Symbol] = struct{}{}
			}
		}
	}

	res, err := c.call(ctx, func() <-chan interface{} {
		return c.core.FetchBalance(map[string]interface{}{})
	})
	if err == nil {
		bal := ccxt.NewBalances(res)
		for asset, total := range bal.Total {
			if total == nil || *total <= 0 {
				continue
			}
			assetU := strings.ToUpper(strings.TrimSpace(asset))
			if isStablecoin(assetU) {
				continue
			}
			for _, quote := range []string{"USDT", "USD", "USDC"} {
				sym := assetU + "/" + quote
				if c.hasMarketSymbol(sym) {
					symbols[sym] = struct{}{}
				}
			}
		}
	}

	out := make([]string, 0, len(symbols))
	for s := range symbols {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// GetBalanceByMarket returns per-market equity breakdown.
// Switches defaultType for each market and fetches balance.
func (c *CCXTConnector) GetBalanceByMarket(ctx context.Context) ([]*MarketBalance, error) {
	if err := c.ensureMarkets(ctx); err != nil {
		return nil, err
	}

	marketTypes := []struct {
		hint      string
		connector string
	}{
		{"spot", MarketSpot},
		{"swap", MarketSwap},
		{"future", MarketFutures},
		{"margin", MarketMargin},
	}

	var balances []*MarketBalance

	for _, mt := range marketTypes {
		c.core.ExtendExchangeOptions(map[string]interface{}{"defaultType": mt.hint})

		res, err := c.call(ctx, func() <-chan interface{} {
			return c.core.FetchBalance(map[string]interface{}{})
		})
		if err != nil {
			continue
		}

		bal := ccxt.NewBalances(res)
		available, equity, _ := extractStablecoinBalance(bal)
		if equity == 0 {
			available, equity = extractAggregateBalance(bal)
		}

		if equity > 0 {
			balances = append(balances, &MarketBalance{
				MarketType:      mt.connector,
				Equity:          equity,
				AvailableMargin: available,
			})
		}
	}

	// Reset to default
	c.core.ExtendExchangeOptions(map[string]interface{}{"defaultType": "swap"})

	return balances, nil
}

// DetectMarketTypes returns which market types this exchange supports.
// Scans loaded markets for spot, swap, future, option, margin capabilities.
func (c *CCXTConnector) DetectMarketTypes(ctx context.Context) ([]string, error) {
	if err := c.ensureMarkets(ctx); err != nil {
		return c.defaultMarketTypes(), nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	detected := make(map[string]struct{})
	for _, m := range c.markets {
		if ptrBool(m.Spot) {
			detected[MarketSpot] = struct{}{}
		}
		if ptrBool(m.Swap) {
			detected[MarketSwap] = struct{}{}
		}
		if ptrBool(m.Future) {
			detected[MarketFutures] = struct{}{}
		}
		if ptrBool(m.Option) {
			detected[MarketOptions] = struct{}{}
		}
		if ptrBool(m.Margin) {
			detected[MarketMargin] = struct{}{}
		}
	}

	if len(detected) == 0 {
		return c.defaultMarketTypes(), nil
	}

	out := make([]string, 0, len(detected))
	for mt := range detected {
		out = append(out, mt)
	}
	sort.Strings(out)
	return out, nil
}

func (c *CCXTConnector) defaultMarketTypes() []string {
	switch c.exchange {
	case "binance":
		return []string{MarketSpot, MarketSwap, MarketFutures}
	case "bybit", "okx", "bitget":
		return []string{MarketSpot, MarketSwap}
	default:
		return []string{MarketSpot}
	}
}

// GetTradesByMarket fetches trades for a specific market type by switching defaultType.
func (c *CCXTConnector) GetTradesByMarket(ctx context.Context, marketType string, since time.Time) ([]*Trade, error) {
	if err := c.ensureMarkets(ctx); err != nil {
		return nil, err
	}

	hint := marketType
	if marketType == MarketFutures {
		hint = "future"
	}
	c.core.ExtendExchangeOptions(map[string]interface{}{"defaultType": hint})
	defer c.core.ExtendExchangeOptions(map[string]interface{}{"defaultType": "swap"})

	return c.fetchMyTrades(ctx, "", since, time.Now().UTC(), hint)
}

// GetFundingFees fetches funding fee history for perpetual/swap positions.
func (c *CCXTConnector) GetFundingFees(ctx context.Context, symbols []string, since time.Time) ([]*FundingFee, error) {
	if err := c.ensureMarkets(ctx); err != nil {
		return nil, err
	}

	var fees []*FundingFee
	for _, symbol := range symbols {
		res, err := c.call(ctx, func() <-chan interface{} {
			return c.core.FetchFundingHistory(symbol, since.UnixMilli(), nil, map[string]interface{}{})
		})
		if err != nil {
			continue
		}

		raw := ccxt.NewTradeArray(res)
		for _, f := range raw {
			ts := ptrInt64(f.Timestamp)
			if ts == 0 {
				continue
			}
			fees = append(fees, &FundingFee{
				Amount:    ptrFloat(f.Amount),
				Symbol:    ptrString(f.Symbol),
				Timestamp: time.UnixMilli(ts).UTC(),
			})
		}
	}

	return fees, nil
}

// GetEarnBalance returns the total earn/staking balance if supported.
func (c *CCXTConnector) GetEarnBalance(ctx context.Context) (float64, error) {
	if err := c.ensureMarkets(ctx); err != nil {
		return 0, err
	}

	c.core.ExtendExchangeOptions(map[string]interface{}{"defaultType": "earn"})
	defer c.core.ExtendExchangeOptions(map[string]interface{}{"defaultType": "swap"})

	res, err := c.call(ctx, func() <-chan interface{} {
		return c.core.FetchBalance(map[string]interface{}{})
	})
	if err != nil {
		return 0, nil // Earn not supported, return 0
	}

	bal := ccxt.NewBalances(res)
	_, equity, _ := extractStablecoinBalance(bal)
	if equity == 0 {
		_, equity = extractAggregateBalance(bal)
	}
	return equity, nil
}

func (c *CCXTConnector) marketTypeForSymbol(symbol string, fallback string) string {
	if symbol == "" {
		return fallback
	}

	c.mu.RLock()
	market, ok := c.markets[symbol]
	c.mu.RUnlock()
	if !ok {
		return fallback
	}

	switch {
	case ptrBool(market.Spot):
		return MarketSpot
	case ptrBool(market.Swap):
		return MarketSwap
	case ptrBool(market.Future):
		return MarketFutures
	case ptrBool(market.Option):
		return MarketOptions
	case ptrBool(market.Margin):
		return MarketMargin
	default:
		return fallback
	}
}

func (c *CCXTConnector) hasMarketSymbol(symbol string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.markets[symbol]
	return ok
}

func (c *CCXTConnector) ensureMarkets(ctx context.Context) error {
	c.mu.RLock()
	if c.loaded {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loaded {
		return nil
	}

	res, err := c.call(ctx, func() <-chan interface{} {
		return c.core.LoadMarkets()
	})
	if err != nil {
		return err
	}

	c.markets = ccxt.NewMarketsMap(res)
	c.loaded = true
	return nil
}

func (c *CCXTConnector) call(ctx context.Context, fn func() <-chan interface{}) (interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ch := fn()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if ccxt.IsError(res) {
			return nil, ccxt.CreateReturnError(res)
		}
		return res, nil
	}
}

func marketTypeFromHint(hint string) string {
	switch strings.ToLower(strings.TrimSpace(hint)) {
	case "swap":
		return MarketSwap
	case "future", "futures":
		return MarketFutures
	case "option", "options":
		return MarketOptions
	case "margin":
		return MarketMargin
	default:
		return MarketSpot
	}
}

func extractStablecoinBalance(b ccxt.Balances) (available float64, equity float64, currency string) {
	for _, coin := range ccxtStablecoins {
		if total := b.Total[coin]; total != nil && *total > 0 {
			equity += *total
			if currency == "" {
				currency = coin
			}
		}
		if free := b.Free[coin]; free != nil && *free > 0 {
			available += *free
			if currency == "" {
				currency = coin
			}
		}
	}

	if equity > 0 && available == 0 {
		available = equity
	}
	return available, equity, currency
}

func extractAggregateBalance(b ccxt.Balances) (available float64, equity float64) {
	for _, total := range b.Total {
		if total != nil {
			equity += *total
		}
	}
	for _, free := range b.Free {
		if free != nil {
			available += *free
		}
	}
	if equity > 0 && available == 0 {
		available = equity
	}
	return available, equity
}

func shouldUseProxy(exchangeID, proxyExchangesEnv string) bool {
	raw := strings.TrimSpace(proxyExchangesEnv)
	if raw == "" {
		raw = "binance"
	}
	target := strings.ToLower(strings.TrimSpace(exchangeID))

	for _, ex := range strings.Split(raw, ",") {
		if strings.ToLower(strings.TrimSpace(ex)) == target {
			return true
		}
	}
	return false
}

func isStablecoin(asset string) bool {
	asset = strings.ToUpper(strings.TrimSpace(asset))
	for _, s := range ccxtStablecoins {
		if asset == s {
			return true
		}
	}
	return false
}

func tradeKey(t *Trade) string {
	if t == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s|%d|%f|%f|%s",
		t.ID, t.Symbol, t.Timestamp.UnixMilli(), t.Price, t.Quantity, t.Side,
	)
}

func ptrFloat(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func ptrInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func ptrString(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

func ptrBool(v *bool) bool {
	return v != nil && *v
}

func parseDatetimeMillis(s string) int64 {
	if s == "" {
		return 0
	}

	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

func firstNumericFromMap(m map[string]interface{}, keys ...string) float64 {
	if m == nil {
		return 0
	}
	for _, key := range keys {
		if val, ok := m[key]; ok {
			if n, ok := anyToFloat64(val); ok {
				return n
			}
		}
	}
	return 0
}

func firstStringFromMap(m map[string]interface{}, keys ...string) string {
	if m == nil {
		return ""
	}
	for _, key := range keys {
		if val, ok := m[key]; ok {
			if s := strings.TrimSpace(fmt.Sprint(val)); s != "" && s != "<nil>" {
				return s
			}
		}
	}
	return ""
}

func anyToFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case json.Number:
		n, err := x.Float64()
		return n, err == nil
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func mapFromAny(v interface{}) (map[string]interface{}, bool) {
	m, ok := v.(map[string]interface{})
	return m, ok
}

func sliceFromAny(v interface{}) ([]interface{}, bool) {
	s, ok := v.([]interface{})
	return s, ok
}

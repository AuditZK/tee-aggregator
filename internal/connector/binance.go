package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	binanceSpotAPI         = "https://api.binance.com"
	binanceFuturesAPI      = "https://fapi.binance.com"
	binanceCoinMFuturesAPI = "https://dapi.binance.com"

	// QUAL-001: extracted to remove duplication across signed-request sites.
	binancePathAccount = "/api/v3/account"
)

// Binance implements Connector for Binance exchange.
type Binance struct {
	apiKey    string
	apiSecret string
	client    *http.Client
	// cachedBreakdown carries the per-market split computed by the last
	// GetBalance, served by GetBalanceByMarket without extra API calls (the
	// midnight herd already rate-limits fapi; same pattern as IBKR).
	cachedBreakdown []*MarketBalance
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
			// Binance's default recvWindow is 5000ms; through the egress proxy
			// a signed call regularly exceeds it and dies with -1021 (HTTP 400,
			// non-retryable) — observed as wallet reads silently dropping while
			// the rebuilder, which already sends 30000, read the same account
			// fine from the same IP.
			params.Set("recvWindow", "30000")
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
	// One public ticker fetch, shared by every wallet that needs pricing (spot
	// alts, COIN-M collateral coins, margin BTC valuation). A transient failure
	// must fail the sync: a margin-heavy account valued without prices would
	// persist near-zero equity.
	priceMap, perr := FetchBinanceStylePriceMap(ctx, b.client, binanceSpotAPI)
	if errors.Is(perr, ErrTransient) {
		return nil, fmt.Errorf("price map: %w", perr)
	}
	if priceMap == nil {
		priceMap = map[string]float64{}
	}

	b.cachedBreakdown = nil

	spotBalance, err := b.getSpotBalance(ctx, priceMap)
	if err != nil {
		return nil, fmt.Errorf("spot balance: %w", err)
	}
	total := spotBalance
	// total aliases spotBalance — snapshot the spot-only equity before the
	// wallet adds below mutate it through the shared pointer.
	nonUM := spotBalance.Equity

	// Non-spot wallets are best-effort ONLY for permission-style refusals (a
	// key without the futures/margin scope, a product never enabled). A
	// TRANSIENT failure (429/5xx/network — e.g. the midnight herd rate-limiting
	// fapi through the shared egress) must fail the whole sync instead:
	// silently dropping a wallet persisted snapshots $16k-$20k short, and a
	// failed sync is a retry while a wrong snapshot is a lie on the curve.
	var futures *Balance
	if fut, ferr := b.getFuturesBalance(ctx); ferr == nil && fut != nil {
		total.Equity += fut.Equity
		total.UnrealizedPnL += fut.UnrealizedPnL
		futures = fut
	} else if errors.Is(ferr, ErrTransient) {
		return nil, fmt.Errorf("futures balance: %w", ferr)
	}

	if eq, cerr := b.getCoinMFuturesEquity(ctx, priceMap); cerr == nil {
		total.Equity += eq
		nonUM += eq
	} else if errors.Is(cerr, ErrTransient) {
		return nil, fmt.Errorf("coin-m balance: %w", cerr)
	}
	if eq, merr := b.getCrossMarginEquity(ctx, priceMap); merr == nil {
		total.Equity += eq
		nonUM += eq
	} else if errors.Is(merr, ErrTransient) {
		return nil, fmt.Errorf("cross margin balance: %w", merr)
	}
	if eq, ierr := b.getIsolatedMarginEquity(ctx, priceMap); ierr == nil {
		total.Equity += eq
		nonUM += eq
	} else if errors.Is(ierr, ErrTransient) {
		return nil, fmt.Errorf("isolated margin balance: %w", ierr)
	}

	// Per-market split, matching the history rebuilder's two-bucket convention
	// (its "spot" carries everything non-UM — spot, COIN-M, cross+iso margin —
	// with available=equity; "swap" is UM futures with the true free margin).
	// Diverging from that convention makes the margin curve jump at the seam
	// between rebuilt days and live days.
	if nonUM != 0 {
		b.cachedBreakdown = append(b.cachedBreakdown, &MarketBalance{
			MarketType: MarketSpot, Equity: nonUM, AvailableMargin: nonUM,
		})
	}
	if futures != nil && (futures.Equity != 0 || futures.Available != 0) {
		b.cachedBreakdown = append(b.cachedBreakdown, &MarketBalance{
			MarketType: MarketSwap, Equity: futures.Equity, AvailableMargin: futures.Available,
		})
	}

	// Available must mirror the buckets: the sync persists it as
	// breakdown.global.available_margin, which is the field the dashboard's
	// free-margin line reads. Left at the spot wallet's free stablecoins it
	// reads 0 for any account holding no loose stables (margin/futures-heavy
	// accounts), flattening the curve onto capital.
	total.Available = nonUM
	if futures != nil {
		total.Available += futures.Available
	}

	return total, nil
}

// GetBalanceByMarket returns the split cached by the last GetBalance call —
// no additional API calls (the midnight herd already rate-limits fapi).
func (b *Binance) GetBalanceByMarket(_ context.Context) ([]*MarketBalance, error) {
	return b.cachedBreakdown, nil
}

func (b *Binance) getSpotBalance(ctx context.Context, priceMap map[string]float64) (*Balance, error) {
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
	// equity of ~0 (CONN-VALUE-001). ValueSpotHoldingsUSD prices them from the
	// shared ticker map (stablecoins 1:1).
	var holdings []SpotHolding
	var stableAvailable float64 // free stablecoins = liquid USD available
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
		}
	}

	return &Balance{
		Available: stableAvailable,
		Equity:    ValueSpotHoldingsUSD(holdings, priceMap),
		Currency:  "USDT",
	}, nil
}

// getCoinMFuturesEquity returns the COIN-M (coin-margined) futures equity in
// USD. Each collateral asset's margin balance (walletBalance + crossUnPnl, in
// the coin — e.g. BTC/ETH) is valued via priceMap. /dapi/v1/balance mirrors the
// USDⓈ-M /fapi/v2/balance shape. Best-effort: a key without COIN-M permission
// or an account that never enabled it returns an error the caller ignores.
func (b *Binance) getCoinMFuturesEquity(ctx context.Context, priceMap map[string]float64) (float64, error) {
	params := url.Values{}
	body, err := b.doRequest(ctx, "GET", binanceCoinMFuturesAPI, "/dapi/v1/balance", params, true)
	if err != nil {
		return 0, err
	}

	var resp []struct {
		Asset      string `json:"asset"`
		Balance    string `json:"balance"`
		CrossUnPnl string `json:"crossUnPnl"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, err
	}

	var holdings []SpotHolding
	for _, a := range resp {
		bal, _ := strconv.ParseFloat(a.Balance, 64)
		upnl, _ := strconv.ParseFloat(a.CrossUnPnl, 64)
		if mb := bal + upnl; mb != 0 {
			holdings = append(holdings, SpotHolding{Asset: a.Asset, Amount: mb})
		}
	}
	return ValueSpotHoldingsUSD(holdings, priceMap), nil
}

// getCrossMarginEquity returns the cross margin wallet's net equity in USD:
// totalNetAssetOfBtc (assets − borrowed liabilities, in BTC) × BTCUSDT.
func (b *Binance) getCrossMarginEquity(ctx context.Context, priceMap map[string]float64) (float64, error) {
	return b.marginNetEquityUSD(ctx, "/sapi/v1/margin/account", priceMap)
}

// getIsolatedMarginEquity returns the isolated margin wallet's net equity in
// USD (same totalNetAssetOfBtc shape, aggregated across all isolated pairs).
func (b *Binance) getIsolatedMarginEquity(ctx context.Context, priceMap map[string]float64) (float64, error) {
	return b.marginNetEquityUSD(ctx, "/sapi/v1/margin/isolated/account", priceMap)
}

// marginNetEquityUSD reads a SAPI margin account endpoint that reports
// totalNetAssetOfBtc (net of borrowed funds, in BTC) and converts it to USD via
// the BTCUSDT price. Best-effort: a key without margin permission errors and
// the caller skips it.
func (b *Binance) marginNetEquityUSD(ctx context.Context, path string, priceMap map[string]float64) (float64, error) {
	params := url.Values{}
	body, err := b.doRequest(ctx, "GET", binanceSpotAPI, path, params, true)
	if err != nil {
		return 0, err
	}

	var resp struct {
		TotalNetAssetOfBtc string `json:"totalNetAssetOfBtc"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, err
	}

	netBtc, _ := strconv.ParseFloat(resp.TotalNetAssetOfBtc, 64)
	if netBtc == 0 {
		return 0, nil
	}
	return netBtc * priceMap["BTCUSDT"], nil
}

func (b *Binance) getFuturesBalance(ctx context.Context) (*Balance, error) {
	params := url.Values{}
	body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v2/account", params, true)
	if err != nil {
		// /fapi/v2/account is being retired by Binance and already errors for
		// some accounts while /fapi/v2/balance still answers (observed in the
		// field: a sub-account whose $16k USDⓈ-M wallet silently vanished from
		// the summed equity because the caller treats this read as
		// best-effort). Fall back to the balance endpoint before giving up.
		return b.getFuturesBalanceFromBalanceEndpoint(ctx)
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

	// /fapi/v2/account is being retired and returns HTTP 200 with an empty
	// totalMarginBalance for some accounts — NOT an error, so the fallback
	// above never fires (observed in the field: a $20k USDⓈ-M master account
	// read as $0 while /fapi/v2/balance still answered $20k). A genuine zero and
	// a degraded-empty response are indistinguishable here, so when the account
	// endpoint reports nothing, cross-check the balance endpoint and prefer it
	// when it finds funds; a truly empty futures wallet still reads 0 from both.
	if equity == 0 {
		if bal, berr := b.getFuturesBalanceFromBalanceEndpoint(ctx); berr == nil && bal.Equity > 0 {
			return bal, nil
		}
	}

	return &Balance{
		Available:     available,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USDT",
	}, nil
}

// getFuturesBalanceFromBalanceEndpoint sums the stable-asset wallet balances
// and cross unrealized PnL from /fapi/v2/balance — the same read the history
// rebuilder uses. Slightly narrower than /fapi/v2/account (non-stable
// collateral in multi-asset mode is not priced here) but proven to answer on
// accounts where the account endpoint fails.
func (b *Binance) getFuturesBalanceFromBalanceEndpoint(ctx context.Context) (*Balance, error) {
	params := url.Values{}
	body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v2/balance", params, true)
	if err != nil {
		return nil, err
	}

	var rows []struct {
		Asset            string `json:"asset"`
		Balance          string `json:"balance"`
		CrossUnPnl       string `json:"crossUnPnl"`
		AvailableBalance string `json:"availableBalance"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}

	out := &Balance{Currency: "USDT"}
	for _, a := range rows {
		if !IsStablecoinUSD(a.Asset) {
			continue
		}
		bal, _ := strconv.ParseFloat(a.Balance, 64)
		upnl, _ := strconv.ParseFloat(a.CrossUnPnl, 64)
		avail, _ := strconv.ParseFloat(a.AvailableBalance, 64)
		out.Equity += bal + upnl
		out.UnrealizedPnL += upnl
		out.Available += avail
	}
	return out, nil
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

// binanceExternalTransferTypes are the universal-transfer types with exactly
// one side inside the tracked perimeter (spot MAIN + UMFUTURE): money through
// them is a real deposit/withdrawal from the timeline's perspective. Sign:
// +1 = into the perimeter, -1 = out. MAIN_UMFUTURE / UMFUTURE_MAIN move value
// between the two tracked wallets and are deliberately absent (they cancel).
// Must mirror the rebuilder's table (history-rebuilder-go exchanges/binance)
// so live and rebuilt cashflows stay consistent across the stitch.
var binanceExternalTransferTypes = map[string]float64{
	"FUNDING_MAIN":     +1,
	"MAIN_FUNDING":     -1,
	"FUNDING_UMFUTURE": +1,
	"UMFUTURE_FUNDING": -1,
	"MARGIN_MAIN":      +1,
	"MAIN_MARGIN":      -1,
	"CMFUTURE_MAIN":    +1,
	"MAIN_CMFUTURE":    -1,
	"MAIN_OPTION":      -1,
	"OPTION_MAIN":      +1,
	"MAIN_MINING":      -1,
	"MINING_MAIN":      +1,
}

func (b *Binance) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	now := time.Now().UTC()

	var flows []*Cashflow
	add := func(t time.Time, usd float64) {
		if usd != 0 {
			flows = append(flows, &Cashflow{Amount: usd, Currency: "USDT", Timestamp: t})
		}
	}

	// Price map only fetched lazily, on the first non-stable coin seen.
	var priceMap map[string]float64
	usdValue := func(coin string, qty float64) float64 {
		if IsStablecoinUSD(coin) {
			return qty
		}
		if priceMap == nil {
			pm, perr := FetchBinanceStylePriceMap(ctx, b.client, binanceSpotAPI)
			if perr != nil {
				pm = map[string]float64{}
			}
			priceMap = pm
		}
		if p := priceMap[strings.ToUpper(coin)+"USDT"]; p > 0 {
			return qty * p
		}
		return 0 // unpriceable dust — never fabricate a flow from it
	}

	// On-chain flows are load-bearing: a broken deposit feed silently books
	// every inflow as trading gain, so their errors fail the fetch.
	if err := b.fetchOnChainDeposits(ctx, since, now, add, usdValue); err != nil {
		return nil, err
	}
	if err := b.fetchOnChainWithdrawals(ctx, since, now, add, usdValue); err != nil {
		return nil, err
	}
	// Universal transfers are best-effort per type: a key without a given
	// wallet product errors for that type, which must not zero the flows above.
	b.fetchExternalTransfers(ctx, since, now, add, usdValue)

	return flows, nil
}

func (b *Binance) fetchOnChainDeposits(ctx context.Context, since, now time.Time, add func(time.Time, float64), usdValue func(string, float64) float64) error {
	params := url.Values{}
	params.Set("startTime", strconv.FormatInt(since.UnixMilli(), 10))
	params.Set("endTime", strconv.FormatInt(now.UnixMilli(), 10))
	params.Set("status", "1") // success
	params.Set("limit", "1000")
	body, err := b.doRequest(ctx, "GET", binanceSpotAPI, "/sapi/v1/capital/deposit/hisrec", params, true)
	if err != nil {
		return fmt.Errorf("deposit history: %w", err)
	}
	var deps []struct {
		Coin       string `json:"coin"`
		Amount     string `json:"amount"`
		InsertTime int64  `json:"insertTime"`
	}
	if err := json.Unmarshal(body, &deps); err != nil {
		return fmt.Errorf("parse deposit history: %w", err)
	}
	for _, d := range deps {
		amt, _ := strconv.ParseFloat(d.Amount, 64)
		add(time.UnixMilli(d.InsertTime).UTC(), usdValue(d.Coin, amt))
	}
	return nil
}

func (b *Binance) fetchOnChainWithdrawals(ctx context.Context, since, now time.Time, add func(time.Time, float64), usdValue func(string, float64) float64) error {
	params := url.Values{}
	params.Set("startTime", strconv.FormatInt(since.UnixMilli(), 10))
	params.Set("endTime", strconv.FormatInt(now.UnixMilli(), 10))
	params.Set("status", "6") // completed
	params.Set("limit", "1000")
	body, err := b.doRequest(ctx, "GET", binanceSpotAPI, "/sapi/v1/capital/withdraw/history", params, true)
	if err != nil {
		return fmt.Errorf("withdraw history: %w", err)
	}
	var wds []struct {
		Coin           string `json:"coin"`
		Amount         string `json:"amount"`
		TransactionFee string `json:"transactionFee"`
		CompleteTime   string `json:"completeTime"` // "2026-07-01 12:34:56" UTC
	}
	if err := json.Unmarshal(body, &wds); err != nil {
		return fmt.Errorf("parse withdraw history: %w", err)
	}
	for _, w := range wds {
		t, terr := time.Parse("2006-01-02 15:04:05", w.CompleteTime)
		if terr != nil {
			continue
		}
		amt, _ := strconv.ParseFloat(w.Amount, 64)
		fee, _ := strconv.ParseFloat(w.TransactionFee, 64)
		// The network fee left the account too.
		add(t.UTC(), -usdValue(w.Coin, amt+fee))
	}
	return nil
}

func (b *Binance) fetchExternalTransfers(ctx context.Context, since, now time.Time, add func(time.Time, float64), usdValue func(string, float64) float64) {
	for typ, sign := range binanceExternalTransferTypes {
		params := url.Values{}
		params.Set("type", typ)
		params.Set("startTime", strconv.FormatInt(since.UnixMilli(), 10))
		params.Set("endTime", strconv.FormatInt(now.UnixMilli(), 10))
		params.Set("size", "100")
		body, err := b.doRequest(ctx, "GET", binanceSpotAPI, "/sapi/v1/asset/transfer", params, true)
		if err != nil {
			continue
		}
		var resp struct {
			Rows []struct {
				Asset     string `json:"asset"`
				Amount    string `json:"amount"`
				Timestamp int64  `json:"timestamp"`
				Status    string `json:"status"`
			} `json:"rows"`
		}
		if json.Unmarshal(body, &resp) != nil {
			continue
		}
		for _, row := range resp.Rows {
			if row.Status != "" && !strings.EqualFold(row.Status, "CONFIRMED") {
				continue
			}
			amt, _ := strconv.ParseFloat(row.Amount, 64)
			add(time.UnixMilli(row.Timestamp).UTC(), sign*usdValue(row.Asset, amt))
		}
	}
}

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
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
	// mu guards the three fields below. GetBalance holds it throughout: the
	// instance is shared by every sync of the connection for an hour, and two
	// balance reads must not interleave their appends.
	mu sync.Mutex
	// cachedBreakdown carries the per-market split computed by the last
	// GetBalance, served by GetBalanceByMarket without extra API calls (the
	// midnight herd already rate-limits fapi; same pattern as IBKR).
	cachedBreakdown []*MarketBalance
	// capabilityWarnings carries key-scope gaps discovered by the last
	// GetBalance (CapabilityWarner) — e.g. -2015 on every fapi endpoint
	// while sapi reads fine means the key lacks the Futures scope and the
	// UM wallet is invisible.
	capabilityWarnings []string
	// coverage grades every wallet the last GetBalance tried to reach
	// (CoverageReporter). Scope note: it answers "did we reach what we
	// tried", not "did we try everything the account holds" — Earn is not
	// fetched at all and therefore not listed.
	coverage []WalletCoverage
	// cashflowNotes carries the markers the last GetCashflows raised.
	cashflowNotes noteList
}

// Binance wallet names. Their own vocabulary, not market types: cross and
// isolated margin are two wallets that both land in the margin market.
const (
	binanceWalletSpot     = "spot"
	binanceWalletUM       = "um_futures"
	binanceWalletCoinM    = "coinm_futures"
	binanceWalletCross    = "cross_margin"
	binanceWalletIsolated = "isolated_margin"

	// Wallets GetBalance does not read: a transfer to one leaves the equity.
	binanceWalletFunding = "funding"
	binanceWalletOption  = "option"
	binanceWalletMining  = "mining"
)

// Coverage implements CoverageReporter.
func (b *Binance) Coverage() []WalletCoverage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.coverage)
}

// noteWallet grades one wallet from the error its fetch returned. A nil error
// is a read whatever the balance held; anything else is a wallet we could not
// look into, which is not the same as one holding nothing.
func (b *Binance) noteWallet(wallet string, err error) {
	c := WalletCoverage{Wallet: wallet, Status: WalletRead}
	if err != nil {
		c.Status = WalletUnreadable
		c.Reason = vendorErrorDetail(err.Error())
	}
	b.coverage = append(b.coverage, c)
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
	b.mu.Lock()
	defer b.mu.Unlock()

	// One public ticker fetch, shared by every wallet that needs pricing (spot
	// alts, COIN-M collateral coins, margin BTC valuation). A transient failure
	// must fail the sync: a margin-heavy account valued without prices would
	// persist near-zero equity.
	// CONN-08: ANY failure class, not just ErrTransient. 451 (geo-block) and
	// 418 (IP ban) are absent from isRetryableStatus, so they arrived here
	// unwrapped and fell through to an empty map — which values every alt,
	// every COIN-M collateral coin and the whole margin BTC position at zero.
	priceMap, perr := FetchBinanceStylePriceMap(ctx, b.client, binanceSpotAPI)
	if perr != nil {
		return nil, fmt.Errorf("%w: binance spot tickers: %v", ErrSpotPricingUnavailable, perr)
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
	// silently dropping a wallet persisted snapshots short by the whole
	// wallet, and a failed sync is a retry while a wrong snapshot is a lie on
	// the curve.
	b.coverage = nil
	b.noteWallet(binanceWalletSpot, nil) // reached: getSpotBalance failing is fatal above

	var futures *Balance
	b.capabilityWarnings = nil
	if fut, ferr := b.getFuturesBalance(ctx, priceMap); ferr == nil && fut != nil {
		total.Equity += fut.Equity
		total.UnrealizedPnL += fut.UnrealizedPnL
		futures = fut
		b.noteWallet(binanceWalletUM, nil)
	} else if errors.Is(ferr, ErrTransient) {
		return nil, fmt.Errorf("futures balance: %w", ferr)
	} else if ferr != nil && (strings.Contains(ferr.Error(), "-2015") || strings.Contains(ferr.Error(), "Invalid API-key")) {
		// Permission-style refusal on the futures scope while the spot read
		// above succeeded with the same key: the UM wallet may hold value
		// (or debt) we cannot see. Not fatal — the sync proceeds on what the
		// key does cover — but surfaced so the frontend can ask the user to
		// tick "Enable Futures" on the key (2026-08-05: an invisible UM
		// wallet moved ±15k and overstated the live equity by its debt).
		b.capabilityWarnings = append(b.capabilityWarnings, "futures_permission_missing")
		b.noteWallet(binanceWalletUM, ferr)
	} else {
		b.noteWallet(binanceWalletUM, ferr)
	}

	if eq, cerr := b.getCoinMFuturesEquity(ctx, priceMap); cerr == nil {
		total.Equity += eq
		nonUM += eq
		b.noteWallet(binanceWalletCoinM, nil)
	} else if errors.Is(cerr, ErrTransient) {
		return nil, fmt.Errorf("coin-m balance: %w", cerr)
	} else {
		// Was dropped in silence: a permission-style refusal here removed the
		// wallet from the equity with no warning and no trace, and a
		// margin-heavy account can hold most of its capital in one of these.
		b.capabilityWarnings = append(b.capabilityWarnings, "coinm_unreadable")
		b.noteWallet(binanceWalletCoinM, cerr)
	}
	if eq, merr := b.getCrossMarginEquity(ctx, priceMap); merr == nil {
		total.Equity += eq
		nonUM += eq
		b.noteWallet(binanceWalletCross, nil)
	} else if errors.Is(merr, ErrTransient) {
		return nil, fmt.Errorf("cross margin balance: %w", merr)
	} else {
		// Was dropped in silence: a permission-style refusal here removed the
		// wallet from the equity with no warning and no trace, and a
		// margin-heavy account can hold most of its capital in one of these.
		b.capabilityWarnings = append(b.capabilityWarnings, "cross_margin_unreadable")
		b.noteWallet(binanceWalletCross, merr)
	}
	if eq, ierr := b.getIsolatedMarginEquity(ctx, priceMap); ierr == nil {
		total.Equity += eq
		nonUM += eq
		b.noteWallet(binanceWalletIsolated, nil)
	} else if errors.Is(ierr, ErrTransient) {
		return nil, fmt.Errorf("isolated margin balance: %w", ierr)
	} else {
		// Was dropped in silence: a permission-style refusal here removed the
		// wallet from the equity with no warning and no trace, and a
		// margin-heavy account can hold most of its capital in one of these.
		b.capabilityWarnings = append(b.capabilityWarnings, "isolated_margin_unreadable")
		b.noteWallet(binanceWalletIsolated, ierr)
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
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.cachedBreakdown), nil
}

// CapabilityWarnings implements CapabilityWarner with the key-scope gaps the
// last GetBalance found and the flows the last GetCashflows could not value.
func (b *Binance) CapabilityWarnings() []string {
	b.mu.Lock()
	out := slices.Clone(b.capabilityWarnings)
	b.mu.Unlock()
	return append(out, b.cashflowNotes.get()...)
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

func (b *Binance) getFuturesBalance(ctx context.Context, priceMap map[string]float64) (*Balance, error) {
	params := url.Values{}
	body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v2/account", params, true)
	if err != nil {
		// /fapi/v2/account is being retired by Binance and already errors for
		// some accounts while /fapi/v2/balance still answers (observed in the
		// field: a sub-account's USDⓈ-M wallet silently vanished from the
		// summed equity because the caller treats this read as best-effort). Fall back to the balance endpoint before giving up.
		return b.getFuturesBalanceFromBalanceEndpoint(ctx)
	}

	// totalMarginBalance already folds in unrealized PnL and every margin asset
	// in multi-asset mode, so it is the true USDⓈ-M futures equity — strictly
	// better than the previous "USDT wallet balance only" read, which dropped
	// non-USDT collateral. It never overlaps the spot wallet, so summing the two
	// in GetBalance does not double-count.
	var resp struct {
		TotalWalletBalance    string                   `json:"totalWalletBalance"`
		TotalMarginBalance    string                   `json:"totalMarginBalance"`
		TotalUnrealizedProfit string                   `json:"totalUnrealizedProfit"`
		AvailableBalance      string                   `json:"availableBalance"`
		Assets                []binanceFuturesAssetRow `json:"assets"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	equity, _ := strconv.ParseFloat(resp.TotalMarginBalance, 64)
	unrealized, _ := strconv.ParseFloat(resp.TotalUnrealizedProfit, 64)
	available, _ := strconv.ParseFloat(resp.AvailableBalance, 64)

	// In single-asset mode the totals count USDT alone: a USDC or BNFCR margin
	// wallet would read as its USDT leftover. Measured before shipping with the
	// balance probe across every connected Binance account: none held
	// collateral outside its totals, so no live equity jumps on the change.
	if binanceTotalsCoverUSDTOnly(resp.TotalWalletBalance, resp.Assets) {
		for _, a := range resp.Assets {
			if strings.EqualFold(a.Asset, "USDT") {
				continue
			}
			equity += binanceMarginAssetUSD(a.Asset, a.MarginBalance, priceMap)
			unrealized += binanceMarginAssetUSD(a.Asset, a.UnrealizedProfit, priceMap)
			available += binanceMarginAssetUSD(a.Asset, a.AvailableBalance, priceMap)
		}
	}

	// /fapi/v2/account is being retired and returns HTTP 200 with an empty
	// totalMarginBalance for some accounts — NOT an error, so the fallback
	// above never fires (observed in the field: a funded USDⓈ-M master account
	// read as zero while /fapi/v2/balance still answered its balance). A
	// genuine zero and a degraded-empty response are indistinguishable here, so
	// when the account endpoint reports nothing, cross-check the balance
	// endpoint and prefer it when it finds funds; a truly empty futures wallet
	// still reads 0 from both.
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

// userTrades returns at most 1000 fills per call and refuses a range wider
// than seven days; binanceMinFillSplit is the narrowest range a full page is
// split into.
const (
	binanceFillPageLimit = 1000
	binanceFillWindow    = 7 * 24 * time.Hour
	binanceMinFillSplit  = time.Second
)

type binanceFuturesFill struct {
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

// getFuturesTrades reads the UM fills over [start, end], seven days at a time.
// A page that comes back full is not the whole range: the call has no cursor
// to continue from, so the range is halved until each half fits, the halves
// sharing their midpoint and the fill id dropping the overlap.
func (b *Binance) getFuturesTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	seen := map[int64]bool{}
	var fills []binanceFuturesFill
	var read func(from, to time.Time) error
	read = func(from, to time.Time) error {
		page, err := b.futuresFillPage(ctx, from, to)
		if err != nil {
			return err
		}
		if len(page) >= binanceFillPageLimit && to.Sub(from) > binanceMinFillSplit {
			mid := from.Add(to.Sub(from) / 2)
			if err := read(from, mid); err != nil {
				return err
			}
			return read(mid, to)
		}
		if len(page) >= binanceFillPageLimit {
			return fmt.Errorf("read binance futures fills: more than %d in one second", binanceFillPageLimit)
		}
		for _, f := range page {
			if !seen[f.ID] {
				seen[f.ID] = true
				fills = append(fills, f)
			}
		}
		return nil
	}
	for winStart := start; winStart.Before(end); winStart = winStart.Add(binanceFillWindow) {
		winEnd := winStart.Add(binanceFillWindow)
		if winEnd.After(end) {
			winEnd = end
		}
		if err := read(winStart, winEnd); err != nil {
			return nil, err
		}
	}

	trades := make([]*Trade, 0, len(fills))
	for _, t := range fills {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Qty, 64)
		fee, _ := strconv.ParseFloat(t.Commission, 64)
		pnl, _ := strconv.ParseFloat(t.RealizedPnl, 64)

		trades = append(trades, &Trade{
			ID:          strconv.FormatInt(t.ID, 10),
			Symbol:      t.Symbol,
			Side:        strings.ToLower(t.Side),
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

func (b *Binance) futuresFillPage(ctx context.Context, from, to time.Time) ([]binanceFuturesFill, error) {
	params := url.Values{}
	params.Set("startTime", strconv.FormatInt(from.UnixMilli(), 10))
	params.Set("endTime", strconv.FormatInt(to.UnixMilli(), 10))
	params.Set("limit", strconv.Itoa(binanceFillPageLimit))

	body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v1/userTrades", params, true)
	if err != nil {
		return nil, err
	}
	var page []binanceFuturesFill
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("decode binance futures fills: %w", err)
	}
	return page, nil
}

// binanceTransferWallets maps each universal-transfer type to the wallets it
// moves value between. Whether a type is a cashflow is not a property of the
// type: it depends on which wallets the equity measures (binanceTransferSign).
var binanceTransferWallets = map[string][2]string{
	"MAIN_UMFUTURE":         {binanceWalletSpot, binanceWalletUM},
	"UMFUTURE_MAIN":         {binanceWalletUM, binanceWalletSpot},
	"MAIN_CMFUTURE":         {binanceWalletSpot, binanceWalletCoinM},
	"CMFUTURE_MAIN":         {binanceWalletCoinM, binanceWalletSpot},
	"MAIN_MARGIN":           {binanceWalletSpot, binanceWalletCross},
	"MARGIN_MAIN":           {binanceWalletCross, binanceWalletSpot},
	"UMFUTURE_MARGIN":       {binanceWalletUM, binanceWalletCross},
	"MARGIN_UMFUTURE":       {binanceWalletCross, binanceWalletUM},
	"CMFUTURE_MARGIN":       {binanceWalletCoinM, binanceWalletCross},
	"MARGIN_CMFUTURE":       {binanceWalletCross, binanceWalletCoinM},
	"ISOLATEDMARGIN_MARGIN": {binanceWalletIsolated, binanceWalletCross},
	"MARGIN_ISOLATEDMARGIN": {binanceWalletCross, binanceWalletIsolated},
	"MAIN_ISOLATED_MARGIN":  {binanceWalletSpot, binanceWalletIsolated},
	"ISOLATED_MARGIN_MAIN":  {binanceWalletIsolated, binanceWalletSpot},
	"MAIN_FUNDING":          {binanceWalletSpot, binanceWalletFunding},
	"FUNDING_MAIN":          {binanceWalletFunding, binanceWalletSpot},
	"UMFUTURE_FUNDING":      {binanceWalletUM, binanceWalletFunding},
	"FUNDING_UMFUTURE":      {binanceWalletFunding, binanceWalletUM},
	"CMFUTURE_FUNDING":      {binanceWalletCoinM, binanceWalletFunding},
	"FUNDING_CMFUTURE":      {binanceWalletFunding, binanceWalletCoinM},
	"MARGIN_FUNDING":        {binanceWalletCross, binanceWalletFunding},
	"FUNDING_MARGIN":        {binanceWalletFunding, binanceWalletCross},
	"MAIN_OPTION":           {binanceWalletSpot, binanceWalletOption},
	"OPTION_MAIN":           {binanceWalletOption, binanceWalletSpot},
	"UMFUTURE_OPTION":       {binanceWalletUM, binanceWalletOption},
	"OPTION_UMFUTURE":       {binanceWalletOption, binanceWalletUM},
	"MARGIN_OPTION":         {binanceWalletCross, binanceWalletOption},
	"OPTION_MARGIN":         {binanceWalletOption, binanceWalletCross},
	"MAIN_MINING":           {binanceWalletSpot, binanceWalletMining},
	"MINING_MAIN":           {binanceWalletMining, binanceWalletSpot},
}

// binanceLiveWallets are the wallets GetBalance sums into the equity.
var binanceLiveWallets = []string{binanceWalletSpot, binanceWalletUM, binanceWalletCoinM, binanceWalletCross, binanceWalletIsolated}

// binanceTransferSign is +1 when the transfer brings value into the measured
// wallets, -1 when it takes value out, 0 when it moves value between two of
// them or between two it does not measure.
func binanceTransferSign(typ string, measured map[string]bool) float64 {
	w, ok := binanceTransferWallets[typ]
	if !ok {
		return 0
	}
	from, to := measured[w[0]], measured[w[1]]
	switch {
	case to && !from:
		return +1
	case from && !to:
		return -1
	}
	return 0
}

// measuredWallets is what the last GetBalance actually read. A wallet the key
// cannot open is outside the equity, so money moved into it did leave. With no
// balance read yet (an operator dump of cashflows alone), every live wallet
// counts.
func (b *Binance) measuredWallets() map[string]bool {
	b.mu.Lock()
	coverage := slices.Clone(b.coverage)
	b.mu.Unlock()
	out := map[string]bool{}
	if len(coverage) == 0 {
		for _, w := range binanceLiveWallets {
			out[w] = true
		}
		return out
	}
	for _, c := range coverage {
		if c.Status == WalletRead {
			out[c.Wallet] = true
		}
	}
	return out
}

func (b *Binance) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	now := time.Now().UTC()

	var flows []*Cashflow
	add := func(t time.Time, usd float64) {
		if usd != 0 {
			flows = append(flows, &Cashflow{Amount: usd, Currency: "USDT", Timestamp: t})
		}
	}

	// Price map only fetched lazily, on the first non-stable coin seen. A
	// flow no price can value is left out rather than booked at a guess, and
	// named in the warnings so its absence from the returns is visible.
	var priceMap map[string]float64
	unpriced := unpricedAssets{}
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
		if qty != 0 {
			unpriced.add(coin)
		}
		return 0
	}
	defer func() { b.cashflowNotes.set(unpriced.markers("binance")) }()

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
	b.fetchExternalTransfers(ctx, since, now, b.measuredWallets(), add, usdValue)
	b.fetchSubAccountAndStrayFlows(ctx, since, now, add, usdValue)
	b.fetchFiatFlows(ctx, since, now, add, usdValue)

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

func (b *Binance) fetchExternalTransfers(ctx context.Context, since, now time.Time, measured map[string]bool, add func(time.Time, float64), usdValue func(string, float64) float64) {
	for typ := range binanceTransferWallets {
		sign := binanceTransferSign(typ, measured)
		if sign == 0 {
			continue
		}
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

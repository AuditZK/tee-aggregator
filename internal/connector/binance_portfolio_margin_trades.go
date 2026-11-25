package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// binancePMVerdict records whether the account runs in Portfolio Margin.
type binancePMVerdict int

const (
	binancePMUnknown binancePMVerdict = iota
	binancePMNo
	binancePMYes
)

// binancePMTradesUnreadable is raised when a Portfolio Margin account's
// fills or funding could not be read from papi: the day is kept on what was
// read, and its trades, volume and fees may be short.
const binancePMTradesUnreadable = "pm_trades_unreadable"

// papi read limits, from the official Portfolio Margin docs
// (developers.binance.com/docs/derivatives/portfolio-margin, mirrored in
// binance/binance-connector-js clients/derivatives-trading-portfolio-margin):
//   - UM Account Trade List, GET /papi/v1/um/userTrades: symbol mandatory,
//     startTime..endTime at most 7 days, limit max 1000, weight 5.
//   - CM Account Trade List, GET /papi/v1/cm/userTrades: symbol or pair,
//     startTime..endTime at most 24 hours, limit max 1000, weight 20.
//   - Margin Account Trade List, GET /papi/v1/margin/myTrades: symbol
//     mandatory, startTime..endTime less than 24 hours, limit max 1000,
//     weight 5.
//   - UM Income History, GET /papi/v1/um/income: weight 30, last three months
//     only, no range cap stated (7 days is its default range, used as the
//     window). CM Income History, GET /papi/v1/cm/income: weight 30, range at
//     most 200 days. Both limit max 1000, no cursor.
//
// None of them pages by cursor alongside a time range, so every read goes
// through readBinanceWindows, which halves a full page.
const (
	binancePMUMFillWindow     = 7 * 24 * time.Hour
	binancePMCMFillWindow     = 24 * time.Hour
	binancePMMarginFillWindow = 24*time.Hour - time.Millisecond
	binancePMUMIncomeWindow   = 7 * 24 * time.Hour
	binancePMCMIncomeWindow   = 200 * 24 * time.Hour
	binancePMPageLimit        = 1000
)

func (b *Binance) portfolioMarginVerdict() binancePMVerdict {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pmVerdict
}

func (b *Binance) setPortfolioMarginVerdict(v binancePMVerdict) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pmVerdict = v
}

// probePortfolioMargin settles the verdict when no balance read did, from the
// error fapi just returned: only its -2015 can hide a Portfolio Margin
// account, and only then is papi asked. A transient papi failure leaves the
// verdict open for the next read.
func (b *Binance) probePortfolioMargin(ctx context.Context, futuresErr error) bool {
	if !isBinancePermissionRefusal(futuresErr) {
		b.setPortfolioMarginVerdict(binancePMNo)
		return false
	}
	body, err := b.doRequest(ctx, "GET", binancePortfolioMarginAPI, "/papi/v1/account", url.Values{}, true)
	if errors.Is(err, ErrTransient) {
		return false
	}
	var acct struct {
		AccountEquity string `json:"accountEquity"`
		ActualEquity  string `json:"actualEquity"`
	}
	if err != nil || json.Unmarshal(body, &acct) != nil || (acct.AccountEquity == "" && acct.ActualEquity == "") {
		b.setPortfolioMarginVerdict(binancePMNo)
		return false
	}
	b.setPortfolioMarginVerdict(binancePMYes)
	return true
}

// binancePMSymbols are the symbols a Portfolio Margin account's fills are
// asked for: the fill lists all need one.
type binancePMSymbols struct {
	um, cm []string
	// income are the symbols the window's UM income names; a spot pair of
	// the same name may have margin fills.
	income []string
}

// getPortfolioMarginTrades reads the window's UM, COIN-M and margin fills. On
// an error it returns the fills read before it.
func (b *Binance) getPortfolioMarginTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	syms, err := b.portfolioMarginSymbols(ctx, start, end)
	if err != nil {
		return nil, err
	}
	var trades []*Trade
	for _, sym := range syms.um {
		fills, err := readBinanceWindows(start, end, binancePMUMFillWindow, binancePMPageLimit, "binance portfolio margin UM fills",
			func(from, to time.Time) ([]binanceFuturesFill, error) {
				var page []binanceFuturesFill
				return page, b.papiPage(ctx, "/papi/v1/um/userTrades", sym, from, to, &page)
			},
			func(f binanceFuturesFill) string { return strconv.FormatInt(f.ID, 10) })
		if err != nil {
			return trades, fmt.Errorf("UM fills %s: %w", sym, err)
		}
		trades = append(trades, binanceUMFillTrades(fills)...)
	}
	for _, sym := range syms.cm {
		fills, err := readBinanceWindows(start, end, binancePMCMFillWindow, binancePMPageLimit, "binance portfolio margin COIN-M fills",
			func(from, to time.Time) ([]binancePMCMFill, error) {
				var page []binancePMCMFill
				return page, b.papiPage(ctx, "/papi/v1/cm/userTrades", sym, from, to, &page)
			},
			func(f binancePMCMFill) string { return strconv.FormatInt(f.ID, 10) })
		if err != nil {
			return trades, fmt.Errorf("COIN-M fills %s: %w", sym, err)
		}
		trades = append(trades, binancePMCMFillTrades(fills)...)
	}
	marginSyms, err := b.portfolioMarginMarginSymbols(ctx, syms.income)
	if err != nil {
		return trades, fmt.Errorf("margin symbols: %w", err)
	}
	for _, sym := range marginSyms {
		fills, err := readBinanceWindows(start, end, binancePMMarginFillWindow, binancePMPageLimit, "binance portfolio margin margin fills",
			func(from, to time.Time) ([]binanceSpotFill, error) {
				var page []binanceSpotFill
				return page, b.papiPage(ctx, "/papi/v1/margin/myTrades", sym, from, to, &page)
			},
			func(f binanceSpotFill) string { return strconv.FormatInt(f.ID, 10) })
		if isBinanceInvalidSymbol(err) {
			// A UM contract with no spot pair of its name (1000PEPEUSDT).
			continue
		}
		if err != nil {
			return trades, fmt.Errorf("margin fills %s: %w", sym, err)
		}
		trades = append(trades, binanceSpotFillTrades(fills)...)
	}
	return trades, nil
}

// papiPage reads one page of a papi fill list for symbol over [from, to].
func (b *Binance) papiPage(ctx context.Context, path, symbol string, from, to time.Time, out any) error {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("startTime", strconv.FormatInt(from.UnixMilli(), 10))
	params.Set("endTime", strconv.FormatInt(to.UnixMilli(), 10))
	params.Set("limit", strconv.Itoa(binancePMPageLimit))
	body, err := b.doRequest(ctx, "GET", binancePortfolioMarginAPI, path, params, true)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// isBinanceInvalidSymbol reports Binance's -1121, a symbol the venue does not
// list.
func isBinanceInvalidSymbol(err error) bool {
	return err != nil && strings.Contains(err.Error(), "-1121")
}

// portfolioMarginSymbols names every UM and COIN-M symbol the window can hold
// fills on: those with an open position, and those the window's income names
// (every fill books a COMMISSION row, every close a REALIZED_PNL one), which
// catches a position opened and closed inside the window.
func (b *Binance) portfolioMarginSymbols(ctx context.Context, start, end time.Time) (binancePMSymbols, error) {
	var out binancePMSymbols
	um, err := b.papiOpenPositionSymbols(ctx, "/papi/v1/um/positionRisk")
	if err != nil {
		return out, fmt.Errorf("UM positions: %w", err)
	}
	cm, err := b.papiOpenPositionSymbols(ctx, "/papi/v1/cm/positionRisk")
	if err != nil {
		return out, fmt.Errorf("COIN-M positions: %w", err)
	}
	umIncome, err := b.papiIncome(ctx, "/papi/v1/um/income", "", start, end, binancePMUMIncomeWindow)
	if err != nil {
		return out, fmt.Errorf("UM income: %w", err)
	}
	cmIncome, err := b.papiIncome(ctx, "/papi/v1/cm/income", "", start, end, binancePMCMIncomeWindow)
	if err != nil {
		return out, fmt.Errorf("COIN-M income: %w", err)
	}
	for _, r := range umIncome {
		if r.Symbol != "" {
			out.income = appendSortedUnique(out.income, r.Symbol)
			um = appendSortedUnique(um, r.Symbol)
		}
	}
	for _, r := range cmIncome {
		if r.Symbol != "" {
			cm = appendSortedUnique(cm, r.Symbol)
		}
	}
	out.um, out.cm = um, cm
	return out, nil
}

func appendSortedUnique(list []string, s string) []string {
	i, found := slices.BinarySearch(list, s)
	if found {
		return list
	}
	return slices.Insert(list, i, s)
}

// papiOpenPositionSymbols returns the symbols holding a non-zero position.
func (b *Binance) papiOpenPositionSymbols(ctx context.Context, path string) ([]string, error) {
	body, err := b.doRequest(ctx, "GET", binancePortfolioMarginAPI, path, url.Values{}, true)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Symbol      string `json:"symbol"`
		PositionAmt string `json:"positionAmt"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	var out []string
	for _, r := range rows {
		if !isZeroAmount(r.PositionAmt) && r.Symbol != "" {
			out = appendSortedUnique(out, r.Symbol)
		}
	}
	return out, nil
}

// portfolioMarginMarginSymbols names the spot pairs the margin fills are
// asked for: each asset the cross margin wallet holds or owes, against USDT
// and USDC, and the pairs the window's UM income names.
func (b *Binance) portfolioMarginMarginSymbols(ctx context.Context, incomeSymbols []string) ([]string, error) {
	body, err := b.doRequest(ctx, "GET", binancePortfolioMarginAPI, "/papi/v1/balance", url.Values{}, true)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Asset               string `json:"asset"`
		CrossMarginAsset    string `json:"crossMarginAsset"`
		CrossMarginBorrowed string `json:"crossMarginBorrowed"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode /papi/v1/balance: %w", err)
	}
	var out []string
	for _, r := range rows {
		if isZeroAmount(r.CrossMarginAsset) && isZeroAmount(r.CrossMarginBorrowed) {
			continue
		}
		asset := strings.ToUpper(r.Asset)
		switch {
		case asset == "USDT" || asset == "USD" || asset == "BNFCR":
		case IsStablecoinUSD(asset):
			out = appendSortedUnique(out, asset+"USDT")
		default:
			out = appendSortedUnique(out, asset+"USDT")
			out = appendSortedUnique(out, asset+"USDC")
		}
	}
	for _, s := range incomeSymbols {
		if strings.HasSuffix(s, "USDT") || strings.HasSuffix(s, "USDC") {
			out = appendSortedUnique(out, s)
		}
	}
	return out, nil
}

// binancePMIncome is one row of /papi/v1/um/income or /papi/v1/cm/income.
// tranId is unique within an incomeType; it is a number in the field and a
// string in Binance's own SDK types, so it is kept raw.
type binancePMIncome struct {
	Symbol     string          `json:"symbol"`
	IncomeType string          `json:"incomeType"`
	Income     string          `json:"income"`
	Asset      string          `json:"asset"`
	Time       int64           `json:"time"`
	TranID     json.RawMessage `json:"tranId"`
}

func (r binancePMIncome) key() string {
	id := strings.Trim(string(r.TranID), `"`)
	if id == "" || id == "null" {
		id = r.Symbol + "@" + strconv.FormatInt(r.Time, 10) + "=" + r.Income + r.Asset
	}
	return r.IncomeType + ":" + id
}

// papiIncome reads an income history over [start, end], every type when
// incomeType is empty.
func (b *Binance) papiIncome(ctx context.Context, path, incomeType string, start, end time.Time, window time.Duration) ([]binancePMIncome, error) {
	return readBinanceWindows(start, end, window, binancePMPageLimit, "binance portfolio margin income",
		func(from, to time.Time) ([]binancePMIncome, error) {
			params := url.Values{}
			if incomeType != "" {
				params.Set("incomeType", incomeType)
			}
			params.Set("startTime", strconv.FormatInt(from.UnixMilli(), 10))
			params.Set("endTime", strconv.FormatInt(to.UnixMilli(), 10))
			params.Set("limit", strconv.Itoa(binancePMPageLimit))
			body, err := b.doRequest(ctx, "GET", binancePortfolioMarginAPI, path, params, true)
			if err != nil {
				return nil, err
			}
			var page []binancePMIncome
			if err := json.Unmarshal(body, &page); err != nil {
				return nil, fmt.Errorf("decode %s: %w", path, err)
			}
			return page, nil
		},
		binancePMIncome.key)
}

// binancePMCMFill is one fill of /papi/v1/cm/userTrades. qty counts
// contracts; baseQty is the same fill in the coin, and commission and
// realizedPnl are in the margin coin.
type binancePMCMFill struct {
	ID              int64  `json:"id"`
	Symbol          string `json:"symbol"`
	Pair            string `json:"pair"`
	Side            string `json:"side"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	BaseQty         string `json:"baseQty"`
	RealizedPnl     string `json:"realizedPnl"`
	MarginAsset     string `json:"marginAsset"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	Time            int64  `json:"time"`
}

// binanceCMContractUSD is the face value of one COIN-M contract: 100 USD on
// the BTC pairs, 10 USD on every other.
func binanceCMContractUSD(pair string) float64 {
	if strings.HasPrefix(strings.ToUpper(pair), "BTC") {
		return 100
	}
	return 10
}

// binancePMCMFillTrades maps COIN-M fills into the swap market. The sync sums
// fees and PnL as dollars, so the coin-denominated commission and PnL are
// valued at the fill's own price (USD per coin); left in the coin, a BTC fee
// read as a few hundredths of a dollar.
func binancePMCMFillTrades(fills []binancePMCMFill) []*Trade {
	trades := make([]*Trade, 0, len(fills))
	for _, f := range fills {
		price, _ := strconv.ParseFloat(f.Price, 64)
		base, _ := strconv.ParseFloat(f.BaseQty, 64)
		contracts, _ := strconv.ParseFloat(f.Qty, 64)
		fee, _ := strconv.ParseFloat(f.Commission, 64)
		pnl, _ := strconv.ParseFloat(f.RealizedPnl, 64)

		t := &Trade{
			ID:          strconv.FormatInt(f.ID, 10),
			Symbol:      f.Symbol,
			Side:        strings.ToLower(f.Side),
			Price:       price,
			Quantity:    base,
			Fee:         fee,
			FeeCurrency: f.CommissionAsset,
			RealizedPnL: pnl,
			Timestamp:   time.UnixMilli(f.Time),
			MarketType:  MarketSwap,
		}
		if base == 0 {
			t.Quantity = contracts
			pair := f.Pair
			if pair == "" {
				pair = f.Symbol
			}
			t.USDNotional = contracts * binanceCMContractUSD(pair)
		}
		coin := strings.ToUpper(f.MarginAsset)
		if !IsStablecoinUSD(f.CommissionAsset) && (coin == "" || strings.EqualFold(f.CommissionAsset, coin)) {
			t.Fee = fee * price
			t.FeeCurrency = "USD"
		}
		if coin != "" && !IsStablecoinUSD(coin) {
			t.RealizedPnL = pnl * price
		}
		trades = append(trades, t)
	}
	return trades
}

// GetFundingFees implements FundingFeesFetcher for a Portfolio Margin
// account: the FUNDING_FEE rows of the UM and COIN-M income since since,
// valued in USD (COIN-M funding is paid in the coin). Any other account
// answers nothing without a request, as it did before this reader existed.
func (b *Binance) GetFundingFees(ctx context.Context, _ []string, since time.Time) ([]*FundingFee, error) {
	if b.portfolioMarginVerdict() != binancePMYes {
		return nil, nil
	}
	now := time.Now().UTC()
	um, err := b.papiIncome(ctx, "/papi/v1/um/income", "FUNDING_FEE", since, now, binancePMUMIncomeWindow)
	if err != nil {
		b.activityNotes.add(binancePMTradesUnreadable)
		return nil, fmt.Errorf("UM funding: %w", err)
	}
	cm, err := b.papiIncome(ctx, "/papi/v1/cm/income", "FUNDING_FEE", since, now, binancePMCMIncomeWindow)
	if err != nil {
		b.activityNotes.add(binancePMTradesUnreadable)
		return nil, fmt.Errorf("COIN-M funding: %w", err)
	}

	var priceMap map[string]float64
	var out []*FundingFee
	for _, r := range append(um, cm...) {
		amt, _ := strconv.ParseFloat(r.Income, 64)
		if amt == 0 {
			continue
		}
		if !IsStablecoinUSD(r.Asset) {
			if priceMap == nil {
				if priceMap, err = FetchBinanceStylePriceMap(ctx, b.client, binanceSpotAPI); err != nil {
					b.activityNotes.add(binancePMTradesUnreadable)
					return nil, fmt.Errorf("price funding: %w", err)
				}
			}
			amt *= priceMap[strings.ToUpper(r.Asset)+"USDT"]
			if amt == 0 {
				continue
			}
		}
		out = append(out, &FundingFee{Amount: amt, Symbol: r.Symbol, Timestamp: time.UnixMilli(r.Time).UTC()})
	}
	return out, nil
}

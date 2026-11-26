package connector

// Live activity of a Binance Portfolio Margin (PM) account: fills, fees and
// funding read from papi, since fapi refuses a PM key with -2015.
//
// Known coverage gaps, accepted:
//   - Margin fills are asked only for the assets /papi/v1/balance shows the
//     cross margin wallet holding or owing now, against USDT and USDC. An
//     asset bought and fully sold inside the window, and any pair quoted in
//     something else (BTC, FDUSD, EUR...), are missed.
//   - A transient papi failure keeps the day on what was read and raises
//     pm_trades_unreadable: the day is short. The activity recount repairs
//     its swap fills and funding later; its margin (spot bucket) fills stay
//     short, the recount does not read that market.
//   - PM options and Portfolio Margin Pro are not read.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
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
	binancePMMarginFillWindow = 24 * time.Hour
	binancePMUMIncomeWindow   = 7 * 24 * time.Hour
	binancePMCMIncomeWindow   = 200 * 24 * time.Hour
	binancePMPageLimit        = 1000
)

// binancePacedWeights is the documented weight of each read that goes
// through the pacer: the papi reads, and the Portfolio Margin Pro reads on
// sapi, fapi and dapi. An unlisted path is charged 20.
var binancePacedWeights = map[string]int{
	"/papi/v1/account":         20,
	"/papi/v1/balance":         20,
	"/papi/v1/um/positionRisk": 5,
	"/papi/v1/cm/positionRisk": 1,
	"/papi/v1/um/userTrades":   5,
	"/papi/v1/cm/userTrades":   20,
	"/papi/v1/margin/myTrades": 5,
	"/papi/v1/um/income":       30,
	"/papi/v1/cm/income":       30,
	// Portfolio Margin Pro. The account read is weighted per UID, not per IP.
	"/sapi/v1/portfolio/account": 5,
	"/sapi/v1/portfolio/balance": 20,
	"/sapi/v1/margin/myTrades":   10,
	"/fapi/v1/income":            30,
	"/dapi/v1/income":            20,
}

// binancePapiBudgetPerMinute caps the papi weight one connector spends per
// minute. Binance documents 6000/min per IP for Portfolio Margin, and the
// enclave's egress IP is shared by every customer: one account stays at a
// fifth of it. A daily sync spends a few hundred; a long recount is what
// reaches the cap and is slowed down rather than banned.
const binancePapiBudgetPerMinute = 1200

// binanceWeightPacer spends a weight budget per fixed one-minute window and
// sleeps out the rest of the window when the next call would overrun it. The
// zero value is ready to use.
type binanceWeightPacer struct {
	mu     sync.Mutex
	budget int
	start  time.Time
	used   int
	now    func() time.Time
	sleep  func(context.Context, time.Duration) error
}

func (p *binanceWeightPacer) wait(ctx context.Context, weight int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	now, sleep, budget := time.Now, sleepCtx, binancePapiBudgetPerMinute
	if p.now != nil {
		now = p.now
	}
	if p.sleep != nil {
		sleep = p.sleep
	}
	if p.budget > 0 {
		budget = p.budget
	}
	t := now()
	if p.start.IsZero() || t.Sub(p.start) >= time.Minute {
		p.start, p.used = t, 0
	}
	if p.used > 0 && p.used+weight > budget {
		if err := sleep(ctx, p.start.Add(time.Minute).Sub(t)); err != nil {
			return err
		}
		p.start, p.used = now(), 0
	}
	p.used += weight
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// papiGet is every signed papi read, paced by its documented weight.
func (b *Binance) papiGet(ctx context.Context, path string, params url.Values) ([]byte, error) {
	return b.pacedGet(ctx, binancePortfolioMarginAPI, path, params)
}

// pacedGet is a signed GET charged to the connector's pacer at the path's
// documented weight.
func (b *Binance) pacedGet(ctx context.Context, base, path string, params url.Values) ([]byte, error) {
	weight, ok := binancePacedWeights[path]
	if !ok {
		weight = 20
	}
	if err := b.papiPacer.wait(ctx, weight); err != nil {
		return nil, err
	}
	return b.doRequest(ctx, "GET", base, path, params, true)
}

// isBinanceThrottled reports a failure that says nothing about the account:
// a retry-exhausted 429/5xx or network error, a 418 IP ban (permanent to
// retryHTTP), or a cancelled context.
func isBinanceThrottled(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrTransient) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "HTTP 418") || strings.Contains(msg, "HTTP 429")
}

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
// account, and only then is papi asked. A throttled papi read leaves the
// verdict open for the next read.
func (b *Binance) probePortfolioMargin(ctx context.Context, futuresErr error) bool {
	if !isBinancePermissionRefusal(futuresErr) {
		b.setPortfolioMarginVerdict(binancePMNo)
		return false
	}
	body, err := b.papiGet(ctx, "/papi/v1/account", url.Values{})
	if isBinanceThrottled(err) {
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

// getPortfolioMarginTrades reads the window's UM, COIN-M and margin fills. On
// an error it returns the fills read before it.
func (b *Binance) getPortfolioMarginTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	um, cm, err := b.portfolioMarginSymbols(ctx, start, end)
	if err != nil {
		return nil, err
	}
	var trades []*Trade
	for _, sym := range um {
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
	for _, sym := range cm {
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
	marginSyms, err := b.marginPoolSymbols(ctx, binancePortfolioMarginAPI, "/papi/v1/balance")
	if err != nil {
		return trades, fmt.Errorf("margin symbols: %w", err)
	}
	marginTrades, err := b.readMarginFills(ctx, binancePortfolioMarginAPI, "/papi/v1/margin/myTrades", marginSyms, start, end)
	return append(trades, marginTrades...), err
}

// readMarginFills reads the margin fills of each symbol over [start, end]
// from a myTrades list (papi's or sapi's: same shape, same "less than 24
// hours" rule, startTime and endTime always sent together). On an error it
// returns the fills read before it.
func (b *Binance) readMarginFills(ctx context.Context, base, path string, symbols []string, start, end time.Time) ([]*Trade, error) {
	var trades []*Trade
	for _, sym := range symbols {
		fills, err := readBinanceWindows(start, end, binancePMMarginFillWindow, binancePMPageLimit, "binance margin fills",
			func(from, to time.Time) ([]binanceSpotFill, error) {
				// "less than 24 hours": a full window stops 1 ms short of the
				// next one, so an exact day still costs a single call.
				if to.Sub(from) >= binancePMMarginFillWindow {
					to = to.Add(-time.Millisecond)
				}
				var page []binanceSpotFill
				return page, b.fillPage(ctx, base, path, sym, from, to, &page)
			},
			func(f binanceSpotFill) string { return strconv.FormatInt(f.ID, 10) })
		if isBinanceThrottled(err) {
			return trades, fmt.Errorf("margin fills %s: %w", sym, err)
		}
		if err != nil {
			// A permanent refusal for one pair (-1121 unlisted, a pair the
			// account cannot trade): the other pairs still count. The
			// connector carries no logger; the code is in err.
			continue
		}
		trades = append(trades, binanceSpotFillTrades(fills)...)
	}
	return trades, nil
}

// papiPage reads one page of a papi fill list for symbol over [from, to].
func (b *Binance) papiPage(ctx context.Context, path, symbol string, from, to time.Time, out any) error {
	return b.fillPage(ctx, binancePortfolioMarginAPI, path, symbol, from, to, out)
}

// fillPage reads one page of a fill list for symbol over [from, to].
func (b *Binance) fillPage(ctx context.Context, base, path, symbol string, from, to time.Time, out any) error {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("startTime", strconv.FormatInt(from.UnixMilli(), 10))
	params.Set("endTime", strconv.FormatInt(to.UnixMilli(), 10))
	params.Set("limit", strconv.Itoa(binancePMPageLimit))
	body, err := b.pacedGet(ctx, base, path, params)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// portfolioMarginSymbols names every UM and COIN-M symbol the window can hold
// fills on: those with an open position, and those the window's income names
// (every fill books a COMMISSION row, every close a REALIZED_PNL one), which
// catches a position opened and closed inside the window.
func (b *Binance) portfolioMarginSymbols(ctx context.Context, start, end time.Time) (um, cm []string, err error) {
	if um, err = b.papiOpenPositionSymbols(ctx, "/papi/v1/um/positionRisk"); err != nil {
		return nil, nil, fmt.Errorf("UM positions: %w", err)
	}
	if cm, err = b.papiOpenPositionSymbols(ctx, "/papi/v1/cm/positionRisk"); err != nil {
		return nil, nil, fmt.Errorf("COIN-M positions: %w", err)
	}
	umIncome, err := b.papiIncome(ctx, "/papi/v1/um/income", "", start, end, binancePMUMIncomeWindow)
	if err != nil {
		return nil, nil, fmt.Errorf("UM income: %w", err)
	}
	cmIncome, err := b.papiIncome(ctx, "/papi/v1/cm/income", "", start, end, binancePMCMIncomeWindow)
	if err != nil {
		return nil, nil, fmt.Errorf("COIN-M income: %w", err)
	}
	for _, r := range umIncome {
		if r.Symbol != "" {
			um = appendSortedUnique(um, r.Symbol)
		}
	}
	for _, r := range cmIncome {
		if r.Symbol != "" {
			cm = appendSortedUnique(cm, r.Symbol)
		}
	}
	return um, cm, nil
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
	body, err := b.papiGet(ctx, path, url.Values{})
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

// marginPoolSymbols names the spot pairs the margin fills are asked for: each
// asset the pool's balance (/papi/v1/balance, or /sapi/v1/portfolio/balance
// on Portfolio Margin Pro, same fields) shows the cross margin wallet
// holding, owing or charging interest on, against USDT and USDC. No pair is
// asked on speculation: each costs weight on an IP every customer shares.
func (b *Binance) marginPoolSymbols(ctx context.Context, base, path string) ([]string, error) {
	body, err := b.pacedGet(ctx, base, path, url.Values{})
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Asset               string `json:"asset"`
		CrossMarginAsset    string `json:"crossMarginAsset"`
		CrossMarginBorrowed string `json:"crossMarginBorrowed"`
		CrossMarginFree     string `json:"crossMarginFree"`
		CrossMarginLocked   string `json:"crossMarginLocked"`
		CrossMarginInterest string `json:"crossMarginInterest"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	var out []string
	for _, r := range rows {
		if isZeroAmount(r.CrossMarginAsset) && isZeroAmount(r.CrossMarginBorrowed) && isZeroAmount(r.CrossMarginFree) &&
			isZeroAmount(r.CrossMarginLocked) && isZeroAmount(r.CrossMarginInterest) {
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
	return b.incomeHistory(ctx, binancePortfolioMarginAPI, path, incomeType, start, end, window)
}

// incomeHistory reads an income list (papi's UM/CM, or fapi's and dapi's on
// Portfolio Margin Pro: same rows) over [start, end].
func (b *Binance) incomeHistory(ctx context.Context, base, path, incomeType string, start, end time.Time, window time.Duration) ([]binancePMIncome, error) {
	return readBinanceWindows(start, end, window, binancePMPageLimit, "binance income",
		func(from, to time.Time) ([]binancePMIncome, error) {
			params := url.Values{}
			if incomeType != "" {
				params.Set("incomeType", incomeType)
			}
			params.Set("startTime", strconv.FormatInt(from.UnixMilli(), 10))
			params.Set("endTime", strconv.FormatInt(to.UnixMilli(), 10))
			params.Set("limit", strconv.Itoa(binancePMPageLimit))
			body, err := b.pacedGet(ctx, base, path, params)
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
// valued in USD (COIN-M funding is paid in the coin), from papi on Portfolio
// Margin and from fapi/dapi on Portfolio Margin Pro. Any other account
// answers ErrFundingNotRead without a request: it reads no funding, and a
// caller that rewrites stored funding must not take that for a zero.
func (b *Binance) GetFundingFees(ctx context.Context, _ []string, since time.Time) ([]*FundingFee, error) {
	now := time.Now().UTC()
	var um, cm []binancePMIncome
	var err error
	switch pro, coinM := b.portfolioMarginProState(); {
	case b.portfolioMarginVerdict() == binancePMYes:
		if um, err = b.papiIncome(ctx, "/papi/v1/um/income", "FUNDING_FEE", since, now, binancePMUMIncomeWindow); err != nil {
			b.activityNotes.add(binancePMTradesUnreadable)
			return nil, fmt.Errorf("UM funding: %w", err)
		}
		if cm, err = b.papiIncome(ctx, "/papi/v1/cm/income", "FUNDING_FEE", since, now, binancePMCMIncomeWindow); err != nil {
			b.activityNotes.add(binancePMTradesUnreadable)
			return nil, fmt.Errorf("COIN-M funding: %w", err)
		}
	case pro == binancePMYes:
		if um, err = b.incomeHistory(ctx, binanceFuturesAPI, "/fapi/v1/income", "FUNDING_FEE", since, now, binancePMUMIncomeWindow); err != nil {
			b.activityNotes.add(binancePMTradesUnreadable)
			return nil, fmt.Errorf("UM funding: %w", err)
		}
		if coinM {
			if cm, err = b.incomeHistory(ctx, binanceCoinMFuturesAPI, "/dapi/v1/income", "FUNDING_FEE", since, now, binancePMCMIncomeWindow); err != nil {
				b.activityNotes.add(binancePMTradesUnreadable)
				return nil, fmt.Errorf("COIN-M funding: %w", err)
			}
		}
	default:
		return nil, ErrFundingNotRead
	}
	return b.fundingFromIncome(ctx, append(um, cm...))
}

// fundingFromIncome values FUNDING_FEE rows in USD, a non-stable asset at
// its spot ticker.
func (b *Binance) fundingFromIncome(ctx context.Context, rows []binancePMIncome) ([]*FundingFee, error) {
	var priceMap map[string]float64
	var out []*FundingFee
	var err error
	for _, r := range rows {
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

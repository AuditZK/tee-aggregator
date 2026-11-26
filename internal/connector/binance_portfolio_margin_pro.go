package connector

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Portfolio Margin Pro (accountType PM_1, and PM_3 for PM Pro SPAN on
// GET /sapi/v1/portfolio/account; PM_2 is the papi flavour handled by the
// Portfolio Margin path) refuses papi with -2015 while fapi and dapi keep
// answering, and Binance values its pool as the "Cross Margin (PM)" and
// "USDⓈ-M Futures (PM)" wallets. GetBalance therefore already measures it,
// through /sapi/v1/margin/account and fapi, and so does the live UM fill
// read. What it did not read is the funding (fapi/dapi income) and the
// pool's margin fills (/sapi/v1/margin/myTrades), and nothing said the
// account was Pro at all.
//
// Detection: /sapi/v1/portfolio/account, weight 5 per UID (not per IP: it
// costs the shared egress nothing), one attempt bounded by
// binancePMProProbeTimeout. It is asked only when:
//   - papi did not answer (PM_2 never reaches it),
//   - the Futures scope works (fapi answered): without it a Pro account's
//     fills and funding cannot be read anyway, and futures_permission_missing
//     already says so,
//   - the cross margin wallet holds or owes anything (gross, not net), since
//     a Pro pool is that wallet. A Pro account whose collateral sits only in
//     its USDⓈ-M wallet is not seen: that wallet alone looks like every
//     classic futures account, and probing all of those is the cost this
//     gate avoids.
//
// It is asked once per connector instance. A Binance error (a JSON code) or
// a 200 naming another accountType settles the verdict as not Pro; a
// throttle, a transport failure, a WAF or proxy page or a truncated body
// leaves it open for the next balance read, and raises pm_trades_unreadable
// so a Pro account whose funding goes unread that day is on record.

// binanceMarkerPMProResidual is raised when the pool GetBalance measured
// (cross margin + USDⓈ-M + COIN-M) and Binance's actualEquity for it differ
// by more than max($50, 0.5%). The stored equity keeps its basis.
const binanceMarkerPMProResidual = "pm_pro_equity_residual"

const (
	binancePMProResidualFloorUSD = 50
	binancePMProResidualShare    = 0.005

	// The probe runs under the balance lock: one attempt, bounded.
	binancePMProProbeTimeout = 10 * time.Second
	// The pool's margin fills get at most this, and never more than a third
	// of what is left of the caller's deadline: the funding and cashflow
	// reads after them share that deadline.
	binancePMProMarginReadCap = 90 * time.Second
)

// binancePMProState is guarded by Binance.mu.
type binancePMProState struct {
	verdict binancePMVerdict
	// coinM is whether the account holds a COIN-M wallet, whose funding is
	// read from dapi.
	coinM bool
	// check is the actualEquity cross-check of the probe, handed out once.
	check BalanceCrossCheck
	// priceMap is the last balance read's ticker map, which prices the pool
	// assets the margin fill read sorts out as dust.
	priceMap map[string]float64
}

// BalanceCrossCheck implements BalanceCrossChecker. The check is taken once
// per connector instance, at its probe, and handed out once: logging it on
// every later sync of the instance would repeat the same numbers.
func (b *Binance) BalanceCrossCheck() (BalanceCrossCheck, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	check := b.pmPro.check
	b.pmPro.check = BalanceCrossCheck{}
	return check, check.Name != ""
}

func (b *Binance) portfolioMarginProState() (binancePMVerdict, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pmPro.verdict, b.pmPro.coinM
}

// settlePortfolioMarginPro runs inside GetBalance (b.mu held) on an account
// papi did not answer for. It never fails the balance read.
func (b *Binance) settlePortfolioMarginPro(ctx context.Context, crossHeld, futuresRead bool, coinMEq, pool float64, priceMap map[string]float64) {
	b.pmPro.priceMap = priceMap
	if b.pmPro.verdict != binancePMUnknown || !crossHeld || !futuresRead {
		return
	}
	status, body, err := b.probeGet(ctx, "/sapi/v1/portfolio/account")
	var acct struct {
		Code         *int   `json:"code"`
		AccountType  string `json:"accountType"`
		ActualEquity string `json:"actualEquity"`
	}
	decoded := err == nil && json.Unmarshal(body, &acct) == nil
	switch {
	case !decoded || status == http.StatusTooManyRequests || status == http.StatusTeapot || status >= 500:
		// Nothing said about the account: ask again on the next read, and
		// leave a trace of the funding this read cannot collect.
		b.capabilityWarnings = append(b.capabilityWarnings, binancePMTradesUnreadable)
		return
	case status != http.StatusOK:
		if acct.Code == nil {
			// A WAF or proxy page that happens to be JSON: not Binance's word.
			b.capabilityWarnings = append(b.capabilityWarnings, binancePMTradesUnreadable)
			return
		}
		b.pmPro.verdict = binancePMNo
		return
	case acct.AccountType != "PM_1" && acct.AccountType != "PM_3":
		b.pmPro.verdict = binancePMNo
		return
	}
	b.pmPro.verdict = binancePMYes
	b.pmPro.coinM = coinMEq != 0

	actual, perr := strconv.ParseFloat(acct.ActualEquity, 64)
	if perr != nil {
		return
	}
	b.pmPro.check = BalanceCrossCheck{Name: "binance_pm_pro_actual_equity", Measured: pool, Reference: actual}
	// Raised on the read that measured it only; later reads of the instance
	// have no new figure to compare.
	if math.Abs(pool-actual) > max(binancePMProResidualFloorUSD, binancePMProResidualShare*math.Abs(actual)) {
		b.capabilityWarnings = append(b.capabilityWarnings, binanceMarkerPMProResidual)
	}
}

// probeGet is one paced, signed attempt at a sapi read, bounded by
// binancePMProProbeTimeout and never retried. It returns the status and body
// of any answer; err is a transport failure.
func (b *Binance) probeGet(ctx context.Context, path string) (int, []byte, error) {
	weight, ok := binancePacedWeights[path]
	if !ok {
		weight = 20
	}
	if err := b.pacer.wait(ctx, weight); err != nil {
		return 0, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, binancePMProProbeTimeout)
	defer cancel()
	params := url.Values{}
	params.Set("recvWindow", "30000")
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("signature", b.sign(params))
	req, err := http.NewRequestWithContext(ctx, "GET", binanceSpotAPI+path+"?"+params.Encode(), nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-MBX-APIKEY", b.apiKey)
	resp, err := b.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

// getPortfolioMarginProMarginTrades reads the margin fills of the pool's
// assets on sapi, under its own deadline: min(binancePMProMarginReadCap, a
// third of what the caller has left). Out of time, it returns what it read
// and an error, and the caller raises pm_trades_unreadable.
func (b *Binance) getPortfolioMarginProMarginTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	budget := binancePMProMarginReadCap
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/3)
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	b.mu.Lock()
	priceMap := b.pmPro.priceMap
	b.mu.Unlock()
	if priceMap == nil {
		pm, err := FetchBinanceStylePriceMap(ctx, b.client, binanceSpotAPI)
		if err != nil {
			return nil, err
		}
		priceMap = pm
	}
	syms, err := b.marginPoolSymbols(ctx, binanceSpotAPI, "/sapi/v1/portfolio/balance", priceMap)
	if err != nil {
		return nil, err
	}
	trades, err := b.readMarginFills(ctx, binanceSpotAPI, "/sapi/v1/margin/myTrades", syms, start, end)
	if err == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = ctx.Err()
	}
	return trades, err
}

// binanceFuturesPositionRow is one position of /fapi/v2/account.
type binanceFuturesPositionRow struct {
	Symbol           string `json:"symbol"`
	PositionAmt      string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	UnrealizedProfit string `json:"unrealizedProfit"`
	Leverage         string `json:"leverage"`
}

// binanceUMPositionMargin is the initial margin the USDⓈ-M positions hold:
// per symbol, the net position at the mark price divided by its leverage.
// A leg's notional at the mark is positionAmt × entryPrice + unrealizedProfit
// exactly (the profit is positionAmt × (mark − entry)), so the account read
// GetBalance already makes is enough. Hedge legs are netted per symbol and a
// symbol with no leverage counts 0, as the history rebuilder does
// (portfolio_margin_held.go), so live and rebuilt days hold the same margin.
func binanceUMPositionMargin(rows []binanceFuturesPositionRow) float64 {
	net := map[string]float64{}
	leverage := map[string]float64{}
	for _, r := range rows {
		amt, _ := strconv.ParseFloat(r.PositionAmt, 64)
		if amt == 0 {
			continue
		}
		entry, _ := strconv.ParseFloat(r.EntryPrice, 64)
		upnl, _ := strconv.ParseFloat(r.UnrealizedProfit, 64)
		net[r.Symbol] += amt*entry + upnl
		if lev, _ := strconv.ParseFloat(r.Leverage, 64); lev > 0 {
			leverage[r.Symbol] = lev
		}
	}
	held := 0.0
	for sym, notional := range net {
		if lev := leverage[sym]; lev > 0 {
			held += math.Abs(notional) / lev
		}
	}
	return held
}

// coinMPositionMargin is the initial margin the COIN-M positions hold, in
// USD: per symbol, the net notional (notionalValue is in the coin) at the
// mark price divided by its leverage, the rebuilder's |contracts| × face ÷
// leverage.
func (b *Binance) coinMPositionMargin(ctx context.Context) (float64, error) {
	body, err := b.pacedGet(ctx, binanceCoinMFuturesAPI, "/dapi/v1/positionRisk", url.Values{})
	if err != nil {
		return 0, err
	}
	var rows []struct {
		Symbol        string `json:"symbol"`
		PositionAmt   string `json:"positionAmt"`
		MarkPrice     string `json:"markPrice"`
		Leverage      string `json:"leverage"`
		NotionalValue string `json:"notionalValue"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return 0, err
	}
	net := map[string]float64{}
	leverage := map[string]float64{}
	for _, r := range rows {
		amt, _ := strconv.ParseFloat(r.PositionAmt, 64)
		if amt == 0 {
			continue
		}
		mark, _ := strconv.ParseFloat(r.MarkPrice, 64)
		coin, _ := strconv.ParseFloat(r.NotionalValue, 64)
		net[r.Symbol] += math.Copysign(math.Abs(coin*mark), amt)
		if lev, _ := strconv.ParseFloat(r.Leverage, 64); lev > 0 {
			leverage[r.Symbol] = lev
		}
	}
	held := 0.0
	for sym, usd := range net {
		if lev := leverage[sym]; lev > 0 {
			held += math.Abs(usd) / lev
		}
	}
	return held, nil
}

// portfolioMarginProSplit is the per-market split of a Portfolio Margin Pro
// account, runs inside GetBalance (b.mu held). It follows the history
// rebuilder's Pro output: swap is the pool (cross margin + USDⓈ-M + COIN-M)
// with its free margin, spot the rest (spot wallet, Earn, isolated margin,
// options) with available = equity.
//
// Under Pro, /fapi/v2/account answers availableBalance 0: the positions'
// margin is drawn from the pool, not from the USDⓈ-M wallet. The classic
// split then kept the cross margin pool in spot at available = equity and
// the USDⓈ-M wallet at 0: the whole pool read as free, nothing deducted for
// the positions. Here free = pool − the positions' initial margin, never
// above the pool, never below 0.
//
// ok is false, and the classic split applies, when the account is not Pro
// or the positions could not be read (the /fapi/v2/balance fallback carries
// none, or the COIN-M positions read failed).
func (b *Binance) portfolioMarginProSplit(ctx context.Context, nonUM, crossEq, coinMEq float64, futures *Balance) ([]*MarketBalance, bool) {
	if b.pmPro.verdict != binancePMYes || futures == nil || !b.umHeldKnown {
		return nil, false
	}
	held := b.umHeld
	if coinMEq != 0 {
		cm, err := b.coinMPositionMargin(ctx)
		if err != nil {
			return nil, false
		}
		held += cm
	}
	poolEq := crossEq + coinMEq + futures.Equity
	free := max(min(poolEq-held, poolEq), 0)
	var split []*MarketBalance
	if spotEq := nonUM - crossEq - coinMEq; spotEq != 0 {
		split = append(split, &MarketBalance{MarketType: MarketSpot, Equity: spotEq, AvailableMargin: spotEq})
	}
	if poolEq != 0 || free != 0 {
		split = append(split, &MarketBalance{MarketType: MarketSwap, Equity: poolEq, AvailableMargin: free})
	}
	return split, true
}

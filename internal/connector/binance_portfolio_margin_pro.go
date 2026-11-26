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

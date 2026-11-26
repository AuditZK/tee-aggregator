package connector

import (
	"context"
	"encoding/json"
	"math"
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
// costs the shared egress nothing), asked only when the cross margin wallet
// holds something, since a Pro pool is that wallet. It is asked once per
// connector instance: a classic account with an empty cross margin wallet
// is never asked, one with cross margin once and remembered as not Pro, a
// Pro account once, its actualEquity cross-check taken from that answer. A
// throttled probe leaves the verdict open for the next balance read.

// binanceMarkerPMProResidual is raised when the pool GetBalance measured
// (cross margin + USDⓈ-M + COIN-M) and Binance's actualEquity for it differ
// by more than max($50, 0.5%). The stored equity keeps its basis.
const binanceMarkerPMProResidual = "pm_pro_equity_residual"

const (
	binancePMProResidualFloorUSD = 50
	binancePMProResidualShare    = 0.005
)

// binancePMProState is guarded by Binance.mu.
type binancePMProState struct {
	verdict binancePMVerdict
	// coinM is whether the account holds a COIN-M wallet, whose funding is
	// read from dapi.
	coinM bool
	check BalanceCrossCheck
	// residual is whether that check exceeded its tolerance; the marker is
	// raised on every balance read of the instance, not only the probing one.
	residual bool
}

// BalanceCrossCheck implements BalanceCrossChecker.
func (b *Binance) BalanceCrossCheck() (BalanceCrossCheck, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pmPro.check, b.pmPro.check.Name != ""
}

func (b *Binance) portfolioMarginProState() (binancePMVerdict, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pmPro.verdict, b.pmPro.coinM
}

// settlePortfolioMarginPro runs inside GetBalance (b.mu held) on an account
// papi did not answer for. It never fails the balance read: a throttled
// probe leaves the verdict open for the next sync, any other failure on a
// first probe reads as not Pro.
func (b *Binance) settlePortfolioMarginPro(ctx context.Context, crossEq, coinMEq, pool float64) {
	// Once per instance: a settled verdict is kept, the cross-check with it.
	if b.pmPro.verdict == binancePMYes && b.pmPro.residual {
		b.capabilityWarnings = append(b.capabilityWarnings, binanceMarkerPMProResidual)
	}
	if b.pmPro.verdict != binancePMUnknown || crossEq == 0 {
		return
	}
	body, err := b.pacedGet(ctx, binanceSpotAPI, "/sapi/v1/portfolio/account", url.Values{})
	if isBinanceThrottled(err) {
		return
	}
	var acct struct {
		AccountType  string `json:"accountType"`
		ActualEquity string `json:"actualEquity"`
	}
	if err != nil || json.Unmarshal(body, &acct) != nil {
		b.pmPro.verdict = binancePMNo
		return
	}
	if acct.AccountType != "PM_1" && acct.AccountType != "PM_3" {
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
	b.pmPro.residual = math.Abs(pool-actual) > max(binancePMProResidualFloorUSD, binancePMProResidualShare*math.Abs(actual))
	if b.pmPro.residual {
		b.capabilityWarnings = append(b.capabilityWarnings, binanceMarkerPMProResidual)
	}
}

// getPortfolioMarginProMarginTrades reads the margin fills of the pool's
// assets on sapi. An asset with no spot pair (USCX) answers -1121, skipped.
func (b *Binance) getPortfolioMarginProMarginTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	syms, err := b.marginPoolSymbols(ctx, binanceSpotAPI, "/sapi/v1/portfolio/balance")
	if err != nil {
		return nil, err
	}
	return b.readMarginFills(ctx, binanceSpotAPI, "/sapi/v1/margin/myTrades", syms, start, end)
}

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const binancePortfolioMarginAPI = "https://papi.binance.com"

var errBinanceNoPortfolioMargin = errors.New("binance portfolio margin account not found")

// isBinancePermissionRefusal reports Binance's -2015. On fapi and dapi a
// Portfolio Margin account answers it exactly like a key without the futures
// scope, so the code alone does not say which of the two it is.
func isBinancePermissionRefusal(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "-2015") || strings.Contains(err.Error(), "Invalid API-key"))
}

// binancePMResidualShare and binancePMResidualFloorUSD bound how far the
// pool's market value may sit from papi's actualEquity before it is taken
// for a misread sub-ledger and actualEquity is kept: the bound beyond which
// the history rebuilder refuses a rebuild (its anchor reconciliation).
const (
	binancePMResidualShare    = 0.005
	binancePMResidualFloorUSD = 50.0
)

// binancePMEquityCheck names the Portfolio Margin pool in an EquityBasisNote.
const binancePMEquityCheck = "binance_pm_market_value"

// getPortfolioMarginBalance reads a Portfolio Margin account, which pools the
// cross margin, USDⓈ-M and COIN-M wallets into one equity in USD.
//
// The equity is the pool at market value: per asset, the cross margin held
// (free + locked) net of the loan, its interest and any negative balance,
// plus the UM and COIN-M wallets and their unrealized PnL, as
// /papi/v1/balance reports them, valued at the spot prices the rest of the
// account is valued at (stablecoins at 1). That is the basis the history
// rebuilder values the pool on, and the live sync's before 2026-11-20.
// actualEquity reads ~0.14% under it on a real account, cause unknown; it is
// kept as the witness, and as the equity of a read whose pool cannot be
// priced whole.
func (b *Binance) getPortfolioMarginBalance(ctx context.Context, priceMap map[string]float64) (*Balance, error) {
	body, err := b.papiGet(ctx, "/papi/v1/account", url.Values{})
	if err != nil {
		return nil, err
	}
	var acct struct {
		AccountEquity        string `json:"accountEquity"`
		ActualEquity         string `json:"actualEquity"`
		AccountInitialMargin string `json:"accountInitialMargin"`
		TotalMarginOpenLoss  string `json:"totalMarginOpenLoss"`
	}
	if err := json.Unmarshal(body, &acct); err != nil {
		return nil, err
	}
	// accountEquity is discounted by each asset's collateral rate; actualEquity
	// is the same account at full value.
	actual, perr := strconv.ParseFloat(acct.ActualEquity, 64)
	if perr != nil {
		actual, perr = strconv.ParseFloat(acct.AccountEquity, 64)
	}
	if perr != nil {
		return nil, errBinanceNoPortfolioMargin
	}

	body, err = b.papiGet(ctx, "/papi/v1/balance", url.Values{})
	if err != nil {
		return nil, err
	}
	var rows []binancePMBalanceRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	unrealized := 0.0
	for _, r := range rows {
		unrealized += binanceMarginAssetUSD(r.Asset, r.UMUnrealizedPNL, priceMap)
		unrealized += binanceMarginAssetUSD(r.Asset, r.CMUnrealizedPNL, priceMap)
	}

	equity, note := binancePMEquity(rows, actual, priceMap)
	b.pmBasis = note

	// totalAvailableBalance starts from the discounted equity, so it reads
	// below the equity on an account holding no position at all. The margin
	// its positions and open orders hold is taken off the equity kept instead.
	initialMargin, _ := strconv.ParseFloat(acct.AccountInitialMargin, 64)
	openOrderMargin, _ := strconv.ParseFloat(acct.TotalMarginOpenLoss, 64)
	available := max(equity-initialMargin-openOrderMargin, 0)

	return &Balance{
		Available:     available,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USDT",
	}, nil
}

// binancePMBalanceRow is one asset of /papi/v1/balance
// (https://developers.binance.com/docs/derivatives/portfolio-margin/account/Account-Balance).
type binancePMBalanceRow struct {
	Asset               string `json:"asset"`
	CrossMarginAsset    string `json:"crossMarginAsset"`
	CrossMarginFree     string `json:"crossMarginFree"`
	CrossMarginLocked   string `json:"crossMarginLocked"`
	CrossMarginBorrowed string `json:"crossMarginBorrowed"`
	CrossMarginInterest string `json:"crossMarginInterest"`
	NegativeBalance     string `json:"negativeBalance"`
	UMWalletBalance     string `json:"umWalletBalance"`
	UMUnrealizedPNL     string `json:"umUnrealizedPNL"`
	CMWalletBalance     string `json:"cmWalletBalance"`
	CMUnrealizedPNL     string `json:"cmUnrealizedPNL"`
}

// netQty is the row's net holding in its own asset. The cross margin side is
// taken as the history rebuilder takes it (fetchPMAnchor): free + locked, or
// crossMarginAsset when both are absent, less the loan, its interest and the
// negative balance.
func (r binancePMBalanceRow) netQty() float64 {
	f := func(s string) float64 {
		v, _ := strconv.ParseFloat(s, 64)
		return v
	}
	held := f(r.CrossMarginFree) + f(r.CrossMarginLocked)
	if held == 0 {
		held = f(r.CrossMarginAsset)
	}
	return held - f(r.CrossMarginBorrowed) - f(r.CrossMarginInterest) - f(r.NegativeBalance) +
		f(r.UMWalletBalance) + f(r.UMUnrealizedPNL) + f(r.CMWalletBalance) + f(r.CMUnrealizedPNL)
}

// binancePMEquity values the pool at market value and returns it, or actual
// when the pool cannot be priced whole or its value sits too far from actual
// to trust; the note says which, and why.
func binancePMEquity(rows []binancePMBalanceRow, actual float64, priceMap map[string]float64) (float64, EquityBasisNote) {
	note := EquityBasisNote{Name: binancePMEquityCheck, Basis: EquityBasisVenue}
	value := 0.0
	var unpriced []string
	for _, r := range rows {
		q := r.netQty()
		if q == 0 {
			continue
		}
		p, ok := SpotPriceUSD(r.Asset, priceMap)
		if !ok {
			unpriced = append(unpriced, strings.ToUpper(strings.TrimSpace(r.Asset)))
			continue
		}
		value += q * p
	}
	if len(unpriced) > 0 {
		// Valued at 0, an unpriced asset would come off the equity whole. The
		// note reaches the log, so it counts the assets and does not name what
		// the user holds.
		slices.Sort(unpriced)
		n := len(slices.Compact(unpriced))
		note.Fallback = fmt.Sprintf("%d assets without a spot price", n)
		if n == 1 {
			note.Fallback = "1 asset without a spot price"
		}
		return actual, note
	}
	note.Residual = value - actual
	if actual != 0 {
		note.ResidualShare = note.Residual / actual
	}
	if math.Abs(note.Residual) > max(binancePMResidualShare*math.Abs(actual), binancePMResidualFloorUSD) {
		note.Fallback = "market value off actualEquity by more than max(0.5%, $50)"
		return actual, note
	}
	note.Basis = EquityBasisMarketValue
	return value, note
}

// EquityBasisNote implements EquityBasisReporter with the note of the last
// Portfolio Margin balance read, handed out once.
func (b *Binance) EquityBasisNote() (EquityBasisNote, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	note := b.pmBasis
	b.pmBasis = EquityBasisNote{}
	return note, note.Name != ""
}

// portfolioMarginAfterRefusal tries the Portfolio Margin read only when fapi
// refused the key, so an account fapi answers never reaches papi. A nil
// balance with a nil error means the account is not in Portfolio Margin.
func (b *Binance) portfolioMarginAfterRefusal(ctx context.Context, futuresErr error, priceMap map[string]float64) (*Balance, error) {
	if !isBinancePermissionRefusal(futuresErr) {
		return nil, nil
	}
	pm, err := b.getPortfolioMarginBalance(ctx, priceMap)
	if isBinanceThrottled(err) {
		// A 418 IP ban is a permanent status to retryHTTP, but it says
		// nothing about the account: read as "not Portfolio Margin", a real
		// PM account would be written that day without its futures pool.
		if !errors.Is(err, ErrTransient) {
			err = fmt.Errorf("%w: %w", ErrTransient, err)
		}
		return nil, err
	}
	if err != nil {
		return nil, nil
	}
	return pm, nil
}

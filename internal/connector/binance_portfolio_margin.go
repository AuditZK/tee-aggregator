package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
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

// getPortfolioMarginBalance reads a Portfolio Margin account, which pools the
// cross margin, USDⓈ-M and COIN-M wallets into one equity in USD.
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
	equity, perr := strconv.ParseFloat(acct.ActualEquity, 64)
	if perr != nil {
		equity, perr = strconv.ParseFloat(acct.AccountEquity, 64)
	}
	if perr != nil {
		return nil, errBinanceNoPortfolioMargin
	}
	// totalAvailableBalance starts from the discounted equity, so it reads
	// below the equity on an account holding no position at all. The margin
	// its positions and open orders hold is taken off the full value instead.
	initialMargin, _ := strconv.ParseFloat(acct.AccountInitialMargin, 64)
	openOrderMargin, _ := strconv.ParseFloat(acct.TotalMarginOpenLoss, 64)
	available := max(equity-initialMargin-openOrderMargin, 0)

	body, err = b.papiGet(ctx, "/papi/v1/balance", url.Values{})
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Asset           string `json:"asset"`
		UMUnrealizedPNL string `json:"umUnrealizedPNL"`
		CMUnrealizedPNL string `json:"cmUnrealizedPNL"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	unrealized := 0.0
	for _, r := range rows {
		unrealized += binanceMarginAssetUSD(r.Asset, r.UMUnrealizedPNL, priceMap)
		unrealized += binanceMarginAssetUSD(r.Asset, r.CMUnrealizedPNL, priceMap)
	}

	return &Balance{
		Available:     available,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USDT",
	}, nil
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

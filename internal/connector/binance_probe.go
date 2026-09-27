package connector

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"strconv"
	"strings"
)

// binanceFuturesAssetRow is one per-asset line of /fapi/v2/account, verbatim.
type binanceFuturesAssetRow struct {
	Asset            string `json:"asset"`
	WalletBalance    string `json:"walletBalance"`
	MarginBalance    string `json:"marginBalance"`
	UnrealizedProfit string `json:"unrealizedProfit"`
	AvailableBalance string `json:"availableBalance"`
}

// binanceTotalsCoverUSDTOnly reports whether /fapi/v2/account's total* fields
// leave the other margin assets out. They are USDT-only in single-asset mode
// and USD-denominated in multi-assets mode, and the account payload does not
// name the mode: when the wallet total equals the USDT wallet alone, anything
// else the account holds is missing from it.
func binanceTotalsCoverUSDTOnly(totalWallet string, assets []binanceFuturesAssetRow) bool {
	total, err := strconv.ParseFloat(totalWallet, 64)
	if err != nil {
		return false
	}
	for _, a := range assets {
		if strings.EqualFold(a.Asset, "USDT") {
			usdt, _ := strconv.ParseFloat(a.WalletBalance, 64)
			return math.Abs(total-usdt) < 1e-8
		}
	}
	return total == 0
}

// binanceMarginAssetUSD values a futures margin asset: stablecoins (BNFCR
// included) at par, other coins at the spot ticker.
func binanceMarginAssetUSD(asset, qty string, priceMap map[string]float64) float64 {
	q, _ := strconv.ParseFloat(qty, 64)
	if q == 0 {
		return 0
	}
	asset = strings.ToUpper(asset)
	if IsStablecoinUSD(asset) {
		return q
	}
	return q * priceMap[asset+"USDT"]
}

// ProbeBalance returns the USDⓈ-M account payload as Binance sent it, the
// account's asset mode, and the Balance GetBalance derives today. It also
// states how much margin collateral the account totals leave out when they
// cover USDT only (single-asset mode, or a BNFCR credit account) — the
// amount a sync counting every margin asset would add to the equity. Nothing
// here changes what a sync records.
func (b *Binance) ProbeBalance(ctx context.Context) (*BalanceProbe, error) {
	probe := &BalanceProbe{
		Exchange:    b.Exchange(),
		MarginBasis: "fapi/v2/account totalMarginBalance + spot + coin-m + cross + isolated margin",
		Account:     map[string]string{},
		Currencies:  []map[string]string{},
	}

	if body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v1/multiAssetsMargin", url.Values{}, true); err == nil {
		var mode struct {
			MultiAssetsMargin *bool `json:"multiAssetsMargin"`
		}
		if json.Unmarshal(body, &mode) == nil && mode.MultiAssetsMargin != nil {
			probe.AccountMode = "multiAssetsMargin=" + strconv.FormatBool(*mode.MultiAssetsMargin)
		}
	} else {
		probe.Notes = append(probe.Notes, "asset mode unavailable: "+vendorErrorDetail(err.Error()))
	}

	body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v2/account", url.Values{}, true)
	if err != nil {
		probe.Notes = append(probe.Notes, "futures account unavailable: "+vendorErrorDetail(err.Error()))
	} else {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			probe.Notes = append(probe.Notes, "futures account unreadable")
		} else {
			for _, k := range []string{
				"totalWalletBalance", "totalMarginBalance", "totalUnrealizedProfit",
				"availableBalance", "totalCrossWalletBalance", "maxWithdrawAmount",
			} {
				if v, ok := raw[k]; ok {
					probe.Account[k] = strings.Trim(string(v), `"`)
				}
			}
			var assets []binanceFuturesAssetRow
			_ = json.Unmarshal(raw["assets"], &assets)
			priceMap, perr := FetchBinanceStylePriceMap(ctx, b.client, binanceSpotAPI)
			if perr != nil {
				probe.Notes = append(probe.Notes, "spot prices unavailable: non-stable collateral valued at 0")
			}
			usdtOnly := binanceTotalsCoverUSDTOnly(probe.Account["totalWalletBalance"], assets)
			outside := 0.0
			for _, a := range assets {
				if isZeroAmount(a.WalletBalance) && isZeroAmount(a.MarginBalance) {
					continue
				}
				probe.Currencies = append(probe.Currencies, map[string]string{
					"asset":            a.Asset,
					"walletBalance":    a.WalletBalance,
					"marginBalance":    a.MarginBalance,
					"unrealizedProfit": a.UnrealizedProfit,
					"availableBalance": a.AvailableBalance,
				})
				if usdtOnly && !strings.EqualFold(a.Asset, "USDT") {
					outside += binanceMarginAssetUSD(a.Asset, a.MarginBalance, priceMap)
				}
			}
			probe.Account["probe.totals_cover_usdt_only"] = strconv.FormatBool(usdtOnly)
			probe.Account["probe.collateral_outside_totals_usd"] = strconv.FormatFloat(outside, 'f', 2, 64)
		}
	}

	derived, err := b.GetBalance(ctx)
	if err != nil {
		probe.Notes = append(probe.Notes, "balance unavailable: "+vendorErrorDetail(err.Error()))
	} else {
		probe.Derived = derived
	}
	return probe, nil
}

func isZeroAmount(s string) bool {
	v, err := strconv.ParseFloat(s, 64)
	return err != nil || v == 0
}

package connector

import (
	"math"
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

func isZeroAmount(s string) bool {
	v, err := strconv.ParseFloat(s, 64)
	return err != nil || v == 0
}

package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Binance values every wallet of an account itself at
// GET /sapi/v1/asset/wallet/balance (Query User Wallet Balance, weight 60,
// quoteAsset defaults to BTC; Binance documents the Universal Transfer
// permission as required, so a key without it is refused and the check stays
// silent). GetBalance reads each wallet it measures from its own endpoint and
// never reads Funding, Trading Bots or Copy Trading. One call to this
// endpoint after those reads says whether money sits where the equity does
// not look, and whether the wallets the equity does read add up to it.
// Markers only: the equity is never changed from this answer.
const (
	binanceMarkerUnmeasuredWallet = "unmeasured_wallet"
	binanceMarkerWalletMismatch   = "wallet_total_mismatch"

	// An unread wallet counts above max($10, 0.5% of the measured equity):
	// dust left in Funding after a withdrawal is not worth a banner.
	binanceUnmeasuredFloorUSD = 10
	binanceUnmeasuredShare    = 0.005
	// The read wallets disagree with the equity above max($25, 1%). The
	// known benign gaps sit well under 1%: the Portfolio Margin pool read as
	// actualEquity runs about 0.15% under Binance's valuation, spot dust no
	// ticker prices, and the seconds between the reads.
	binanceMismatchFloorUSD = 25
	binanceMismatchShare    = 0.01

	// The check is one attempt, never retried, bounded well under the sync's
	// own timeouts.
	binanceWalletCheckTimeout = 5 * time.Second
)

// binanceWalletRead says, per walletName normalised by binanceWalletKey,
// whether GetBalance measures that wallet. A name not listed counts as not
// measured: a wallet Binance adds later must not hide money.
var binanceWalletRead = map[string]bool{
	"spot":           true,
	"crossmargin":    true,
	"crossmarginpm":  true,
	"isolatedmargin": true,
	"usdmfutures":    true, // "USDⓈ-M Futures": the Ⓢ is not ASCII
	"usdsmfutures":   true,
	"coinmfutures":   true,
	// Binance's Earn wallet holds more than Simple Earn (staking, dual
	// investment); the part GetBalance does not read shows as a mismatch,
	// not as an unread wallet.
	"earn":    true,
	"options": true,
	"funding": false, "tradingbots": false, "copytrading": false,
}

// binanceWalletKey keeps the ASCII letters of a walletName, lowercased:
// "USDⓈ-M Futures" → "usdmfutures", "Cross Margin (PM)" → "crossmarginpm".
func binanceWalletKey(name string) string {
	var sb strings.Builder
	for _, r := range strings.ToLower(name) {
		if r >= 'a' && r <= 'z' {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

type binanceWalletValue struct {
	WalletName string `json:"walletName"`
	Balance    string `json:"balance"`
}

// binanceWalletMarkers compares Binance's own wallet valuation with what the
// balance read measured. checkTotal is false when a wallet the balance read
// tries was unreadable: that gap is already reported, and the total would
// repeat it.
func binanceWalletMarkers(wallets []binanceWalletValue, equity, unrealized float64, checkTotal bool) []string {
	var out []string
	unread, read := 0.0, 0.0
	for _, w := range wallets {
		v, err := strconv.ParseFloat(w.Balance, 64)
		if err != nil {
			continue
		}
		if binanceWalletRead[binanceWalletKey(w.WalletName)] {
			read += v
		} else if v > 0 {
			unread += v
		}
	}
	if unread > max(binanceUnmeasuredFloorUSD, binanceUnmeasuredShare*math.Abs(equity)) {
		out = append(out, binanceMarkerUnmeasuredWallet)
	}
	if checkTotal {
		tol := max(binanceMismatchFloorUSD, binanceMismatchShare*math.Abs(equity))
		// Whether Binance's futures rows carry the unrealized PnL is not
		// documented: either reading of it agreeing is agreement.
		if math.Abs(read-equity) > tol && math.Abs(read-(equity-unrealized)) > tol {
			out = append(out, binanceMarkerWalletMismatch)
		}
	}
	return out
}

// walletCoverageMarkers asks Binance for its wallet valuation once and
// returns the markers it supports. Any failure, a ban included, returns
// nothing: this check must never fail, retry or slow the sync.
func (b *Binance) walletCoverageMarkers(ctx context.Context, equity, unrealized float64, checkTotal bool) []string {
	timeout := b.walletCheckTimeout
	if timeout <= 0 {
		timeout = binanceWalletCheckTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	body, err := b.signedGetOnce(ctx, binanceSpotAPI, "/sapi/v1/asset/wallet/balance", url.Values{"quoteAsset": {"USDT"}})
	if err != nil {
		return nil
	}
	var wallets []binanceWalletValue
	if json.Unmarshal(body, &wallets) != nil || len(wallets) == 0 {
		return nil
	}
	return binanceWalletMarkers(wallets, equity, unrealized, checkTotal)
}

// signedGetOnce is a single signed GET with no retry, for a read that is
// worth one attempt and no more.
func (b *Binance) signedGetOnce(ctx context.Context, baseURL, path string, params url.Values) ([]byte, error) {
	params.Set("recvWindow", "30000")
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("signature", b.sign(params))
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+path+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MBX-APIKEY", b.apiKey)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, TruncatedBody(body))
	}
	return body, nil
}

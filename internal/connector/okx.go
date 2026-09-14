package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// okxRegionalHosts lists the OKX API domains in probe order.
//
// OKX runs one order book per regulatory entity, and an API key only exists
// on the entity that issued it: a key created on www.okx.com (global) answers
// only there, a key from OKX Europe (accounts registered on my.okx.com, the
// MiCA entity) only on eea.okx.com, a key from OKX US / AU (app.okx.com) only
// on us.okx.com. Every other domain answers 401 code 50119 "API key doesn't
// exist" for it.
//
// Nothing in the credential shape says which entity issued a key and the
// connect form does not ask, so the connector probes: the first domain that
// recognises the key is pinned for the lifetime of the connector. The cost is
// one refused request per region skipped, once per connector instance, i.e.
// once per sync for a non-global account. Observed 2026-08-29: a French
// signup's OKX Europe key was refused three times on the global domain and
// reported to them as bad credentials.
var okxRegionalHosts = []string{
	"https://www.okx.com", // global
	"https://eea.okx.com", // OKX Europe: accounts registered on my.okx.com
	"https://us.okx.com",  // OKX US / AU: accounts registered on app.okx.com
}

// okxUnknownKeyCodes are the OKX error codes meaning "this domain has never
// seen this key": the only signal worth trying the next region on. Every
// other rejection (bad signature 50113, wrong passphrase 50105, IP not
// whitelisted 50110) proves the key exists on the domain that answered, so
// probing further would only turn a precise error into "doesn't exist".
var okxUnknownKeyCodes = map[string]bool{
	"50119": true, // "API key doesn't exist"
	"50111": true, // "Invalid OK-ACCESS-KEY"
}

// OKX implements Connector for OKX exchange
type OKX struct {
	apiKey     string
	apiSecret  string
	passphrase string
	client     *http.Client

	mu    sync.Mutex
	hosts []string // candidate API domains; collapses to one once a key is recognised

	cashflowWarnings []string // markers from the last GetCashflows, guarded by mu
}

// NewOKX creates a new OKX connector
func NewOKX(creds *Credentials) *OKX {
	return newOKXWithHosts(creds, &http.Client{Timeout: 30 * time.Second}, okxRegionalHosts)
}

// newOKXWithHosts is the constructor the tests use to point the connector at
// fake regional hosts.
func newOKXWithHosts(creds *Credentials, client *http.Client, hosts []string) *OKX {
	return &OKX{
		apiKey:     creds.APIKey,
		apiSecret:  creds.APISecret,
		passphrase: creds.Passphrase,
		client:     client,
		hosts:      append([]string(nil), hosts...),
	}
}

func (o *OKX) Exchange() string {
	return "okx"
}

func (o *OKX) sign(timestamp, method, path, body string) string {
	return signHMACBase64(o.apiSecret, timestamp+method+path+body)
}

// candidateHosts returns the domains still in play, in probe order.
func (o *OKX) candidateHosts() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.hosts...)
}

// pinHost keeps only the domain that recognised the key: every later call on
// this connector goes straight there and no other region is probed again.
func (o *OKX) pinHost(host string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.hosts = []string{host}
}

func (o *OKX) doRequest(ctx context.Context, method, path string) ([]byte, error) {
	hosts := o.candidateHosts()
	if len(hosts) == 0 {
		return nil, errors.New("okx: no API host configured")
	}

	var err error
	for i, host := range hosts {
		var body []byte
		body, err = o.doRequestAt(ctx, host, method, path)
		if err == nil {
			o.pinHost(host)
			return body, nil
		}
		if !okxKeyUnknownHere(body, err) {
			return nil, err
		}
		if i == len(hosts)-1 && i > 0 {
			return nil, fmt.Errorf("okx: API key unknown on every OKX region (%s): %w",
				strings.Join(okxHostNames(hosts), ", "), err)
		}
	}
	return nil, err
}

// doRequestAt signs and sends one request against host. On failure the
// response body comes back alongside the error so the caller can read the
// OKX error code out of it: retryHTTP folds the body into the error text and
// does not retry a 401, so nothing is lost by looking at it.
func (o *OKX) doRequestAt(ctx context.Context, host, method, path string) ([]byte, error) {
	body, err := retryHTTP(o.client, func() (*http.Request, error) {
		timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		signature := o.sign(timestamp, method, path, "")

		req, err := http.NewRequestWithContext(ctx, method, host+path, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("OK-ACCESS-KEY", o.apiKey)
		req.Header.Set("OK-ACCESS-SIGN", signature)
		req.Header.Set("OK-ACCESS-TIMESTAMP", timestamp)
		req.Header.Set("OK-ACCESS-PASSPHRASE", o.passphrase)
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return body, err
	}

	var result struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
	}
	json.Unmarshal(body, &result)
	if result.Code != "0" {
		return body, fmt.Errorf("okx API error: %s", vendorErrorDetail(result.Msg))
	}

	return body, nil
}

// okxKeyUnknownHere reports whether a failed request means the answering
// domain has never issued the key, the one case where another region can
// succeed. OKX answers HTTP 401 with a JSON envelope for it, so the body is
// checked first; the error text is the fallback for the paths where only the
// folded body survives.
func okxKeyUnknownHere(body []byte, err error) bool {
	if err == nil {
		return false
	}
	var envelope struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(body, &envelope) == nil && envelope.Code != "" {
		return okxUnknownKeyCodes[envelope.Code]
	}
	msg := err.Error()
	for code := range okxUnknownKeyCodes {
		if strings.Contains(msg, `"code":"`+code+`"`) {
			return true
		}
	}
	return false
}

// okxHostNames strips the scheme for error messages: "www.okx.com" reads as a
// region, "https://www.okx.com" reads as a URL somebody should click.
func okxHostNames(hosts []string) []string {
	names := make([]string, 0, len(hosts))
	for _, h := range hosts {
		names = append(names, strings.TrimPrefix(strings.TrimPrefix(h, "https://"), "http://"))
	}
	return names
}

func (o *OKX) TestConnection(ctx context.Context) error {
	_, err := o.doRequest(ctx, "GET", "/api/v5/account/balance")
	return err
}

// okxBalanceResponse is GET /api/v5/account/balance. Every figure is kept as
// the raw string OKX sent, because the emptiness of a field is data: OKX
// documents that `"" will be returned for inapplicable fields under the current
// account level`, which is what lets okxReadMargin tell the account modes apart
// without a second request.
type okxBalanceResponse struct {
	Data []okxAccountBalance `json:"data"`
}

// okxAccountBalance is one account-level entry of the balance response.
type okxAccountBalance struct {
	UTime    string               `json:"uTime"`
	TotalEq  string               `json:"totalEq"`
	IsoEq    string               `json:"isoEq"`
	AdjEq    string               `json:"adjEq"`
	AvailEq  string               `json:"availEq"`
	OrdFroz  string               `json:"ordFroz"`
	IMR      string               `json:"imr"`
	MMR      string               `json:"mmr"`
	MgnRatio string               `json:"mgnRatio"`
	UPL      string               `json:"upl"`
	Details  []okxCurrencyBalance `json:"details"`
}

// okxCurrencyBalance is one currency line of the balance response.
type okxCurrencyBalance struct {
	Ccy       string `json:"ccy"`
	Eq        string `json:"eq"`
	EqUsd     string `json:"eqUsd"`
	CashBal   string `json:"cashBal"`
	AvailBal  string `json:"availBal"`
	AvailEq   string `json:"availEq"`
	FrozenBal string `json:"frozenBal"`
	IsoEq     string `json:"isoEq"`
	UPL       string `json:"upl"`
	IMR       string `json:"imr"`
	MMR       string `json:"mmr"`
}

// OKX account modes, and which field carries free margin in each.
//
// acctLv comes from GET /api/v5/account/config — 1 Spot mode, 2 Futures mode
// (the old single-currency margin), 3 Multi-currency margin, 4 Portfolio
// margin. The balance endpoint does not repeat it, but it does not need to:
// "Distribution of applicable fields under each account level" in the v5 docs
// maps every field to the modes it exists in, and the same page states that
// `"" will be returned for inapplicable fields under the current account
// level`. The mode is therefore readable off the payload itself.
//
//	field                acctLv 1   acctLv 2   acctLv 3   acctLv 4
//	                     spot       futures    multi-ccy  portfolio
//	totalEq              yes        yes        yes        yes
//	adjEq                yes        —          yes        yes
//	imr                  yes        —          yes        yes
//	upl        (account) —          —          yes        yes
//	availEq    (account) —          —          yes        yes
//	> availEq  (per ccy) —          yes        yes        yes
//	> upl      (per ccy) —          yes        yes        yes
//	> availBal (per ccy) yes        yes        yes        yes
//
// Free margin, per mode:
//
//   - acctLv 3 / 4 — adjEq − imr, both already in USD. adjEq is "the net fiat
//     value of the assets in the account that can provide margins for spot,
//     expiry futures, perpetual futures and options under the cross-margin
//     mode"; imr is "the sum of initial margins of all open positions and
//     pending orders under cross-margin mode". imr already covers resting
//     orders, so ordFroz must not be subtracted on top of it. OKX checks an
//     order against exactly this pair: "the order didn't pass delta
//     verification because if the order were to succeed, the change in adjEq
//     would be smaller than the change in IMR".
//   - acctLv 2 — Σ per-currency availEq, "available equity of currency", priced
//     into USD. The account-level USD trio does not exist in this mode.
//   - acctLv 1 — Σ per-currency availBal. A spot account carries no cross
//     position, so the balance not locked by a resting order IS the free
//     margin, and free == equity on an account with nothing on the book is the
//     correct answer rather than a bug.
//
// Unrealized P&L follows the same split: account-level upl (USD, "unrealized
// PnL across all open cross-margin positions at the account level") in modes
// 3/4, Σ per-currency upl priced into USD otherwise. Per-currency upl can read
// 0 on an account whose account-level upl does not, which is why the two are
// not interchangeable.
//
// What this must never go back to: Σ availBal in modes 2/3/4. availBal is
// "available balance of currency" — the balance not locked by ORDERS. It does
// not deduct the margin held by open cross positions, so on a margin account
// it sums to the entire equity. Live proof, 2026-09-11: an OKX account running
// a short options straddle against a perpetual hedge reported 22 077 free out
// of 22 077 equity, and every OKX row written between 2026-08-31 and 2026-09-14
// had free margin exactly equal to equity.
const (
	okxBasisAdjEqMinusIMR = "account.adjEq - account.imr"
	okxBasisSumAvailEq    = "sum(details.availEq * usd_rate)"
	okxBasisSumAvailBal   = "sum(details.availBal * usd_rate)"

	okxModeCrossUSD = "multi-currency or portfolio margin (acctLv 3/4)"
	okxModeFutures  = "futures / single-currency margin (acctLv 2)"
	okxModeSpot     = "spot (acctLv 1)"
)

// okxMarginReading is what one balance payload says about free margin, plus
// the field pair it was read from — the probe endpoint reports both so an
// operator can see which branch fired on a real account.
type okxMarginReading struct {
	Available  float64
	Unrealized float64
	Basis      string
	Mode       string
}

// okxReadMargin picks the margin-aware fields for the mode the payload is in.
// See the table above for the mapping and the doc quotes behind it.
func okxReadMargin(account okxAccountBalance) okxMarginReading {
	// Modes 3 and 4. Account-level upl is the discriminator: it is the one of
	// the three that OKX leaves empty in spot mode, which otherwise also
	// carries adjEq and imr (spot borrowing) and would take this branch and
	// report its haircut-discounted collateral value as free cash.
	if account.AdjEq != "" && account.IMR != "" && account.UPL != "" {
		adjEq, okAdjEq := okxFloat(account.AdjEq)
		imr, okIMR := okxFloat(account.IMR)
		if okAdjEq && okIMR {
			upl, _ := okxFloat(account.UPL)
			free := adjEq - imr
			if free < 0 {
				// imr above adjEq means the account is past its initial-margin
				// budget. There is no margin free, not a negative amount of it.
				free = 0
			}
			return okxMarginReading{
				Available:  free,
				Unrealized: upl,
				Basis:      okxBasisAdjEqMinusIMR,
				Mode:       okxModeCrossUSD,
			}
		}
	}

	var sumAvailEq, sumAvailBal, sumUPL float64
	haveAvailEq := false
	for _, d := range account.Details {
		rate := okxUSDRate(d.Eq, d.EqUsd)
		if v, ok := okxFloat(d.AvailEq); ok {
			sumAvailEq += v * rate
			haveAvailEq = true
		}
		if v, ok := okxFloat(d.AvailBal); ok {
			sumAvailBal += v * rate
		}
		if v, ok := okxFloat(d.UPL); ok {
			sumUPL += v * rate
		}
	}

	// Mode 2: availEq is the per-currency figure that already has position
	// margin taken out of it.
	if haveAvailEq {
		return okxMarginReading{
			Available:  sumAvailEq,
			Unrealized: sumUPL,
			Basis:      okxBasisSumAvailEq,
			Mode:       okxModeFutures,
		}
	}

	// Mode 1, and the last resort for any payload that carries neither of the
	// margin-aware sets — a spot balance is all availBal has ever measured
	// correctly.
	return okxMarginReading{
		Available:  sumAvailBal,
		Unrealized: sumUPL,
		Basis:      okxBasisSumAvailBal,
		Mode:       okxModeSpot,
	}
}

// okxFloat parses one OKX numeric string. A field that does not apply to the
// account's mode comes back as "", and that must not read as a measured zero:
// the bool is what the mode detection keys off.
func okxFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// fetchAccountBalance returns the first (and only) account entry of the
// balance response, or nil when OKX answered with no account at all.
func (o *OKX) fetchAccountBalance(ctx context.Context) (*okxAccountBalance, error) {
	body, err := o.doRequest(ctx, "GET", "/api/v5/account/balance")
	if err != nil {
		return nil, err
	}

	var resp okxBalanceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if len(resp.Data) == 0 {
		return nil, nil
	}
	return &resp.Data[0], nil
}

func (o *OKX) GetBalance(ctx context.Context) (*Balance, error) {
	account, err := o.fetchAccountBalance(ctx)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return &Balance{Currency: "USDT"}, nil
	}

	// Equity stays totalEq — total account assets in USD, the one field every
	// mode fills in. adjEq is a collateral valuation, not the account's worth,
	// and swapping it in here would move the equity curve of every OKX user.
	equity, _ := strconv.ParseFloat(account.TotalEq, 64)
	reading := okxReadMargin(*account)

	// The "free margin never exceeds equity" invariant is held one layer up,
	// in service.clampAvailableMargin, so it applies to every venue and to the
	// reconstruction path as well as this one.
	return &Balance{
		Available:     reading.Available,
		Equity:        equity,
		UnrealizedPnL: reading.Unrealized,
		Currency:      "USDT",
	}, nil
}

// okxUSDRate prices one currency line in USD from the pair OKX already returns
// on it. Reading free margin and unrealized P&L off the USDT line alone left
// every account settled in anything else reporting a free margin of zero while
// its total equity stayed right. Falling back to 1 keeps the USD-pegged lines
// correct when OKX omits eqUsd.
func okxUSDRate(eq, eqUSD string) float64 {
	quantity, _ := strconv.ParseFloat(eq, 64)
	usd, _ := strconv.ParseFloat(eqUSD, 64)
	if quantity == 0 || usd == 0 {
		return 1
	}
	return usd / quantity
}

func (o *OKX) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := o.doRequest(ctx, "GET", "/api/v5/account/positions")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			InstId   string `json:"instId"`
			PosSide  string `json:"posSide"`
			Pos      string `json:"pos"`
			AvgPx    string `json:"avgPx"`
			MarkPx   string `json:"markPx"`
			Upl      string `json:"upl"`
			InstType string `json:"instType"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var positions []*Position
	for _, p := range resp.Data {
		size, _ := strconv.ParseFloat(p.Pos, 64)
		if size == 0 {
			continue
		}

		entry, _ := strconv.ParseFloat(p.AvgPx, 64)
		mark, _ := strconv.ParseFloat(p.MarkPx, 64)
		unrealized, _ := strconv.ParseFloat(p.Upl, 64)

		side := "long"
		if p.PosSide == "short" || size < 0 {
			side = "short"
			if size < 0 {
				size = -size
			}
		}

		positions = append(positions, &Position{
			Symbol:        p.InstId,
			Side:          side,
			Size:          size,
			EntryPrice:    entry,
			MarkPrice:     mark,
			UnrealizedPnL: unrealized,
			MarketType:    okxMarketType(p.InstType),
		})
	}

	return positions, nil
}

// okxFillInstTypes are the product lines fills-history is queried on. OKX
// makes instType mandatory and answers for exactly one per call, so asking
// only for SWAP made every spot and margin fill invisible: an account trading
// spot reported zero trades, zero volume and zero fees while its equity moved
// daily. One call per line is the price of seeing them.
var okxFillInstTypes = []string{"SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"}

// okxMarketType files every fill under swap, whatever product line it came
// from. OKX runs a unified account and this connector cannot split equity per
// product, so the sync layer files the whole balance under one bucket
// (primaryMarketType okx=swap). A fill typed by its own product line would land
// in a bucket holding no equity — per-market return then divides by zero, and
// the equity sits in a bucket showing no activity. Truthful per-product typing
// has to wait for a balance split, in this connector and in the rebuilder
// together.
func okxMarketType(string) string {
	return MarketSwap
}

func (o *OKX) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	var trades []*Trade
	var lastErr error
	answered := false

	for _, instType := range okxFillInstTypes {
		batch, err := o.fillsFor(ctx, instType, start, end)
		if err != nil {
			lastErr = err
			continue
		}
		answered = true
		trades = append(trades, batch...)
	}

	// A product line the account never enabled answers with an error; only a
	// run where every line failed is a real failure worth surfacing.
	if !answered && lastErr != nil {
		return nil, lastErr
	}
	return trades, nil
}

func (o *OKX) fillsFor(ctx context.Context, instType string, start, end time.Time) ([]*Trade, error) {
	path := fmt.Sprintf("/api/v5/trade/fills-history?instType=%s&begin=%d&end=%d&limit=100",
		instType, start.UnixMilli(), end.UnixMilli())

	body, err := o.doRequest(ctx, "GET", path)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			TradeId  string `json:"tradeId"`
			InstId   string `json:"instId"`
			Side     string `json:"side"`
			FillPx   string `json:"fillPx"`
			FillSz   string `json:"fillSz"`
			Fee      string `json:"fee"`
			FeeCcy   string `json:"feeCcy"`
			Ts       string `json:"ts"`
			InstType string `json:"instType"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, t := range resp.Data {
		price, _ := strconv.ParseFloat(t.FillPx, 64)
		qty, _ := strconv.ParseFloat(t.FillSz, 64)
		fee, _ := strconv.ParseFloat(t.Fee, 64)
		ts, _ := strconv.ParseInt(t.Ts, 10, 64)

		trades = append(trades, &Trade{
			ID:       t.TradeId,
			Symbol:   t.InstId,
			Side:     t.Side,
			Price:    price,
			Quantity: qty,
			// OKX signs fee from the account's viewpoint — negative when
			// charged, positive on a rebate — while every other connector (and
			// the aggregation summing Trade.Fee) books a cost as positive.
			// Passed through raw, a live day's fees summed NEGATIVE.
			Fee:         -fee,
			FeeCurrency: t.FeeCcy,
			Timestamp:   time.UnixMilli(ts),
			MarketType:  okxMarketType(t.InstType),
		})
	}

	return trades, nil
}

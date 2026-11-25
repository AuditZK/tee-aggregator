package connector

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	bitstampAPI  = "https://www.bitstamp.net"
	bitstampHost = "www.bitstamp.net"
	bitstampPage = 1000
	// The venue caps offset at 200000; a window past it fails instead of
	// being booked on a truncated ledger.
	bitstampMaxOffset = 200000
	// user_transactions refuses a since_timestamp older than thirty days.
	bitstampLedgerReach = 30 * 24 * time.Hour
)

// Capability warnings: a part of the account the key cannot read is left out
// of the equity, and the sync status says which.
const (
	// Staked assets leave the balances.
	BitstampEarnWarning = "bitstamp_earn_unreadable"
	// Open perpetual positions carry unrealized PnL.
	BitstampDerivativesWarning = "bitstamp_derivatives_unreadable"
)

// Bitstamp is a read-only connector for one Bitstamp account (main or sub,
// whichever the key is bound to).
//
// Equity is every balance and Earn subscription valued at the venue's own USD
// tickers, plus the unrealized PnL of open perpetual positions: their
// realized and settled PnL is paid into the balances by the periodic
// settlement.
type Bitstamp struct {
	apiKey  string
	secret  string
	client  *http.Client
	baseURL string
	host    string

	mu       sync.Mutex
	warnings []string
}

func NewBitstamp(creds *Credentials) *Bitstamp {
	return &Bitstamp{
		apiKey:  strings.TrimSpace(creds.APIKey),
		secret:  strings.TrimSpace(creds.APISecret),
		client:  &http.Client{Timeout: 30 * time.Second},
		baseURL: bitstampAPI,
		host:    bitstampHost,
	}
}

func (b *Bitstamp) Exchange() string { return "bitstamp" }

func (b *Bitstamp) DetectIsPaper(_ context.Context) (bool, error) { return false, nil }

func (b *Bitstamp) TestConnection(ctx context.Context) error {
	_, err := b.GetBalance(ctx)
	return err
}

func (b *Bitstamp) CapabilityWarnings() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.warnings...)
}

type bitstampBalance struct {
	Currency  string          `json:"currency"`
	Total     bitstampDecimal `json:"total"`
	Available bitstampDecimal `json:"available"`
}

type bitstampEarn struct {
	Currency string          `json:"currency"`
	Amount   bitstampDecimal `json:"amount"`
}

type bitstampPosition struct {
	Market        string          `json:"market"`
	Side          string          `json:"side"`
	Size          bitstampDecimal `json:"size"`
	EntryPrice    bitstampDecimal `json:"entry_price"`
	MarkPrice     bitstampDecimal `json:"mark_price"`
	PnLUnrealized bitstampDecimal `json:"pnl_unrealized"`
}

func (b *Bitstamp) GetBalance(ctx context.Context) (*Balance, error) {
	var balances []bitstampBalance
	if err := b.private(ctx, http.MethodPost, "/api/v2/account_balances/", nil, &balances); err != nil {
		return nil, err
	}
	var warnings []string
	var earn []bitstampEarn
	if err := b.private(ctx, http.MethodGet, "/api/v2/earn/subscriptions/", nil, &earn); err != nil {
		if errors.Is(err, ErrTransient) {
			return nil, err
		}
		warnings = append(warnings, BitstampEarnWarning)
	}
	positions, err := b.fetchPositions(ctx)
	if err != nil {
		if errors.Is(err, ErrTransient) {
			return nil, err
		}
		warnings = append(warnings, BitstampDerivativesWarning)
	}

	var holdings []SpotHolding
	var available []SpotHolding
	for _, bal := range balances {
		holdings = append(holdings, SpotHolding{Asset: bal.Currency, Amount: float64(bal.Total)})
		available = append(available, SpotHolding{Asset: bal.Currency, Amount: float64(bal.Available)})
	}
	for _, e := range earn {
		holdings = append(holdings, SpotHolding{Asset: e.Currency, Amount: float64(e.Amount)})
	}
	prices, err := b.priceMapFor(ctx, holdings)
	if err != nil {
		return nil, err
	}
	var unrealized float64
	for _, p := range positions {
		unrealized += float64(p.PnLUnrealized)
	}
	equity := ValueSpotHoldingsUSD(holdings, prices) + unrealized

	b.mu.Lock()
	b.warnings = warnings
	b.mu.Unlock()

	return &Balance{
		Equity:        equity,
		Available:     math.Min(math.Max(ValueSpotHoldingsUSD(available, prices), 0), math.Max(equity, 0)),
		UnrealizedPnL: unrealized,
		Currency:      "USD",
	}, nil
}

func (b *Bitstamp) GetPositions(ctx context.Context) ([]*Position, error) {
	rows, err := b.fetchPositions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*Position, 0, len(rows))
	for _, r := range rows {
		size := math.Abs(float64(r.Size))
		if size == 0 {
			continue
		}
		side := "long"
		if strings.EqualFold(r.Side, "sell") || strings.EqualFold(r.Side, "short") || r.Size < 0 {
			side = "short"
		}
		out = append(out, &Position{
			Symbol:        r.Market,
			Side:          side,
			Size:          size,
			EntryPrice:    float64(r.EntryPrice),
			MarkPrice:     float64(r.MarkPrice),
			UnrealizedPnL: float64(r.PnLUnrealized),
			MarketType:    MarketSwap,
		})
	}
	return out, nil
}

// fetchPositions reads the open perpetual positions. An account without
// derivatives answers API5506, which is an account holding none.
func (b *Bitstamp) fetchPositions(ctx context.Context) ([]bitstampPosition, error) {
	var rows []bitstampPosition
	err := b.private(ctx, http.MethodGet, "/api/v2/open_positions/", nil, &rows)
	if errors.Is(err, errBitstampNoDerivatives) {
		return nil, nil
	}
	return rows, err
}

// GetTrades returns the spot fills from the ledger and the perpetual fills
// from the derivatives trade history.
func (b *Bitstamp) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	rows, err := b.fetchLedger(ctx, start, end)
	if err != nil {
		return nil, err
	}
	prices, err := b.priceMapFor(ctx, nil)
	if err != nil {
		return nil, err
	}
	var trades []*Trade
	for _, r := range rows {
		if r.Type != "2" {
			continue
		}
		base, quote, rate, ok := r.pair()
		if !ok {
			continue
		}
		baseAmt := r.amount(base)
		if baseAmt == 0 {
			continue
		}
		side := "buy"
		if baseAmt < 0 {
			side = "sell"
		}
		quoteUSD := bitstampUSDPrice(quote, prices)
		trades = append(trades, &Trade{
			ID:          strconv.FormatInt(r.ID, 10),
			Symbol:      strings.ToUpper(base + "/" + quote),
			Side:        side,
			Price:       rate,
			Quantity:    math.Abs(baseAmt),
			USDNotional: math.Abs(r.amount(quote)) * quoteUSD,
			Fee:         float64(r.Fee) * quoteUSD,
			FeeCurrency: "USD",
			Timestamp:   r.at,
			MarketType:  MarketSpot,
		})
	}

	perps, err := b.fetchPerpTrades(ctx, start, end)
	if err != nil {
		return nil, err
	}
	return append(trades, perps...), nil
}

// GetCashflows returns what entered or left the account: deposits,
// withdrawals and transfers between accounts, each valued at today's price of
// its currency. Staking moves stay inside the equity, which counts Earn.
func (b *Bitstamp) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	rows, err := b.fetchLedger(ctx, since, time.Time{})
	if err != nil {
		return nil, err
	}
	prices, err := b.priceMapFor(ctx, nil)
	if err != nil {
		return nil, err
	}
	var flows []*Cashflow
	for _, r := range rows {
		if !bitstampCapitalTypes[r.Type] {
			continue
		}
		var usd float64
		for currency, amount := range r.amounts {
			usd += amount * bitstampUSDPrice(currency, prices)
		}
		if usd != 0 {
			flows = append(flows, &Cashflow{Amount: usd, Currency: "USD", Timestamp: r.at})
		}
	}
	return flows, nil
}

// bitstampCapitalTypes are the ledger types that bring capital in or take it
// out: deposit, withdrawal, sub-account transfer, inter-account transfer.
var bitstampCapitalTypes = map[string]bool{"0": true, "1": true, "14": true, "35": true}

// bitstampTx is one ledger row. The venue keys each currency's signed amount
// by its code and the rate by the pair, "btc_usd", so they are read off the
// raw object.
type bitstampTx struct {
	ID      int64
	Type    string
	Fee     bitstampDecimal
	at      time.Time
	amounts map[string]float64
	rates   map[string]float64
}

func (r *bitstampTx) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	r.amounts, r.rates = map[string]float64{}, map[string]float64{}
	for k, v := range raw {
		key := strings.ToLower(k)
		switch key {
		case "id":
			r.ID = int64(ParseLooseNumber(v))
		case "type":
			r.Type = strings.Trim(strings.TrimSpace(string(v)), `"`)
		case "fee":
			r.Fee = bitstampDecimal(ParseLooseNumber(v))
		case "datetime":
			var s string
			_ = json.Unmarshal(v, &s)
			at, err := time.ParseInLocation("2006-01-02 15:04:05.999999", s, time.UTC)
			if err != nil {
				return fmt.Errorf("bitstamp ledger row with an unreadable date")
			}
			r.at = at
		case "order_id", "self_trade", "self_trade_order_id", "sub_account_id":
		default:
			if !bitstampCode(key) {
				continue
			}
			if strings.Contains(key, "_") {
				r.rates[key] = ParseLooseNumber(v)
			} else if n := ParseLooseNumber(v); n != 0 {
				r.amounts[key] = n
			}
		}
	}
	return nil
}

func (r *bitstampTx) amount(currency string) float64 { return r.amounts[currency] }

// pair returns the traded pair of a trade row and its rate.
func (r *bitstampTx) pair() (base, quote string, rate float64, ok bool) {
	for k, v := range r.rates {
		parts := strings.SplitN(k, "_", 2)
		if len(parts) == 2 && v > 0 {
			return parts[0], parts[1], v, true
		}
	}
	return "", "", 0, false
}

// bitstampCode reports a key shaped like a currency code or a pair of them.
func bitstampCode(key string) bool {
	if key == "" || len(key) > 24 {
		return false
	}
	for _, c := range key {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}

// fetchLedger walks user_transactions between two instants, oldest first. A
// zero end means now. The venue serves thirty days back.
func (b *Bitstamp) fetchLedger(ctx context.Context, start, end time.Time) ([]bitstampTx, error) {
	if floor := time.Now().Add(-bitstampLedgerReach + time.Hour); start.Before(floor) {
		start = floor
	}
	var all []bitstampTx
	for offset := 0; offset <= bitstampMaxOffset; offset += bitstampPage {
		form := url.Values{
			"offset":          {strconv.Itoa(offset)},
			"limit":           {strconv.Itoa(bitstampPage)},
			"sort":            {"asc"},
			"since_timestamp": {strconv.FormatInt(start.Unix(), 10)},
		}
		if !end.IsZero() {
			form.Set("until_timestamp", strconv.FormatInt(end.Unix(), 10))
		}
		var rows []bitstampTx
		if err := b.private(ctx, http.MethodPost, "/api/v2/user_transactions/", form, &rows); err != nil {
			return nil, err
		}
		all = append(all, rows...)
		if len(rows) < bitstampPage {
			return all, nil
		}
	}
	return nil, errors.New("bitstamp: ledger window holds more rows than the venue pages")
}

func (b *Bitstamp) fetchPerpTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	q := url.Values{"limit": {strconv.Itoa(bitstampPage)}, "sort": {"asc"}, "since_timestamp": {strconv.FormatInt(start.Unix(), 10)}}
	if !end.IsZero() {
		q.Set("until_timestamp", strconv.FormatInt(end.Unix(), 10))
	}
	var rows []struct {
		TradeID  string          `json:"trade_id"`
		Datetime string          `json:"datetime"`
		Fee      bitstampDecimal `json:"fee"`
		Market   string          `json:"market"`
		Side     string          `json:"side"`
		Price    bitstampDecimal `json:"price"`
		Amount   bitstampDecimal `json:"amount"`
	}
	err := b.private(ctx, http.MethodGet, "/api/v2/trade_history/?"+q.Encode(), nil, &rows)
	if errors.Is(err, errBitstampNoDerivatives) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var trades []*Trade
	for _, r := range rows {
		at, err := bitstampTime(r.Datetime)
		if err != nil {
			continue
		}
		side := "buy"
		if strings.EqualFold(r.Side, "sell") || strings.EqualFold(r.Side, "short") {
			side = "sell"
		}
		qty := math.Abs(float64(r.Amount))
		trades = append(trades, &Trade{
			ID:          r.TradeID,
			Symbol:      r.Market,
			Side:        side,
			Price:       float64(r.Price),
			Quantity:    qty,
			USDNotional: qty * float64(r.Price),
			Fee:         float64(r.Fee),
			FeeCurrency: "USD",
			Timestamp:   at,
			MarketType:  MarketSwap,
		})
	}
	return trades, nil
}

// priceMapFor reads every spot ticker into a USD price per asset, keyed the way
// ValueSpotHoldingsUSD looks it up. A failed read fails the balance when there
// is anything to price beyond USD stablecoins.
func (b *Bitstamp) priceMapFor(ctx context.Context, holdings []SpotHolding) (map[string]float64, error) {
	prices, err := b.fetchPriceMap(ctx)
	if err == nil {
		return prices, nil
	}
	for _, h := range holdings {
		if h.Amount > 0 && !IsStablecoinUSD(h.Asset) {
			return nil, fmt.Errorf("%w: bitstamp tickers: %v", ErrSpotPricingUnavailable, err)
		}
	}
	return map[string]float64{}, nil
}

func (b *Bitstamp) fetchPriceMap(ctx context.Context) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/api/v2/ticker/", nil)
	if err != nil {
		return nil, err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, bitstampTransport(err)
	}
	data, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ticker answered %d", resp.StatusCode)
	}
	var tickers []struct {
		Market     string          `json:"market"`
		MarketType string          `json:"market_type"`
		Last       json.RawMessage `json:"last"`
	}
	if err := json.Unmarshal(data, &tickers); err != nil {
		return nil, fmt.Errorf("decode tickers: %w", err)
	}
	byQuote := map[string]map[string]float64{}
	for _, t := range tickers {
		if t.MarketType != "" && !strings.EqualFold(t.MarketType, "SPOT") {
			continue
		}
		parts := strings.SplitN(strings.ToUpper(t.Market), "/", 2)
		last := ParseLooseNumber(t.Last)
		if len(parts) != 2 || last <= 0 {
			continue
		}
		if byQuote[parts[1]] == nil {
			byQuote[parts[1]] = map[string]float64{}
		}
		byQuote[parts[1]][parts[0]] = last
	}
	usd := map[string]float64{}
	for base, p := range byQuote["USD"] {
		usd[base] = p
	}
	// A base quoted only in another currency is converted through that
	// currency's USD price.
	for quote, bases := range byQuote {
		if quote == "USD" || usd[quote] <= 0 {
			continue
		}
		for base, p := range bases {
			if _, ok := usd[base]; !ok {
				usd[base] = p * usd[quote]
			}
		}
	}
	if len(usd) == 0 {
		return nil, errors.New("no USD-quoted ticker")
	}
	prices := make(map[string]float64, len(usd))
	for asset, p := range usd {
		prices[asset+"USDT"] = p
	}
	return prices, nil
}

// bitstampUSDPrice is the USD value of one unit of a currency.
func bitstampUSDPrice(currency string, prices map[string]float64) float64 {
	c := strings.ToUpper(strings.TrimSpace(currency))
	if c == "USD" {
		return 1
	}
	if p := prices[c+"USDT"]; p > 0 {
		return p
	}
	if IsStablecoinUSD(c) {
		return 1
	}
	return 0
}

var errBitstampNoDerivatives = errors.New("bitstamp: account holds no derivatives")

// private sends one signed request (Bitstamp v2 authentication) and decodes
// the answer into out. A path may carry its query string.
func (b *Bitstamp) private(ctx context.Context, method, pathAndQuery string, form url.Values, out any) error {
	if b.apiKey == "" || b.secret == "" {
		return errors.New("bitstamp: invalid credentials, expected an API key and its secret")
	}
	path, query, _ := strings.Cut(pathAndQuery, "?")
	if query != "" {
		query = "?" + query
	}
	body := ""
	contentType := ""
	if len(form) > 0 {
		body = form.Encode()
		contentType = "application/x-www-form-urlencoded"
	}
	nonce, err := bitstampNonce()
	if err != nil {
		return err
	}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	message := "BITSTAMP " + b.apiKey + method + b.host + path + query + contentType + nonce + timestamp + "v2" + body
	mac := hmac.New(sha256.New, []byte(b.secret))
	mac.Write([]byte(message))

	req, err := http.NewRequestWithContext(ctx, method, b.baseURL+path+query, strings.NewReader(body))
	if err != nil {
		return errors.New("bitstamp: build request")
	}
	req.Header.Set("X-Auth", "BITSTAMP "+b.apiKey)
	req.Header.Set("X-Auth-Signature", hex.EncodeToString(mac.Sum(nil)))
	req.Header.Set("X-Auth-Nonce", nonce)
	req.Header.Set("X-Auth-Timestamp", timestamp)
	req.Header.Set("X-Auth-Version", "v2")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: bitstamp request: %w", ErrTransient, bitstampTransport(err))
	}
	data, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return fmt.Errorf("read bitstamp response: %w", err)
	}

	var refusal struct {
		Status string          `json:"status"`
		Code   string          `json:"code"`
		Reason json.RawMessage `json:"reason"`
	}
	_ = json.Unmarshal(data, &refusal)
	if resp.StatusCode != http.StatusOK || strings.EqualFold(refusal.Status, "error") {
		return bitstampError(resp.StatusCode, refusal.Code, string(refusal.Reason))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode bitstamp %s: %w", path, err)
	}
	return nil
}

func bitstampError(status int, code, reason string) error {
	detail := vendorErrorDetail(strings.TrimSpace(code + " " + reason))
	switch code {
	case "API0001", "API0005", "API0008", "API0011", "API0000":
		return fmt.Errorf("bitstamp: invalid credentials (%s)", detail)
	case "API0002":
		return fmt.Errorf("%w: bitstamp (%s)", ErrIPRestricted, detail)
	case "API0003":
		return fmt.Errorf("bitstamp: permission denied (%s)", detail)
	case "API0006":
		return fmt.Errorf("bitstamp: permission denied, the account is frozen (%s)", detail)
	case "API5506":
		return errBitstampNoDerivatives
	case "API0004", "API0017":
		return fmt.Errorf("%w: bitstamp clock or nonce refused (%s)", ErrTransient, detail)
	}
	if status == http.StatusTooManyRequests || isRetryableStatus(status) || strings.HasPrefix(code, "400.06") || code == "400.002" {
		return fmt.Errorf("%w: bitstamp API error %d (%s)", ErrTransient, status, detail)
	}
	return fmt.Errorf("bitstamp API error %d (%s)", status, detail)
}

// bitstampTransport drops the request URL from a transport error.
func bitstampTransport(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

func bitstampNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", errors.New("bitstamp: no randomness for the nonce")
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], nil
}

func bitstampTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999", "2006-01-02T15:04:05.999999"} {
		if at, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return at.UTC(), nil
		}
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n > 1e14 {
			return time.UnixMicro(n).UTC(), nil
		}
		return time.Unix(n, 0).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unreadable bitstamp date")
}

// bitstampDecimal reads an amount the venue sends as a string or a number.
type bitstampDecimal float64

func (d *bitstampDecimal) UnmarshalJSON(b []byte) error {
	*d = bitstampDecimal(ParseLooseNumber(b))
	return nil
}
